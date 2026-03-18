"""
SQS → S3 Bronze Layer Consumer
================================
Reads messages from SQS and writes them to S3 as the Bronze layer
(raw JSON, partitioned by date). This is the first stage of the
Medallion architecture.

Usage:
    python bronze_consumer.py --bucket YOUR-BUCKET-NAME
    python bronze_consumer.py --bucket YOUR-BUCKET-NAME --duration 300
"""

import json
import time
import logging
import argparse
import boto3
from datetime import datetime, timezone
from collections import defaultdict

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("bronze-consumer")

# ── Configuration ─────────────────────────────────────────────────────────────
QUEUE_NAME     = "faang-data-platform-ecommerce-events"
AWS_REGION     = "us-east-1"
BATCH_SIZE     = 10       # SQS max messages per receive call
FLUSH_EVERY    = 100      # Write to S3 every N messages
WAIT_SECONDS   = 20       # Long polling — reduces empty receives


class BronzeConsumer:
    def __init__(self, queue_name: str, bucket: str, region: str):
        self.sqs       = boto3.client("sqs", region_name=region)
        self.s3        = boto3.client("s3", region_name=region)
        self.bucket    = bucket
        self.queue_url = self.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

        # Buffer: partition_key → list of event dicts
        self.buffer        = defaultdict(list)
        self.total_written = 0
        self.total_read    = 0

        logger.info(f"Bronze consumer ready")
        logger.info(f"  Queue  : {self.queue_url}")
        logger.info(f"  Bucket : s3://{self.bucket}/bronze/raw_events/")

    def _partition_key(self, event: dict) -> str:
        """Build S3 partition path from event timestamp."""
        try:
            ts = datetime.fromisoformat(event["timestamp"])
        except Exception:
            ts = datetime.now(timezone.utc)
        return f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}"

    def _flush_to_s3(self):
        """Write buffered events to S3, one file per partition."""
        if not self.buffer:
            return

        now       = datetime.now(timezone.utc)
        file_ts   = now.strftime("%Y%m%d_%H%M%S")
        total     = sum(len(v) for v in self.buffer.values())

        for partition, events in self.buffer.items():
            key  = f"bronze/raw_events/{partition}/events_{file_ts}.json"
            body = "\n".join(json.dumps(e) for e in events)  # newline-delimited JSON

            self.s3.put_object(
                Bucket      = self.bucket,
                Key         = key,
                Body        = body.encode("utf-8"),
                ContentType = "application/json",
            )
            logger.info(f"  ✓ Wrote {len(events):,} events → s3://{self.bucket}/{key}")

        self.total_written += total
        self.buffer.clear()

    def _receive_messages(self) -> list:
        """Poll SQS for up to BATCH_SIZE messages."""
        response = self.sqs.receive_message(
            QueueUrl            = self.queue_url,
            MaxNumberOfMessages = BATCH_SIZE,
            WaitTimeSeconds     = WAIT_SECONDS,
            AttributeNames      = ["All"],
        )
        return response.get("Messages", [])

    def _delete_messages(self, messages: list):
        """Batch delete processed messages from SQS."""
        if not messages:
            return
        entries = [
            {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
            for i, m in enumerate(messages)
        ]
        self.sqs.delete_message_batch(
            QueueUrl = self.queue_url,
            Entries  = entries,
        )

    def run(self, duration_seconds: int = 0):
        """
        Main consumer loop.
        Reads from SQS → buffers → flushes to S3 Bronze layer.
        """
        start          = time.time()
        empty_polls    = 0
        messages_since_flush = 0

        logger.info(f"Starting Bronze consumer | flush_every={FLUSH_EVERY} messages")

        try:
            while True:
                messages = self._receive_messages()

                if not messages:
                    empty_polls += 1
                    if empty_polls >= 3:
                        # Flush whatever is buffered and exit if queue is empty
                        if self.buffer:
                            logger.info("Queue empty — flushing remaining buffer")
                            self._flush_to_s3()
                        logger.info("Queue is empty — consumer finishing.")
                        break
                    continue

                empty_polls = 0

                # Parse and buffer each message
                for msg in messages:
                    try:
                        event = json.loads(msg["Body"])
                        partition = self._partition_key(event)
                        self.buffer[partition].append(event)
                        self.total_read += 1
                        messages_since_flush += 1
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping malformed message: {e}")

                # Delete from SQS after successful buffering
                self._delete_messages(messages)

                # Flush to S3 when buffer is big enough
                if messages_since_flush >= FLUSH_EVERY:
                    self._flush_to_s3()
                    messages_since_flush = 0

                # Duration check
                if duration_seconds and (time.time() - start) >= duration_seconds:
                    logger.info(f"Duration {duration_seconds}s reached — flushing and stopping.")
                    self._flush_to_s3()
                    break

        except KeyboardInterrupt:
            logger.info("Interrupted — flushing buffer before exit")
            self._flush_to_s3()

        finally:
            elapsed = time.time() - start
            logger.info("=" * 50)
            logger.info(f"Bronze consumer finished")
            logger.info(f"  Total read    : {self.total_read:,} messages")
            logger.info(f"  Total written : {self.total_written:,} events to S3")
            logger.info(f"  Elapsed       : {elapsed:.1f}s")
            logger.info(f"  S3 location   : s3://{self.bucket}/bronze/raw_events/")
            logger.info("=" * 50)


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQS → S3 Bronze Layer Consumer")
    parser.add_argument("--bucket",   required=True, help="S3 bucket name")
    parser.add_argument("--queue",    default=QUEUE_NAME)
    parser.add_argument("--region",   default=AWS_REGION)
    parser.add_argument("--duration", type=int, default=0, help="Max runtime seconds (0=until queue empty)")
    args = parser.parse_args()

    consumer = BronzeConsumer(
        queue_name = args.queue,
        bucket     = args.bucket,
        region     = args.region,
    )
    consumer.run(duration_seconds=args.duration)
