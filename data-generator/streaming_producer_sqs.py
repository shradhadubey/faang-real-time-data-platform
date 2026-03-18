"""
Real-Time E-Commerce Event Producer (SQS Version)
===================================================
Simulates high-volume e-commerce events and publishes them to
AWS SQS. Free-tier compatible (1M messages/month free).

Author: Portfolio Project
Architecture: FAANG-Level Data Platform
"""

import json
import time
import uuid
import random
import logging
import argparse
import boto3
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("sqs-producer")

# ── Configuration ─────────────────────────────────────────────────────────────
QUEUE_NAME       = "faang-ecommerce-events"
AWS_REGION       = "us-east-1"
BATCH_SIZE       = 10        # SQS max batch size is 10
RECORDS_PER_SEC  = 50        # Conservative for free tier

# ── Domain Data ───────────────────────────────────────────────────────────────
PRODUCTS = [
    {"id": "P001", "name": "MacBook Pro 16",    "category": "Electronics", "base_price": 2499.99},
    {"id": "P002", "name": "iPhone 15 Pro",     "category": "Electronics", "base_price": 999.99},
    {"id": "P003", "name": "AirPods Pro",       "category": "Electronics", "base_price": 249.99},
    {"id": "P004", "name": "Nike Air Max",      "category": "Footwear",    "base_price": 129.99},
    {"id": "P005", "name": "Levi's 501 Jeans",  "category": "Apparel",     "base_price": 69.99},
    {"id": "P006", "name": "Dyson V15",         "category": "Home",        "base_price": 749.99},
    {"id": "P007", "name": "Kindle Paperwhite", "category": "Electronics", "base_price": 139.99},
    {"id": "P008", "name": "Instant Pot Duo",   "category": "Kitchen",     "base_price": 99.99},
    {"id": "P009", "name": "Yoga Mat Premium",  "category": "Sports",      "base_price": 79.99},
    {"id": "P010", "name": "Coffee Maker Pro",  "category": "Kitchen",     "base_price": 189.99},
]

EVENT_TYPES  = [("page_view", 0.45), ("product_view", 0.25), ("add_to_cart", 0.15),
                ("purchase", 0.10), ("search", 0.03), ("wishlist_add", 0.02)]
DEVICE_TYPES = [("mobile", 0.55), ("desktop", 0.30), ("tablet", 0.15)]
COUNTRIES    = [("US", 0.40), ("GB", 0.12), ("DE", 0.08), ("FR", 0.07),
                ("CA", 0.07), ("AU", 0.06), ("JP", 0.05), ("IN", 0.05),
                ("BR", 0.05), ("MX", 0.05)]


def weighted_choice(choices):
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


@dataclass
class ECommerceEvent:
    event_id:       str
    timestamp:      str
    user_id:        str
    session_id:     str
    event_type:     str
    product_id:     Optional[str]
    product_name:   Optional[str]
    category:       Optional[str]
    price:          Optional[float]
    quantity:       Optional[int]
    device_type:    str
    country:        str
    revenue:        Optional[float]
    schema_version: str = "1.0"

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


def generate_event() -> ECommerceEvent:
    event_type  = weighted_choice(EVENT_TYPES)
    device_type = weighted_choice(DEVICE_TYPES)
    country     = weighted_choice(COUNTRIES)

    product = price = quantity = revenue = None

    if event_type in ("product_view", "add_to_cart", "purchase", "wishlist_add"):
        product  = random.choice(PRODUCTS)
        price    = round(product["base_price"] * random.uniform(0.7, 1.1), 2)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.1, 0.06, 0.04])[0]
        if event_type == "purchase":
            revenue = round(price * quantity, 2)

    return ECommerceEvent(
        event_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat(),
        user_id=f"U{random.randint(100000, 999999)}",
        session_id=str(uuid.uuid4()),
        event_type=event_type,
        product_id=product["id"] if product else None,
        product_name=product["name"] if product else None,
        category=product["category"] if product else None,
        price=price,
        quantity=quantity,
        device_type=device_type,
        country=country,
        revenue=revenue,
    )


# ── SQS Publisher ─────────────────────────────────────────────────────────────
class SQSProducer:
    def __init__(self, queue_name: str, region: str):
        self.sqs         = boto3.client("sqs", region_name=region)
        self.queue_url   = self.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        self.total_sent  = 0
        self.total_failed = 0
        logger.info(f"Connected to SQS queue: {self.queue_url}")

    def send_batch(self, events: list) -> None:
        """Send up to 10 events in one SQS batch (API maximum)."""
        entries = [
            {
                "Id":          str(i),
                "MessageBody": event.to_json(),
                "MessageGroupId": event.user_id,  # for FIFO queues
            }
            for i, event in enumerate(events)
        ]

        # SQS standard queue — remove MessageGroupId
        entries_standard = [
            {"Id": e["Id"], "MessageBody": e["MessageBody"]}
            for e in entries
        ]

        try:
            response = self.sqs.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries_standard,
            )
            sent   = len(response.get("Successful", []))
            failed = len(response.get("Failed", []))
            self.total_sent   += sent
            self.total_failed += failed

            if failed:
                logger.warning(f"{failed} messages failed in batch")

        except Exception as e:
            logger.error(f"Batch send error: {e}")
            raise


# ── Main Producer Loop ─────────────────────────────────────────────────────────
def run_producer(
    queue_name: str = QUEUE_NAME,
    region: str = AWS_REGION,
    records_per_second: int = RECORDS_PER_SEC,
    duration_seconds: int = 0,
    dry_run: bool = False,
):
    producer  = SQSProducer(queue_name, region) if not dry_run else None
    start     = time.time()
    iteration = 0

    logger.info(f"Starting SQS producer | RPS={records_per_second} | dry_run={dry_run}")

    try:
        while True:
            batch_start = time.time()
            events      = [generate_event() for _ in range(BATCH_SIZE)]

            if dry_run:
                logger.info(f"[DRY RUN] Sample: {events[0].to_json()[:120]}...")
            else:
                producer.send_batch(events)

            iteration += 1
            elapsed    = time.time() - batch_start
            sleep_time = max(0, (BATCH_SIZE / records_per_second) - elapsed)
            time.sleep(sleep_time)

            if iteration % 20 == 0:
                total_elapsed = time.time() - start
                actual_rps    = (iteration * BATCH_SIZE) / total_elapsed
                logger.info(
                    f"Stats | sent={producer.total_sent if producer else iteration*10:,} "
                    f"| actual_rps={actual_rps:.0f}"
                )

            if duration_seconds and (time.time() - start) >= duration_seconds:
                logger.info(f"Duration {duration_seconds}s reached — stopping.")
                break

    except KeyboardInterrupt:
        logger.info("Producer stopped by user (Ctrl+C)")
    finally:
        if producer:
            logger.info(f"Final | sent={producer.total_sent:,} | failed={producer.total_failed}")


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQS E-Commerce Event Producer")
    parser.add_argument("--queue",    default=QUEUE_NAME)
    parser.add_argument("--region",   default=AWS_REGION)
    parser.add_argument("--rps",      type=int, default=RECORDS_PER_SEC)
    parser.add_argument("--duration", type=int, default=0)
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    run_producer(
        queue_name=args.queue,
        region=args.region,
        records_per_second=args.rps,
        duration_seconds=args.duration,
        dry_run=args.dry_run,
    )