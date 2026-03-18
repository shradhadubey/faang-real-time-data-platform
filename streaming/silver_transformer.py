"""
Bronze → Silver Layer Transformer
===================================
Reads raw JSON files from the Bronze layer, applies data quality
checks, transforms and enriches the data, then writes clean
Parquet files to the Silver layer.

No Spark needed — runs locally using pandas + pyarrow.

Usage:
    python silver_transformer.py --bucket YOUR-BUCKET-NAME
    python silver_transformer.py --bucket YOUR-BUCKET-NAME --date 2026-03-18
"""

import io
import json
import logging
import argparse
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date, timezone
from typing import Optional

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("silver-transformer")

# ── Configuration ─────────────────────────────────────────────────────────────
AWS_REGION = "us-east-1"

VALID_EVENT_TYPES  = {"page_view", "product_view", "add_to_cart", "purchase", "search", "wishlist_add"}
VALID_DEVICE_TYPES = {"mobile", "desktop", "tablet"}

# ── PyArrow Schema — enforces Silver layer types ───────────────────────────────
SILVER_SCHEMA = pa.schema([
    pa.field("event_id",        pa.string(),    nullable=False),
    pa.field("user_id",         pa.string(),    nullable=False),
    pa.field("session_id",      pa.string(),    nullable=True),
    pa.field("event_type",      pa.string(),    nullable=False),
    pa.field("product_id",      pa.string(),    nullable=True),
    pa.field("product_name",    pa.string(),    nullable=True),
    pa.field("category",        pa.string(),    nullable=True),
    pa.field("price",           pa.float64(),   nullable=True),
    pa.field("quantity",        pa.int32(),     nullable=True),
    pa.field("device_type",     pa.string(),    nullable=False),
    pa.field("country",         pa.string(),    nullable=False),
    pa.field("revenue",         pa.float64(),   nullable=True),
    pa.field("event_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("event_date",      pa.string(),    nullable=False),
    pa.field("event_hour",      pa.int32(),     nullable=False),
    pa.field("event_year",      pa.int32(),     nullable=False),
    pa.field("event_month",     pa.int32(),     nullable=False),
    pa.field("event_day",       pa.int32(),     nullable=False),
    pa.field("schema_version",  pa.string(),    nullable=True),
    pa.field("processed_at",    pa.string(),    nullable=False),
])


class SilverTransformer:
    def __init__(self, bucket: str, region: str):
        self.s3     = boto3.client("s3", region_name=region)
        self.bucket = bucket

        # Stats
        self.total_read     = 0
        self.total_valid    = 0
        self.total_rejected = 0
        self.files_written  = 0

        logger.info("Silver transformer ready")
        logger.info(f"  Source : s3://{bucket}/bronze/raw_events/")
        logger.info(f"  Target : s3://{bucket}/silver/cleaned_events/")

    # ── S3 Helpers ────────────────────────────────────────────────────────────

    def _list_bronze_files(self, partition: str) -> list:
        """List all JSON files in a Bronze partition."""
        prefix   = f"bronze/raw_events/{partition}/"
        paginator = self.s3.get_paginator("list_objects_v2")
        files    = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json"):
                    files.append(obj["Key"])

        return files

    def _read_bronze_file(self, key: str) -> list[dict]:
        """Download and parse a newline-delimited JSON file from S3."""
        response = self.s3.get_object(Bucket=self.bucket, Key=key)
        content  = response["Body"].read().decode("utf-8")
        records  = []

        for line in content.strip().split("\n"):
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning(f"Skipping malformed line in {key}")

        return records

    def _write_parquet_to_s3(self, df: pd.DataFrame, key: str):
        """Write a DataFrame as Parquet to S3."""
        table  = pa.Table.from_pandas(df, schema=SILVER_SCHEMA, safe=False)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        self.s3.put_object(
            Bucket      = self.bucket,
            Key         = key,
            Body        = buffer.getvalue(),
            ContentType = "application/octet-stream",
        )
        self.files_written += 1
        logger.info(f"  ✓ Wrote {len(df):,} records → s3://{self.bucket}/{key}")

    def _write_rejected_to_s3(self, records: list[dict], partition: str):
        """Write rejected records to dead-letter path for auditing."""
        if not records:
            return
        key  = f"silver/rejected/{partition}/rejected_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        body = "\n".join(json.dumps(r) for r in records)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        logger.warning(f"  ⚠ Wrote {len(records)} rejected records → s3://{self.bucket}/{key}")

    # ── Validation ────────────────────────────────────────────────────────────

    def _validate(self, record: dict) -> tuple[bool, str]:
        """Return (is_valid, reason). Reason is empty string if valid."""
        if not record.get("event_id"):
            return False, "missing_event_id"
        if not record.get("user_id"):
            return False, "missing_user_id"
        if not record.get("timestamp"):
            return False, "missing_timestamp"
        if record.get("event_type") not in VALID_EVENT_TYPES:
            return False, f"invalid_event_type:{record.get('event_type')}"
        if record.get("device_type") not in VALID_DEVICE_TYPES:
            return False, f"invalid_device_type:{record.get('device_type')}"
        if record.get("price") is not None and record["price"] <= 0:
            return False, "invalid_price"
        if record.get("quantity") is not None and record["quantity"] < 1:
            return False, "invalid_quantity"
        return True, ""

    # ── Transformation ────────────────────────────────────────────────────────

    def _transform(self, records: list[dict]) -> tuple[pd.DataFrame, list[dict]]:
        """
        Validate, clean and enrich records.
        Returns (valid_df, rejected_records).
        """
        valid    = []
        rejected = []
        now_str  = datetime.now(timezone.utc).isoformat()

        for record in records:
            is_valid, reason = self._validate(record)

            if not is_valid:
                record["_rejection_reason"] = reason
                rejected.append(record)
                continue

            # Parse timestamp
            try:
                ts = pd.Timestamp(record["timestamp"], tz="UTC")
            except Exception:
                record["_rejection_reason"] = "unparseable_timestamp"
                rejected.append(record)
                continue

            # Revenue fallback for purchases
            revenue = record.get("revenue")
            if record["event_type"] == "purchase" and revenue is None:
                price    = record.get("price")
                quantity = record.get("quantity")
                if price and quantity:
                    revenue = round(price * quantity, 2)

            valid.append({
                "event_id":        record["event_id"],
                "user_id":         record["user_id"],
                "session_id":      record.get("session_id"),
                "event_type":      record["event_type"].lower().strip(),
                "product_id":      record.get("product_id"),
                "product_name":    record.get("product_name"),
                "category":        record.get("category"),
                "price":           float(record["price"]) if record.get("price") else None,
                "quantity":        int(record["quantity"]) if record.get("quantity") else None,
                "device_type":     record["device_type"].lower().strip(),
                "country":         record["country"].upper().strip(),
                "revenue":         float(revenue) if revenue else None,
                "event_timestamp": ts,
                "event_date":      str(ts.date()),
                "event_hour":      int(ts.hour),
                "event_year":      int(ts.year),
                "event_month":     int(ts.month),
                "event_day":       int(ts.day),
                "schema_version":  record.get("schema_version", "1.0"),
                "processed_at":    now_str,
            })

        df = pd.DataFrame(valid) if valid else pd.DataFrame(columns=[f.name for f in SILVER_SCHEMA])
        return df, rejected

    # ── Main Runner ───────────────────────────────────────────────────────────

    def run(self, run_date: Optional[date] = None):
        """Process Bronze → Silver for a given date partition."""
        if run_date is None:
            run_date = date.today()

        partition = f"year={run_date.year}/month={run_date.month:02d}/day={run_date.day:02d}"
        logger.info(f"Processing partition: {partition}")

        # List Bronze files
        bronze_files = self._list_bronze_files(partition)
        if not bronze_files:
            logger.warning(f"No Bronze files found for partition {partition}")
            return

        logger.info(f"Found {len(bronze_files)} Bronze file(s) to process")

        all_valid    = []
        all_rejected = []

        # Read and transform each file
        for key in bronze_files:
            records = self._read_bronze_file(key)
            self.total_read += len(records)
            logger.info(f"  Reading {len(records):,} records from {key.split('/')[-1]}")

            valid_df, rejected = self._transform(records)
            if not valid_df.empty:
                all_valid.append(valid_df)
            all_rejected.extend(rejected)

        self.total_rejected += len(all_rejected)

        # Write Silver Parquet
        if all_valid:
            combined_df      = pd.concat(all_valid, ignore_index=True)
            self.total_valid += len(combined_df)
            silver_key       = (
                "silver/cleaned_events/"
                f"event_year={run_date.year}/"
                f"event_month={run_date.month:02d}/"
                f"event_day={run_date.day:02d}/"
                f"silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            )
            self._write_parquet_to_s3(combined_df, silver_key)

        # Write rejected records
        if all_rejected:
            self._write_rejected_to_s3(all_rejected, partition)

        # Summary
        logger.info("=" * 55)
        logger.info(f"Silver transformation complete for {run_date}")
        logger.info(f"  Total read     : {self.total_read:,}")
        logger.info(f"  Valid → Silver : {self.total_valid:,}")
        logger.info(f"  Rejected       : {self.total_rejected:,} "
                    f"({self.total_rejected/max(self.total_read,1)*100:.1f}%)")
        logger.info(f"  Files written  : {self.files_written}")
        logger.info(f"  S3 location    : s3://{self.bucket}/silver/cleaned_events/")
        logger.info("=" * 55)


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze → Silver Transformer")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--date",   default=None,  help="Date to process (YYYY-MM-DD), default=today")
    parser.add_argument("--region", default=AWS_REGION)
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else None

    transformer = SilverTransformer(bucket=args.bucket, region=args.region)
    transformer.run(run_date=run_date)
