"""
Silver → Gold Layer Aggregator
================================
Reads cleaned Parquet from Silver layer and produces three
Gold analytics tables:

  1. revenue_metrics   — hourly revenue by country + category
  2. top_products      — product leaderboard with conversion rates
  3. user_segments     — users segmented by spend behaviour

No Spark needed — runs locally using pandas + pyarrow.

Usage:
    python gold_aggregator.py --bucket YOUR-BUCKET-NAME
    python gold_aggregator.py --bucket YOUR-BUCKET-NAME --date 2026-03-18
"""

import io
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
logger = logging.getLogger("gold-aggregator")

# ── Configuration ─────────────────────────────────────────────────────────────
AWS_REGION = "us-east-1"


class GoldAggregator:
    def __init__(self, bucket: str, region: str):
        self.s3     = boto3.client("s3", region_name=region)
        self.bucket = bucket

        logger.info("Gold aggregator ready")
        logger.info(f"  Source : s3://{bucket}/silver/cleaned_events/")
        logger.info(f"  Target : s3://{bucket}/gold/")

    # ── S3 Helpers ────────────────────────────────────────────────────────────

    def _list_silver_files(self, partition: str) -> list:
        """List all Parquet files in a Silver partition."""
        prefix    = f"silver/cleaned_events/{partition}/"
        paginator = self.s3.get_paginator("list_objects_v2")
        files     = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    files.append(obj["Key"])
        return files

    def _read_parquet_from_s3(self, key: str) -> pd.DataFrame:
        """Download and read a Parquet file from S3 into a DataFrame."""
        response = self.s3.get_object(Bucket=self.bucket, Key=key)
        buffer   = io.BytesIO(response["Body"].read())
        return pq.read_table(buffer).to_pandas()

    def _write_parquet_to_s3(self, df: pd.DataFrame, key: str):
        """Write a DataFrame as Parquet to S3."""
        table  = pa.Table.from_pandas(df, preserve_index=False)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        self.s3.put_object(
            Bucket      = self.bucket,
            Key         = key,
            Body        = buffer.getvalue(),
            ContentType = "application/octet-stream",
        )
        logger.info(f"  ✓ Wrote {len(df):,} rows → s3://{self.bucket}/{key}")

    # ── Gold Table 1: Revenue Metrics ─────────────────────────────────────────

    def _build_revenue_metrics(self, df: pd.DataFrame, run_date: date) -> pd.DataFrame:
        """
        Hourly revenue breakdown by country and category.
        Powers the main revenue dashboard.
        """
        purchases = df[df["event_type"] == "purchase"].copy()

        if purchases.empty:
            logger.warning("No purchase events found — revenue metrics will be empty")
            return pd.DataFrame()

        revenue = (
            purchases
            .groupby(["event_hour", "country", "category"], dropna=False)
            .agg(
                total_revenue    = ("revenue",  "sum"),
                total_orders     = ("event_id", "count"),
                total_units_sold = ("quantity", "sum"),
                avg_order_value  = ("price",    "mean"),
                unique_buyers    = ("user_id",  "nunique"),
            )
            .reset_index()
        )

        revenue["total_revenue"]   = revenue["total_revenue"].round(2)
        revenue["avg_order_value"] = revenue["avg_order_value"].round(2)
        revenue["report_date"]     = str(run_date)
        revenue["report_year"]     = run_date.year
        revenue["report_month"]    = run_date.month
        revenue["report_day"]      = run_date.day
        revenue["created_at"]      = datetime.now(timezone.utc).isoformat()

        return revenue

    # ── Gold Table 2: Top Products ─────────────────────────────────────────────

    def _build_top_products(self, df: pd.DataFrame, run_date: date, top_n: int = 20) -> pd.DataFrame:
        """
        Product leaderboard with revenue, conversion rate and rank.
        """
        products = df[df["product_id"].notna()].copy()

        if products.empty:
            logger.warning("No product events found")
            return pd.DataFrame()

        # Aggregate per product using pivot approach
        metrics = []
        for (pid, pname, cat), grp in products.groupby(["product_id", "product_name", "category"]):
            metrics.append({
                "product_id":    pid,
                "product_name":  pname,
                "category":      cat,
                "revenue":       round(grp[grp["event_type"] == "purchase"]["revenue"].sum(), 2),
                "purchase_count":int((grp["event_type"] == "purchase").sum()),
                "cart_adds":     int((grp["event_type"] == "add_to_cart").sum()),
                "product_views": int((grp["event_type"] == "product_view").sum()),
                "unique_users":  int(grp["user_id"].nunique()),
            })

        result = pd.DataFrame(metrics)

        # Conversion rate: purchases / cart_adds
        result["conversion_rate"] = (
            result["purchase_count"] / result["cart_adds"].replace(0, 1)
        ).round(4)

        # Rank by revenue
        result = result.sort_values("revenue", ascending=False).head(top_n)
        result["revenue_rank"] = range(1, len(result) + 1)

        result["report_date"]  = str(run_date)
        result["report_year"]  = run_date.year
        result["report_month"] = run_date.month
        result["report_day"]   = run_date.day
        result["created_at"]   = datetime.now(timezone.utc).isoformat()

        return result

    # ── Gold Table 3: User Segments ────────────────────────────────────────────

    def _build_user_segments(self, df: pd.DataFrame, run_date: date) -> pd.DataFrame:
        """
        Segment users by daily spend and activity level.
        Useful for marketing and retention analysis.
        """
        metrics = []

        for (uid, device, country), grp in df.groupby(["user_id", "device_type", "country"]):
            purchases    = grp[grp["event_type"] == "purchase"]
            daily_spend  = round(purchases["revenue"].sum(), 2)
            event_count  = len(grp)
            order_count  = len(purchases)
            session_count = grp["session_id"].nunique()

            # Segment logic
            if daily_spend > 500:
                segment = "vip"
            elif daily_spend > 100:
                segment = "high_value"
            elif daily_spend > 0:
                segment = "buyer"
            elif event_count > 10:
                segment = "engaged_browser"
            else:
                segment = "casual_browser"

            metrics.append({
                "user_id":      uid,
                "device_type":  device,
                "country":      country,
                "daily_spend":  daily_spend,
                "order_count":  order_count,
                "event_count":  event_count,
                "session_count":session_count,
                "user_segment": segment,
                "report_date":  str(run_date),
                "report_year":  run_date.year,
                "report_month": run_date.month,
                "report_day":   run_date.day,
                "created_at":   datetime.now(timezone.utc).isoformat(),
            })

        return pd.DataFrame(metrics)

    # ── Main Runner ───────────────────────────────────────────────────────────

    def run(self, run_date: Optional[date] = None):
        if run_date is None:
            run_date = date.today()

        partition = (
            f"event_year={run_date.year}/"
            f"event_month={run_date.month:02d}/"
            f"event_day={run_date.day:02d}"
        )

        logger.info(f"Processing Silver partition: {partition}")

        # Load Silver files
        silver_files = self._list_silver_files(partition)
        if not silver_files:
            logger.error(f"No Silver files found for {partition} — run silver_transformer.py first")
            return

        logger.info(f"Loading {len(silver_files)} Silver file(s)...")
        dfs = [self._read_parquet_from_s3(key) for key in silver_files]
        df  = pd.concat(dfs, ignore_index=True)
        logger.info(f"Loaded {len(df):,} Silver records")

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        # ── Gold Table 1: Revenue Metrics ─────────────────────────────────────
        logger.info("Building revenue metrics...")
        revenue_df = self._build_revenue_metrics(df, run_date)
        if not revenue_df.empty:
            self._write_parquet_to_s3(
                revenue_df,
                "gold/revenue_metrics/"
                f"report_year={run_date.year}/report_month={run_date.month:02d}/report_day={run_date.day:02d}/"
                f"revenue_{ts}.parquet"
            )

        # ── Gold Table 2: Top Products ─────────────────────────────────────────
        logger.info("Building top products...")
        products_df = self._build_top_products(df, run_date)
        if not products_df.empty:
            self._write_parquet_to_s3(
                products_df,
                "gold/top_products/"
                f"report_year={run_date.year}/report_month={run_date.month:02d}/report_day={run_date.day:02d}/"
                f"products_{ts}.parquet"
            )

        # ── Gold Table 3: User Segments ────────────────────────────────────────
        logger.info("Building user segments...")
        users_df = self._build_user_segments(df, run_date)
        if not users_df.empty:
            self._write_parquet_to_s3(
                users_df,
                "gold/user_segments/"
                f"report_year={run_date.year}/report_month={run_date.month:02d}/report_day={run_date.day:02d}/"
                f"users_{ts}.parquet"
            )

        # ── Summary ────────────────────────────────────────────────────────────
        logger.info("=" * 55)
        logger.info(f"Gold aggregation complete for {run_date}")
        logger.info(f"  Silver records processed : {len(df):,}")
        logger.info(f"  Revenue metric rows      : {len(revenue_df):,}")
        logger.info(f"  Top products rows        : {len(products_df):,}")
        logger.info(f"  User segment rows        : {len(users_df):,}")
        logger.info(f"  S3 location              : s3://{self.bucket}/gold/")
        logger.info("=" * 55)


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver → Gold Aggregator")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--date",   default=None,  help="Date YYYY-MM-DD (default=today)")
    parser.add_argument("--region", default=AWS_REGION)
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date) if args.date else None

    aggregator = GoldAggregator(bucket=args.bucket, region=args.region)
    aggregator.run(run_date=run_date)
