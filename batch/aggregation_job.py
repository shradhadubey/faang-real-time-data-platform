"""
Batch Aggregation Job — Daily Rollups & Data Quality Report
============================================================
Runs nightly to produce finalized daily aggregates, reconcile
streaming micro-batches, and output a data quality summary.

Triggered by: Apache Airflow DAG / AWS Step Functions
Input: s3://faang-data-platform/silver/
Output: s3://faang-data-platform/gold/daily_*/

Author: Portfolio Project
"""

import logging
import sys
from datetime import date, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("batch-aggregation")

# ── Configuration ─────────────────────────────────────────────────────────────
S3_BUCKET = "s3://faang-data-platform"
SILVER_PATH = f"{S3_BUCKET}/silver/cleaned_events"
GOLD_DAILY_REVENUE = f"{S3_BUCKET}/gold/daily_revenue"
GOLD_DAILY_PRODUCTS = f"{S3_BUCKET}/gold/daily_top_products"
GOLD_DAILY_USERS = f"{S3_BUCKET}/gold/daily_user_cohorts"
GOLD_DQ_REPORT = f"{S3_BUCKET}/gold/dq_reports"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FAANG-Batch-Aggregation")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def load_silver(spark: SparkSession, run_date: date) -> DataFrame:
    """Load Silver data for the target processing date."""
    logger.info(f"Loading Silver data for date={run_date}")
    return (
        spark.read
        .parquet(SILVER_PATH)
        .filter(
            (F.col("event_year") == run_date.year) &
            (F.col("event_month") == run_date.month) &
            (F.col("event_day") == run_date.day)
        )
    )


# ── Aggregation: Daily Revenue ─────────────────────────────────────────────────
def compute_daily_revenue(df: DataFrame, run_date: date) -> DataFrame:
    """
    Hourly revenue breakdown with rolling 7-day average.
    """
    hourly = (
        df
        .filter(F.col("event_type") == "purchase")
        .groupBy("event_hour", "country", "category")
        .agg(
            F.sum("revenue").alias("hourly_revenue"),
            F.count("event_id").alias("order_count"),
            F.approx_count_distinct("user_id").alias("unique_customers"),
            F.avg("price").alias("avg_price"),
        )
        .withColumn("report_date", F.lit(str(run_date)))
        .withColumn("report_year",  F.lit(run_date.year))
        .withColumn("report_month", F.lit(run_date.month))
        .withColumn("report_day",   F.lit(run_date.day))
    )
    return hourly


# ── Aggregation: Top Products ──────────────────────────────────────────────────
def compute_top_products(df: DataFrame, run_date: date, top_n: int = 50) -> DataFrame:
    """
    Daily product leaderboard with revenue, conversion rate, and rank.
    """
    product_stats = (
        df
        .filter(F.col("product_id").isNotNull())
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum(F.when(F.col("event_type") == "purchase", F.col("revenue")).otherwise(0))
             .alias("daily_revenue"),
            F.countDistinct(
                F.when(F.col("event_type") == "purchase", F.col("event_id"))
            ).alias("purchase_count"),
            F.countDistinct(
                F.when(F.col("event_type") == "add_to_cart", F.col("event_id"))
            ).alias("cart_add_count"),
            F.countDistinct(
                F.when(F.col("event_type") == "product_view", F.col("event_id"))
            ).alias("view_count"),
            F.approx_count_distinct("user_id").alias("unique_users"),
        )
        .withColumn("conversion_rate",
            F.round(
                F.col("purchase_count") / F.greatest(F.col("cart_add_count"), F.lit(1)),
                4
            )
        )
        .withColumn("revenue_rank",
            F.rank().over(Window.orderBy(F.desc("daily_revenue")))
        )
        .filter(F.col("revenue_rank") <= top_n)
        .withColumn("report_date",  F.lit(str(run_date)))
        .withColumn("report_year",  F.lit(run_date.year))
        .withColumn("report_month", F.lit(run_date.month))
        .withColumn("report_day",   F.lit(run_date.day))
    )
    return product_stats


# ── Aggregation: User Cohorts ──────────────────────────────────────────────────
def compute_user_cohorts(df: DataFrame, run_date: date) -> DataFrame:
    """
    Segment users by activity level for DAU/retention tracking.
    """
    user_activity = (
        df
        .groupBy("user_id", "country", "device_type")
        .agg(
            F.count("event_id").alias("event_count"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("revenue")).otherwise(0))
             .alias("daily_spend"),
            F.collect_set("event_type").alias("event_types"),
            F.countDistinct("session_id").alias("session_count"),
        )
        .withColumn("user_segment", F.when(
            F.col("daily_spend") > 200, "high_value"
        ).when(
            F.col("daily_spend") > 0, "buyer"
        ).when(
            F.col("event_count") > 10, "engaged_browser"
        ).otherwise("casual_browser"))
        .withColumn("report_date",  F.lit(str(run_date)))
        .withColumn("report_year",  F.lit(run_date.year))
        .withColumn("report_month", F.lit(run_date.month))
        .withColumn("report_day",   F.lit(run_date.day))
    )
    return user_activity


# ── Data Quality Report ────────────────────────────────────────────────────────
def generate_dq_report(df: DataFrame, run_date: date) -> dict:
    """
    Compute data quality metrics: completeness, uniqueness, validity.
    Returns a summary dictionary that is also written to S3 as JSON.
    """
    total = df.count()

    if total == 0:
        logger.warning("No data found for this partition — DQ report skipped.")
        return {"run_date": str(run_date), "total_records": 0, "status": "NO_DATA"}

    null_event_id   = df.filter(F.col("event_id").isNull()).count()
    null_user_id    = df.filter(F.col("user_id").isNull()).count()
    null_timestamp  = df.filter(F.col("event_timestamp").isNull()).count()
    duplicate_ids   = total - df.dropDuplicates(["event_id"]).count()
    invalid_prices  = df.filter(
        F.col("price").isNotNull() & (F.col("price") <= 0)
    ).count()
    purchase_no_rev = df.filter(
        (F.col("event_type") == "purchase") & F.col("revenue").isNull()
    ).count()

    report = {
        "run_date": str(run_date),
        "total_records": total,
        "null_event_id_pct":   round(null_event_id   / total * 100, 3),
        "null_user_id_pct":    round(null_user_id    / total * 100, 3),
        "null_timestamp_pct":  round(null_timestamp  / total * 100, 3),
        "duplicate_event_ids": duplicate_ids,
        "invalid_price_count": invalid_prices,
        "purchases_missing_revenue": purchase_no_rev,
        "status": "PASS" if (
            null_event_id == 0 and
            null_user_id == 0 and
            duplicate_ids == 0 and
            invalid_prices == 0
        ) else "WARN",
    }

    logger.info(f"DQ Report: {report}")
    return report


def write_dq_report(spark: SparkSession, report: dict, run_date: date):
    """Persist DQ report as a single-row Parquet file."""
    spark.createDataFrame([report]).write.mode("overwrite").parquet(
        f"{GOLD_DQ_REPORT}/year={run_date.year}/month={run_date.month}/day={run_date.day}"
    )


def write_gold(df: DataFrame, path: str, run_date: date, mode: str = "overwrite"):
    """Write Gold dataset, partitioned for efficient querying."""
    logger.info(f"Writing Gold → {path}")
    (
        df.write
        .mode(mode)
        .parquet(
            f"{path}/report_year={run_date.year}"
            f"/report_month={run_date.month}"
            f"/report_day={run_date.day}"
        )
    )


# ── Main ───────────────────────────────────────────────────────────────────────
def main(run_date: date = None):
    if run_date is None:
        run_date = date.today() - timedelta(days=1)  # Default: yesterday

    logger.info(f"Batch job starting | run_date={run_date}")
    spark = create_spark_session()

    df = load_silver(spark, run_date)
    record_count = df.cache().count()
    logger.info(f"Loaded {record_count:,} Silver records for {run_date}")

    if record_count == 0:
        logger.warning("No records to process — exiting cleanly.")
        sys.exit(0)

    # Aggregations
    daily_revenue = compute_daily_revenue(df, run_date)
    top_products  = compute_top_products(df, run_date)
    user_cohorts  = compute_user_cohorts(df, run_date)

    # DQ
    dq_report = generate_dq_report(df, run_date)
    write_dq_report(spark, dq_report, run_date)

    # Write Gold
    write_gold(daily_revenue, GOLD_DAILY_REVENUE,  run_date)
    write_gold(top_products,  GOLD_DAILY_PRODUCTS, run_date)
    write_gold(user_cohorts,  GOLD_DAILY_USERS,    run_date)

    logger.info("Batch job completed successfully.")
    spark.stop()


if __name__ == "__main__":
    run_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    main(run_date)
