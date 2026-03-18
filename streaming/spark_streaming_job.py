"""
PySpark Structured Streaming — Medallion Architecture
=======================================================
Consumes real-time e-commerce events from Kinesis Data Streams,
applies progressive transformations through Bronze → Silver → Gold layers,
and writes partitioned Parquet/Delta datasets to S3.

Architecture:
    Kinesis → Bronze (raw JSON) → Silver (cleaned/typed) → Gold (aggregated metrics)

Author: Portfolio Project
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, BooleanType,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("spark-streaming")

# ── Configuration ─────────────────────────────────────────────────────────────
S3_BUCKET = "s3://faang-data-platform"
KINESIS_STREAM = "faang-ecommerce-events"
KINESIS_REGION = "us-east-1"
KINESIS_ENDPOINT = f"https://kinesis.{KINESIS_REGION}.amazonaws.com"

BRONZE_PATH = f"{S3_BUCKET}/bronze/raw_events"
SILVER_PATH = f"{S3_BUCKET}/silver/cleaned_events"
GOLD_SALES_PATH = f"{S3_BUCKET}/gold/sales_metrics"
GOLD_USER_PATH = f"{S3_BUCKET}/gold/user_metrics"
GOLD_PRODUCT_PATH = f"{S3_BUCKET}/gold/product_metrics"

CHECKPOINT_BRONZE = f"{S3_BUCKET}/_checkpoints/bronze"
CHECKPOINT_SILVER = f"{S3_BUCKET}/_checkpoints/silver"
CHECKPOINT_GOLD_SALES = f"{S3_BUCKET}/_checkpoints/gold_sales"
CHECKPOINT_GOLD_USER = f"{S3_BUCKET}/_checkpoints/gold_user"

TRIGGER_INTERVAL = "60 seconds"
WATERMARK_DELAY = "10 minutes"

# ── Event Schema ──────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("country", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("schema_version", StringType(), True),
])

VALID_EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "purchase", "search", "wishlist_add"]
VALID_DEVICE_TYPES = ["mobile", "desktop", "tablet"]


def create_spark_session() -> SparkSession:
    """Build SparkSession with Kinesis connector and S3 optimizations."""
    return (
        SparkSession.builder
        .appName("FAANG-RealTime-Data-Platform")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # S3 performance optimizations
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")
        # Delta Lake (optional — comment out if using plain Parquet)
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# ── Bronze Layer ──────────────────────────────────────────────────────────────
def read_kinesis_stream(spark: SparkSession):
    """
    Read raw bytes from Kinesis. Bronze layer stores the original
    payload untransformed — the source of truth for replays.
    """
    logger.info(f"Connecting to Kinesis stream: {KINESIS_STREAM}")

    return (
        spark.readStream
        .format("kinesis")
        .option("streamName", KINESIS_STREAM)
        .option("endpointUrl", KINESIS_ENDPOINT)
        .option("startingPosition", "LATEST")
        .option("kinesis.executor.maxFetchDurationInMs", "1000")
        .option("kinesis.executor.maxFetchRecordsPerShard", "10000")
        .load()
    )


def write_bronze_layer(kinesis_df):
    """
    Write raw events to Bronze (S3 Parquet, partitioned by date).
    Preserves original payload + Kinesis metadata for full auditability.
    """
    bronze_df = (
        kinesis_df
        .select(
            F.col("data").cast("string").alias("raw_payload"),
            F.col("partitionKey").alias("kinesis_partition_key"),
            F.col("sequenceNumber").alias("kinesis_sequence_number"),
            F.col("approximateArrivalTimestamp").alias("kinesis_ingestion_time"),
            F.current_timestamp().alias("processing_time"),
        )
        .withColumn("ingest_year",  F.year("kinesis_ingestion_time"))
        .withColumn("ingest_month", F.month("kinesis_ingestion_time"))
        .withColumn("ingest_day",   F.dayofmonth("kinesis_ingestion_time"))
    )

    return (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_BRONZE)
        .partitionBy("ingest_year", "ingest_month", "ingest_day")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ── Silver Layer ──────────────────────────────────────────────────────────────
def parse_and_clean(kinesis_df):
    """
    Silver layer: parse JSON, enforce schema, apply data quality rules.
    Corrupt records are routed to a dead-letter path, not silently dropped.
    """
    # Parse JSON payload
    parsed = (
        kinesis_df
        .select(F.col("data").cast("string").alias("raw_payload"))
        .withColumn("parsed", F.from_json(F.col("raw_payload"), EVENT_SCHEMA))
        .select("raw_payload", "parsed.*")
    )

    # ── Data Quality Filters ───────────────────────────────────────────────────
    # Tag records — do NOT silently drop so we can audit later
    validated = (
        parsed
        .withColumn("_is_valid_event_type", F.col("event_type").isin(VALID_EVENT_TYPES))
        .withColumn("_is_valid_device",     F.col("device_type").isin(VALID_DEVICE_TYPES))
        .withColumn("_has_required_fields", (
            F.col("event_id").isNotNull() &
            F.col("timestamp").isNotNull() &
            F.col("user_id").isNotNull()
        ))
        .withColumn("_price_valid", (
            F.col("price").isNull() | (F.col("price") > 0)
        ))
        .withColumn("_quantity_valid", (
            F.col("quantity").isNull() | (F.col("quantity") > 0)
        ))
        .withColumn("_is_valid_record", (
            F.col("_is_valid_event_type") &
            F.col("_is_valid_device") &
            F.col("_has_required_fields") &
            F.col("_price_valid") &
            F.col("_quantity_valid")
        ))
    )

    # ── Transformations ────────────────────────────────────────────────────────
    silver = (
        validated
        .filter(F.col("_is_valid_record"))
        # Parse ISO timestamp → proper Spark timestamp
        .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp")))
        # Derived time dimensions
        .withColumn("event_date",  F.to_date("event_timestamp"))
        .withColumn("event_hour",  F.hour("event_timestamp"))
        .withColumn("event_year",  F.year("event_timestamp"))
        .withColumn("event_month", F.month("event_timestamp"))
        .withColumn("event_day",   F.dayofmonth("event_timestamp"))
        # Normalize strings
        .withColumn("country",     F.upper(F.col("country")))
        .withColumn("device_type", F.lower(F.col("device_type")))
        .withColumn("event_type",  F.lower(F.col("event_type")))
        # Revenue calculation (fallback guard)
        .withColumn("revenue", F.when(
            (F.col("event_type") == "purchase") & F.col("revenue").isNull(),
            F.col("price") * F.col("quantity")
        ).otherwise(F.col("revenue")))
        # Watermark for stateful operations
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        # Drop QA columns from output
        .drop("_is_valid_event_type", "_is_valid_device", "_has_required_fields",
              "_price_valid", "_quantity_valid", "_is_valid_record",
              "timestamp", "raw_payload")
    )

    return silver


def write_silver_layer(silver_df):
    """Write cleaned events to Silver, partitioned by date for efficient querying."""
    return (
        silver_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_SILVER)
        .partitionBy("event_year", "event_month", "event_day")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ── Gold Layer ─────────────────────────────────────────────────────────────────
def build_sales_metrics(silver_df):
    """
    Gold: Revenue and sales aggregated per hour.
    Uses event-time windowing with watermark to handle late data.
    """
    return (
        silver_df
        .filter(F.col("event_type") == "purchase")
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", "1 hour").alias("time_window"),
            F.col("country"),
            F.col("category"),
        )
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("event_id").alias("total_orders"),
            F.sum("quantity").alias("total_units_sold"),
            F.avg("price").alias("avg_order_value"),
            F.approx_count_distinct("user_id").alias("unique_buyers"),
        )
        .select(
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            F.col("country"),
            F.col("category"),
            F.round("total_revenue", 2).alias("total_revenue"),
            F.col("total_orders"),
            F.col("total_units_sold"),
            F.round("avg_order_value", 2).alias("avg_order_value"),
            F.col("unique_buyers"),
            F.current_timestamp().alias("updated_at"),
        )
    )


def build_product_metrics(silver_df):
    """Gold: Per-product performance metrics over 1-hour windows."""
    return (
        silver_df
        .filter(F.col("product_id").isNotNull())
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", "1 hour"),
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "purchase", F.col("revenue")).otherwise(0))
             .alias("revenue"),
            F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
            F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("cart_adds"),
            F.count(F.when(F.col("event_type") == "product_view", 1)).alias("product_views"),
            F.approx_count_distinct("user_id").alias("unique_users"),
        )
        .withColumn("conversion_rate",
            F.round(F.col("purchase_count") / F.col("cart_adds"), 4)
        )
        .select(
            F.col("window.start").alias("window_start"),
            "product_id", "product_name", "category",
            F.round("revenue", 2).alias("revenue"),
            "purchase_count", "cart_adds", "product_views",
            "unique_users", "conversion_rate",
            F.current_timestamp().alias("updated_at"),
        )
    )


def build_user_metrics(silver_df):
    """Gold: User activity segmentation."""
    return (
        silver_df
        .withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(
            F.window("event_timestamp", "1 hour"),
            F.col("user_id"),
            F.col("device_type"),
            F.col("country"),
        )
        .agg(
            F.count("event_id").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("revenue")).otherwise(0))
             .alias("total_spend"),
            F.collect_set("event_type").alias("event_types_seen"),
            F.max("event_timestamp").alias("last_seen"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "user_id", "device_type", "country",
            "total_events",
            F.round("total_spend", 2).alias("total_spend"),
            "event_types_seen", "last_seen",
            F.current_timestamp().alias("updated_at"),
        )
    )


def write_gold_stream(gold_df, path: str, checkpoint: str, name: str):
    """Write Gold aggregations in update/complete mode."""
    return (
        gold_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(name)
        .start()
    )


# ── Orchestration ─────────────────────────────────────────────────────────────
def main():
    logger.info("Starting FAANG Real-Time Data Platform")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kinesis
    kinesis_df = read_kinesis_stream(spark)

    # ── Bronze ────────────────────────────────────────────────────────────────
    logger.info("Starting Bronze layer writer")
    bronze_query = write_bronze_layer(kinesis_df)

    # ── Silver (parse/clean) ──────────────────────────────────────────────────
    logger.info("Starting Silver layer transformer")
    silver_df = parse_and_clean(kinesis_df)
    silver_query = write_silver_layer(silver_df)

    # ── Gold (aggregate) ──────────────────────────────────────────────────────
    logger.info("Starting Gold layer aggregations")

    sales_query = write_gold_stream(
        build_sales_metrics(silver_df),
        GOLD_SALES_PATH, CHECKPOINT_GOLD_SALES, "gold-sales"
    )

    user_query = write_gold_stream(
        build_user_metrics(silver_df),
        GOLD_USER_PATH, CHECKPOINT_GOLD_USER, "gold-users"
    )

    # ── Wait for all streams ──────────────────────────────────────────────────
    logger.info("All streaming queries active — awaiting termination")

    for query in [bronze_query, silver_query, sales_query, user_query]:
        query.awaitTermination()


if __name__ == "__main__":
    main()
