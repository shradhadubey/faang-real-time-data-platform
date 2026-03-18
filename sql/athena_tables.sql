-- ============================================================
-- FAANG Data Platform — Athena Table Definitions
-- ============================================================
-- Run these in the Athena console or via athena_utils.py
-- Workgroup: faang-data-platform-workgroup
-- Database:  faang_data_platform_db
-- ============================================================

-- ── Bronze: Raw Events ────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS faang_data_platform_db.bronze_events (
    raw_payload              STRING,
    kinesis_partition_key    STRING,
    kinesis_sequence_number  STRING,
    kinesis_ingestion_time   TIMESTAMP,
    processing_time          TIMESTAMP
)
PARTITIONED BY (
    ingest_year  INT,
    ingest_month INT,
    ingest_day   INT
)
STORED AS PARQUET
LOCATION 's3://faang-data-platform-dev-123456789/bronze/raw_events/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'classification'      = 'parquet'
);

-- ── Silver: Cleaned Events ────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS faang_data_platform_db.silver_events (
    event_id        STRING,
    user_id         STRING,
    session_id      STRING,
    event_type      STRING,
    product_id      STRING,
    product_name    STRING,
    category        STRING,
    price           DOUBLE,
    quantity        INT,
    device_type     STRING,
    country         STRING,
    revenue         DOUBLE,
    event_timestamp TIMESTAMP,
    event_date      DATE,
    event_hour      INT
)
PARTITIONED BY (
    event_year  INT,
    event_month INT,
    event_day   INT
)
STORED AS PARQUET
LOCATION 's3://faang-data-platform-dev-123456789/silver/cleaned_events/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'has_encrypted_data'  = 'false'
);

-- ── Gold: Hourly Sales Metrics ────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS faang_data_platform_db.gold_sales_metrics (
    window_start     TIMESTAMP,
    window_end       TIMESTAMP,
    country          STRING,
    category         STRING,
    total_revenue    DOUBLE,
    total_orders     BIGINT,
    total_units_sold BIGINT,
    avg_order_value  DOUBLE,
    unique_buyers    BIGINT,
    updated_at       TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://faang-data-platform-dev-123456789/gold/sales_metrics/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ── Gold: Daily Revenue (Batch) ───────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS faang_data_platform_db.gold_daily_revenue (
    event_hour        INT,
    country           STRING,
    category          STRING,
    hourly_revenue    DOUBLE,
    order_count       BIGINT,
    unique_customers  BIGINT,
    avg_price         DOUBLE,
    report_date       STRING
)
PARTITIONED BY (
    report_year  INT,
    report_month INT,
    report_day   INT
)
STORED AS PARQUET
LOCATION 's3://faang-data-platform-dev-123456789/gold/daily_revenue/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ── Gold: Top Products ────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS faang_data_platform_db.gold_top_products (
    product_id       STRING,
    product_name     STRING,
    category         STRING,
    daily_revenue    DOUBLE,
    purchase_count   BIGINT,
    cart_add_count   BIGINT,
    view_count       BIGINT,
    unique_users     BIGINT,
    conversion_rate  DOUBLE,
    revenue_rank     INT,
    report_date      STRING
)
PARTITIONED BY (
    report_year  INT,
    report_month INT,
    report_day   INT
)
STORED AS PARQUET
LOCATION 's3://faang-data-platform-dev-123456789/gold/daily_top_products/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Load partitions after Glue crawler runs
MSCK REPAIR TABLE faang_data_platform_db.silver_events;
MSCK REPAIR TABLE faang_data_platform_db.gold_daily_revenue;
MSCK REPAIR TABLE faang_data_platform_db.gold_top_products;
