-- ============================================================
-- FAANG Data Platform — Athena Table DDLs
-- ============================================================
-- Run in Athena Console → faang_data_platform_db
-- Replace YOUR-BUCKET-NAME before running
-- ============================================================

-- ── Gold: Revenue Metrics ─────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS gold_revenue_metrics (
    event_hour       INT,
    country          STRING,
    category         STRING,
    total_revenue    DOUBLE,
    total_orders     BIGINT,
    total_units_sold BIGINT,
    avg_order_value  DOUBLE,
    unique_buyers    BIGINT,
    report_date      STRING,
    created_at       STRING
)
PARTITIONED BY (report_year INT, report_month INT, report_day INT)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET-NAME/gold/revenue_metrics/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ── Gold: Top Products ────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS gold_top_products (
    product_id      STRING,
    product_name    STRING,
    category        STRING,
    revenue         DOUBLE,
    purchase_count  BIGINT,
    cart_adds       BIGINT,
    product_views   BIGINT,
    unique_users    BIGINT,
    conversion_rate DOUBLE,
    revenue_rank    INT,
    report_date     STRING,
    created_at      STRING
)
PARTITIONED BY (report_year INT, report_month INT, report_day INT)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET-NAME/gold/top_products/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ── Gold: User Segments ───────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS gold_user_segments (
    user_id        STRING,
    device_type    STRING,
    country        STRING,
    daily_spend    DOUBLE,
    order_count    BIGINT,
    event_count    BIGINT,
    session_count  BIGINT,
    user_segment   STRING,
    report_date    STRING,
    created_at     STRING
)
PARTITIONED BY (report_year INT, report_month INT, report_day INT)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET-NAME/gold/user_segments/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ── Silver: Cleaned Events ────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS silver_events (
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
    event_date      STRING,
    event_hour      INT,
    schema_version  STRING,
    processed_at    STRING
)
PARTITIONED BY (event_year INT, event_month INT, event_day INT)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET-NAME/silver/cleaned_events/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ── Load all partitions ───────────────────────────────────────────────────────
MSCK REPAIR TABLE gold_revenue_metrics;
MSCK REPAIR TABLE gold_top_products;
MSCK REPAIR TABLE gold_user_segments;
MSCK REPAIR TABLE silver_events;
