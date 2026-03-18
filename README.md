# 🚀 Real-Time Data Platform — FAANG Architecture

[![CI/CD](https://github.com/yourusername/faang-real-time-data-platform/actions/workflows/ci_cd.yml/badge.svg)](https://github.com/yourusername/faang-real-time-data-platform/actions)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?logo=terraform)](infrastructure/terraform)
[![PySpark](https://img.shields.io/badge/Streaming-PySpark-E25A1C?logo=apache-spark)](streaming/spark_streaming_job.py)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](data-generator)
[![AWS](https://img.shields.io/badge/Cloud-AWS-FF9900?logo=amazon-aws)](infrastructure)

> A production-grade, real-time data platform processing **1,000+ events/second** through a Medallion architecture on AWS — built to demonstrate Senior Data Engineer skills at FAANG scale.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FAANG Real-Time Data Platform                        │
│                                                                               │
│  ┌──────────────┐    ┌─────────────────┐    ┌──────────────────────────────┐ │
│  │  E-Commerce  │    │    Kinesis       │    │    PySpark Structured        │ │
│  │  Event       │───▶│    Data          │───▶│    Streaming (EMR)           │ │
│  │  Generator   │    │    Streams       │    │                              │ │
│  │  (Python)    │    │  2 Shards        │    │  60-second micro-batches     │ │
│  │  1000 RPS    │    │  1MB/s write     │    │  Watermark: 10 min           │ │
│  └──────────────┘    └─────────────────┘    └──────────────────────────────┘ │
│                                                          │                    │
│                              ┌───────────────────────────┤                   │
│                              │                           │                   │
│                    ┌─────────▼────────┐      ┌──────────▼────────────────┐   │
│                    │   BRONZE Layer   │      │      SILVER Layer          │   │
│                    │  s3://…/bronze/  │      │  s3://…/silver/            │   │
│                    │                  │      │                             │   │
│                    │  Raw JSON        │      │  Parsed + validated        │   │
│                    │  + Kinesis meta  │      │  Typed schema               │   │
│                    │  Partitioned     │      │  Null/range checks         │   │
│                    │  by ingest date  │      │  Partitioned by event date │   │
│                    └──────────────────┘      └────────────┬───────────────┘   │
│                                                           │                   │
│                                             ┌─────────────▼───────────────┐   │
│                                             │        GOLD Layer            │   │
│                                             │  s3://…/gold/               │   │
│                                             │                              │   │
│                                             │  ├─ sales_metrics/          │   │
│                                             │  ├─ user_metrics/           │   │
│                                             │  ├─ product_metrics/        │   │
│                                             │  └─ dq_reports/             │   │
│                                             └─────────────┬───────────────┘   │
│                                                           │                   │
│              ┌─────────────────────┐          ┌──────────▼───────────────┐    │
│              │   Apache Airflow    │          │   AWS Glue Data Catalog  │    │
│              │                     │          │                          │    │
│              │  Daily batch DAG    │          │  bronze_events           │    │
│              │  EMR orchestration  │          │  silver_events           │    │
│              │  DQ validation      │          │  gold_sales_metrics      │    │
│              └─────────────────────┘          └──────────┬───────────────┘    │
│                                                          │                    │
│                                             ┌────────────▼───────────────┐    │
│                                             │      Amazon Athena          │    │
│                                             │                             │    │
│                                             │  Serverless SQL queries    │    │
│                                             │  Standard workgroup        │    │
│                                             └────────────┬───────────────┘    │
│                                                          │                    │
│                                             ┌────────────▼───────────────┐    │
│                                             │   QuickSight / Power BI    │    │
│                                             │                             │    │
│                                             │  Revenue dashboard         │    │
│                                             │  Product analytics         │    │
│                                             │  Geographic activity map   │    │
│                                             └─────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏗️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Ingestion** | Python + Boto3 | 1,000 RPS event simulation |
| **Streaming** | AWS Kinesis Data Streams | Durable, ordered message queue |
| **Processing** | PySpark Structured Streaming on EMR | Medallion transformations |
| **Storage** | Amazon S3 + Parquet + Snappy | Cost-efficient columnar data lake |
| **Catalog** | AWS Glue Data Catalog | Schema registry, partition management |
| **Query** | Amazon Athena (serverless) | Ad-hoc + dashboard SQL |
| **Orchestration** | Apache Airflow (MWAA) | DAG scheduling, lineage |
| **Quality** | Great Expectations | Schema, null, range validations |
| **IaC** | Terraform | Reproducible AWS infrastructure |
| **Monitoring** | Amazon CloudWatch | Metrics, alarms, dashboards |
| **CI/CD** | GitHub Actions | Lint, test, plan, deploy |

---

## 📁 Repository Structure

```
faang-real-time-data-platform/
│
├── architecture/
│   └── architecture_diagram.png          # System architecture visual
│
├── data-generator/
│   └── streaming_producer.py             # Kinesis event producer (1000 RPS)
│
├── streaming/
│   └── spark_streaming_job.py            # Bronze → Silver → Gold streaming
│
├── batch/
│   └── aggregation_job.py                # Nightly daily rollup + DQ report
│
├── infrastructure/
│   └── terraform/
│       ├── main.tf                        # S3, Kinesis, IAM, Glue, Athena
│       └── variables.tf
│
├── orchestration/
│   └── airflow_dag.py                     # Nightly batch pipeline DAG
│
├── data_quality/
│   └── great_expectations_suite.py       # Schema + null + range checks
│
├── sql/
│   ├── athena_tables.sql                  # External table DDLs
│   └── analytics_queries.sql             # 11 analytics queries
│
├── monitoring/
│   └── cloudwatch_config.md              # Alarms, dashboards, runbooks
│
├── .github/
│   └── workflows/
│       └── ci_cd.yml                      # 5-job CI/CD pipeline
│
└── README.md
```

---

## ⚡ Quick Start (Under 8 Hours)

### Prerequisites

```bash
# Install required tools
brew install terraform awscli python@3.11
pip install boto3 pyspark great-expectations apache-airflow

# Configure AWS
aws configure
```

### Step 1 — Deploy Infrastructure (~15 min)

```bash
cd infrastructure/terraform

# Initialize
terraform init \
  -backend-config="bucket=YOUR-TFSTATE-BUCKET" \
  -backend-config="region=us-east-1"

# Preview resources
terraform plan -var="environment=dev"

# Deploy (creates S3, Kinesis, IAM, Glue, Athena)
terraform apply -var="environment=dev" -auto-approve

# Save outputs
terraform output
```

**Resources created:** 1× S3 bucket, 1× Kinesis stream (2 shards), 3× IAM roles, 1× Glue database, 3× Glue tables, 2× Glue crawlers, 1× Athena workgroup, 2× CloudWatch alarms

**Estimated cost:** ~$3–5/day (dev environment, Kinesis dominates)

### Step 2 — Start Event Producer (~2 min)

```bash
cd data-generator

# Test locally (no AWS calls)
python streaming_producer.py --dry-run

# Start producing to Kinesis (1,000 events/second)
python streaming_producer.py \
  --stream faang-data-platform-dev-ecommerce-events \
  --rps 1000

# Output:
# 2024-01-15T10:00:01 [INFO] Stats | sent=10,000 | failed=0 | actual_rps=998
```

### Step 3 — Launch Spark Streaming (~10 min)

```bash
# Submit to EMR (replace CLUSTER-ID from Terraform output or create via console)
aws emr add-steps \
  --cluster-id j-XXXXXXXXXX \
  --steps '[{
    "Name": "spark-streaming",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "cluster",
        "s3://YOUR-BUCKET/scripts/spark_streaming_job.py"
      ]
    }
  }]'
```

Data will begin appearing in S3 within ~60 seconds:
- `s3://YOUR-BUCKET/bronze/raw_events/ingest_year=.../`
- `s3://YOUR-BUCKET/silver/cleaned_events/event_year=.../`
- `s3://YOUR-BUCKET/gold/sales_metrics/`

### Step 4 — Query with Athena (~2 min)

```bash
# Create tables
aws athena start-query-execution \
  --query-string file://sql/athena_tables.sql \
  --work-group faang-data-platform-workgroup \
  --query-execution-context Database=faang_data_platform_db

# Run analytics query
aws athena start-query-execution \
  --query-string "SELECT product_id, SUM(price * quantity) AS revenue
                  FROM gold_sales_metrics
                  GROUP BY product_id ORDER BY revenue DESC LIMIT 10" \
  --work-group faang-data-platform-workgroup
```

### Step 5 — Run Data Quality Checks (~3 min)

```bash
cd data_quality

# Validate yesterday's Silver data
python great_expectations_suite.py --date $(date -d "yesterday" +%Y-%m-%d) --layer all

# Sample output:
# ============================================================
# DATA QUALITY REPORT — 2024-01-14
# ============================================================
#   ✓ PASS  silver_events_suite  (100.0%  |  0 failed)
#   ✓ PASS  gold_sales_suite     (100.0%  |  0 failed)
# ============================================================
```

### Step 6 — Set Up Airflow DAG (~10 min)

```bash
# Copy DAG to your MWAA environment
aws s3 cp orchestration/airflow_dag.py s3://YOUR-MWAA-BUCKET/dags/

# The DAG will appear in Airflow UI within 1-2 minutes
# Toggle it ON to enable nightly scheduling
```

---

## 📊 Sample Analytics Queries

### Top Products by Revenue

```sql
SELECT
    product_id,
    product_name,
    category,
    SUM(daily_revenue)  AS total_revenue,
    SUM(purchase_count) AS orders,
    ROUND(AVG(conversion_rate), 4) AS avg_conversion
FROM faang_data_platform_db.gold_top_products
WHERE report_year  = 2024
  AND report_month = 1
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC
LIMIT 10;
```

### Real-Time Revenue (Last 2 Hours)

```sql
SELECT
    window_start,
    SUM(total_revenue) AS revenue,
    SUM(total_orders)  AS orders,
    SUM(unique_buyers) AS buyers
FROM faang_data_platform_db.gold_sales_metrics
WHERE window_start >= NOW() - INTERVAL '2' HOUR
GROUP BY window_start
ORDER BY window_start DESC;
```

### Purchase Funnel

```sql
SELECT
    event_type,
    COUNT(DISTINCT user_id)  AS users,
    ROUND(COUNT(DISTINCT user_id) * 100.0
          / MAX(COUNT(DISTINCT user_id)) OVER (), 2) AS funnel_pct
FROM faang_data_platform_db.silver_events
WHERE event_year = 2024 AND event_month = 1 AND event_day = 15
GROUP BY event_type
ORDER BY users DESC;
```

---

## 📈 Performance Characteristics

| Metric | Value |
|--------|-------|
| Producer throughput | 1,000 events/sec (scalable to 10K+) |
| Kinesis shards | 2 (2 MB/s write, 4 MB/s read) |
| Spark micro-batch interval | 60 seconds |
| Late data watermark | 10 minutes |
| S3 compression | Snappy Parquet (~70% compression vs JSON) |
| Athena query cost | ~$5 / TB scanned |
| Bronze storage (30 days) | ~50 GB (auto-transitions to Glacier) |

---

## 🔧 Data Quality Framework

Three-tier validation using Great Expectations:

| Check Type | Examples | Threshold |
|------------|---------|-----------|
| **Completeness** | `event_id`, `user_id`, `timestamp` not null | 100% |
| **Uniqueness** | No duplicate `event_id` values | 0 duplicates |
| **Value sets** | `event_type` ∈ valid enum, country = 2-char ISO | 100% |
| **Ranges** | `price` ∈ [0.01, 50000], `quantity` ∈ [1, 100] | 99% |
| **Volume** | Row count between 1K and 50M | Hard fail |

---

## 🏗️ Infrastructure Details

| Resource | Config | Monthly Cost (Dev) |
|----------|--------|--------------------|
| S3 Data Lake | Standard + lifecycle to IA→Glacier | ~$2 |
| Kinesis (2 shards) | On-demand pricing | ~$22 |
| Glue Crawlers | 2× daily runs | ~$1 |
| Athena | Pay per query ($5/TB) | ~$5 |
| EMR (Spot instances) | m5.xlarge × 3, transient | ~$10 |
| CloudWatch | Logs + 5 alarms | ~$3 |
| **Total** | | **~$43/month** |

---


## 📄 License

MIT License — free to use as a portfolio project or learning resource.
