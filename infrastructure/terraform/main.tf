################################################################################
# FAANG Real-Time Data Platform — Terraform Infrastructure
# Provisions: S3, Kinesis, IAM, Glue, Athena, CloudWatch
################################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state — update bucket/key for your account
  backend "s3" {
    bucket         = "faang-platform-tfstate-sd"
    key            = "data-platform/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "faang-platform-tflock"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "FAANG-DataPlatform"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  region      = data.aws_region.current.name
  bucket_name = "${var.project_name}-${var.environment}-${local.account_id}"
}

################################################################################
# S3 — Data Lake Buckets
################################################################################

resource "aws_s3_bucket" "data_lake" {
  bucket        = local.bucket_name
  force_destroy = var.environment != "production"
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle: auto-transition old data to cheaper storage tiers
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-archival"
    status = "Enabled"
    filter { prefix = "bronze/" }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "checkpoints-expiry"
    status = "Enabled"
    filter { prefix = "_checkpoints/" }

    expiration {
      days = 7
    }
  }
}

# Folder structure (marker objects)
resource "aws_s3_object" "layer_prefixes" {
  for_each = toset([
    "bronze/raw_events/",
    "silver/cleaned_events/",
    "gold/sales_metrics/",
    "gold/user_metrics/",
    "gold/product_metrics/",
    "gold/daily_revenue/",
    "gold/daily_top_products/",
    "gold/daily_user_cohorts/",
    "gold/dq_reports/",
    "_checkpoints/",
  ])
  bucket  = aws_s3_bucket.data_lake.id
  key     = each.value
  content = ""
}

################################################################################
# SQS Queue (Free Tier — replaces Kinesis)
################################################################################

resource "aws_sqs_queue" "events" {
  name                       = "${var.project_name}-ecommerce-events"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400   # 1 day
  receive_wait_time_seconds  = 20      # long polling — reduces API calls

  tags = {
    Project = "FAANG-DataPlatform"
  }
}

output "sqs_queue_url" {
  description = "SQS Queue URL for the producer"
  value       = aws_sqs_queue.events.url
}

output "sqs_queue_arn" {
  description = "SQS Queue ARN"
  value       = aws_sqs_queue.events.arn
}

################################################################################
# IAM Roles & Policies
################################################################################

# ── Spark / EMR Role ──────────────────────────────────────────────────────────
resource "aws_iam_role" "spark_role" {
  name = "${var.project_name}-spark-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "spark_s3" {
  name = "spark-s3-access"
  role = aws_iam_role.spark_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage", "sqs:DeleteMessage",
          "sqs:GetQueueAttributes", "sqs:GetQueueUrl",
        ]
        Resource = aws_sqs_queue.events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData", "logs:*"]
        Resource = "*"
      }
    ]
  })
}

# ── Glue Role ─────────────────────────────────────────────────────────────────
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:*"]
      Resource = [
        aws_s3_bucket.data_lake.arn,
        "${aws_s3_bucket.data_lake.arn}/*",
      ]
    }]
  })
}

# ── Producer Role (EC2 / Lambda) ──────────────────────────────────────────────
resource "aws_iam_role" "producer_role" {
  name = "${var.project_name}-producer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "producer_kinesis" {
  name = "producer-kinesis-write"
  role = aws_iam_role.producer_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["sqs:SendMessage", "sqs:SendMessageBatch", "sqs:GetQueueUrl"]
      Resource = aws_sqs_queue.events.arn
    }]
  })
}

################################################################################
# AWS Glue Data Catalog
################################################################################

resource "aws_glue_catalog_database" "data_platform" {
  name        = "${replace(var.project_name, "-", "_")}_db"
  description = "FAANG Real-Time Data Platform — Glue Catalog"
}

# Bronze table
resource "aws_glue_catalog_table" "bronze_events" {
  name          = "bronze_events"
  database_name = aws_glue_catalog_database.data_platform.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"  = "parquet"
    "EXTERNAL"        = "TRUE"
    "parquet.compress" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/bronze/raw_events/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }

    columns {
      name = "raw_payload"
      type = "string"
    }
    columns {
      name = "kinesis_partition_key"
      type = "string"
    }
    columns {
      name = "kinesis_sequence_number"
      type = "string"
    }
    columns {
      name = "kinesis_ingestion_time"
      type = "timestamp"
    }
    columns {
      name = "processing_time"
      type = "timestamp"
    }
  }

  partition_keys {
    name = "ingest_year"
    type = "int"
  }
  partition_keys {
    name = "ingest_month"
    type = "int"
  }
  partition_keys {
    name = "ingest_day"
    type = "int"
  }
}

# Silver table
resource "aws_glue_catalog_table" "silver_events" {
  name          = "silver_events"
  database_name = aws_glue_catalog_database.data_platform.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"   = "parquet"
    "EXTERNAL"         = "TRUE"
    "parquet.compress" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/silver/cleaned_events/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }
    
    columns {
          name = "event_id"
          type = "string"
        }
        columns {
          name = "user_id"
          type = "string"
        }
        columns {
          name = "session_id"
          type = "string"
        }
        columns {
          name = "event_type"
          type = "string"
        }
        columns {
          name = "product_id"
          type = "string"
        }
        columns {
          name = "product_name"
          type = "string"
        }
        columns {
          name = "category"
          type = "string"
        }
        columns {
          name = "price"
          type = "double"
        }
        columns {
          name = "quantity"
          type = "int"
        }
        columns {
          name = "device_type"
          type = "string"
        }
        columns {
          name = "country"
          type = "string"
        }
        columns {
          name = "revenue"
          type = "double"
        }
        columns {
          name = "event_timestamp"
          type = "timestamp"
        }
        columns {
          name = "event_date"
          type = "date"
        }
        columns {
          name = "event_hour"
          type = "int"
        }
  }

  partition_keys {
    name = "event_year"
    type = "int"
  }
  partition_keys {
    name = "event_month"
    type = "int"
  }
  partition_keys {
    name = "event_day"
    type = "int"
  }
}

# Gold sales metrics table
resource "aws_glue_catalog_table" "gold_sales_metrics" {
  name          = "gold_sales_metrics"
  database_name = aws_glue_catalog_database.data_platform.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"   = "parquet"
    "EXTERNAL"         = "TRUE"
    "parquet.compress" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/gold/sales_metrics/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters            = { "serialization.format" = "1" }
    }

    columns {
          name = "window_start"
          type = "timestamp"
        }
        columns {
          name = "window_end"
          type = "timestamp"
        }
        columns {
          name = "country"
          type = "string"
        }
        columns {
          name = "category"
          type = "string"
        }
        columns {
          name = "total_revenue"
          type = "double"
        }
        columns {
          name = "total_orders"
          type = "bigint"
        }
        columns {
          name = "total_units_sold"
          type = "bigint"
        }
        columns {
          name = "avg_order_value"
          type = "double"
        }
        columns {
          name = "unique_buyers"
          type = "bigint"
        }
        columns {
          name = "updated_at"
          type = "timestamp"
        }
      }
    }

################################################################################
# Athena
################################################################################

resource "aws_athena_workgroup" "data_platform" {
  name        = "${var.project_name}-workgroup"
  description = "FAANG Data Platform queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}



################################################################################
# Glue Crawler — auto-discover partitions
################################################################################

resource "aws_glue_crawler" "silver_crawler" {
  name          = "${var.project_name}-silver-crawler"
  database_name = aws_glue_catalog_database.data_platform.name
  role          = aws_iam_role.glue_role.arn
  description   = "Discovers new Silver partitions daily"

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/silver/cleaned_events/"
  }

  schedule = "cron(0 2 * * ? *)"  # 2 AM UTC daily

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

resource "aws_glue_crawler" "gold_crawler" {
  name          = "${var.project_name}-gold-crawler"
  database_name = aws_glue_catalog_database.data_platform.name
  role          = aws_iam_role.glue_role.arn
  description   = "Discovers new Gold partitions"

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/gold/"
  }

  schedule = "cron(30 2 * * ? *)"
}

################################################################################
# CloudWatch — Monitoring & Alerts
################################################################################

resource "aws_cloudwatch_log_group" "data_platform" {
  name              = "/faang-data-platform"
  retention_in_days = 30
}

################################################################################
# Outputs
################################################################################

output "s3_bucket_name" {
  description = "Data Lake S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "glue_database_name" {
  description = "Glue catalog database"
  value       = aws_glue_catalog_database.data_platform.name
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.data_platform.name
}

output "spark_role_arn" {
  description = "IAM role ARN for Spark/EMR"
  value       = aws_iam_role.spark_role.arn
}


