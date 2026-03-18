# CloudWatch Monitoring & Alerting Guide
## FAANG Real-Time Data Platform

---

## Architecture

```
Application Logs → CloudWatch Log Groups → Metric Filters → Alarms → SNS → Slack/Email
```

---

## Log Groups

| Log Group | Retention | Purpose |
|-----------|-----------|---------|
| `/faang-data-platform/producer` | 7 days | Kinesis producer throughput & errors |
| `/faang-data-platform/spark-streaming` | 14 days | Spark job status, lag, checkpoints |
| `/faang-data-platform/batch` | 30 days | Nightly aggregation results |
| `/faang-data-platform/data-quality` | 30 days | GE check results |
| `/faang-data-platform/airflow` | 30 days | DAG runs and task outcomes |

---

## Key Metrics & Alarms

### 1. Kinesis Data Stream

```json
{
  "AlarmName": "kinesis-iterator-age-high",
  "Namespace": "AWS/Kinesis",
  "MetricName": "GetRecords.IteratorAgeMilliseconds",
  "Statistic": "Maximum",
  "Threshold": 60000,
  "ComparisonOperator": "GreaterThanThreshold",
  "EvaluationPeriods": 3,
  "Period": 60,
  "AlarmDescription": "Consumer is >60s behind — Spark job may be down or under-provisioned"
}
```

```json
{
  "AlarmName": "kinesis-write-throttle",
  "MetricName": "WriteProvisionedThroughputExceeded",
  "Statistic": "Sum",
  "Threshold": 10,
  "EvaluationPeriods": 2,
  "Period": 60,
  "AlarmDescription": "Write throttling detected — increase shard count"
}
```

### 2. Spark Streaming Health (Custom Metrics)

The `spark_streaming_job.py` publishes these metrics via `boto3.put_metric_data`:

```python
cloudwatch = boto3.client("cloudwatch")

cloudwatch.put_metric_data(
    Namespace="FANGDataPlatform/Streaming",
    MetricData=[
        {
            "MetricName": "RecordsProcessedPerSecond",
            "Value": actual_rps,
            "Unit": "Count/Second",
        },
        {
            "MetricName": "BatchProcessingTimeMs",
            "Value": batch_ms,
            "Unit": "Milliseconds",
        },
        {
            "MetricName": "CheckpointLagSeconds",
            "Value": checkpoint_lag,
            "Unit": "Seconds",
        },
    ]
)
```

**Corresponding Alarms:**

```json
{
  "AlarmName": "spark-batch-processing-slow",
  "Namespace": "FANGDataPlatform/Streaming",
  "MetricName": "BatchProcessingTimeMs",
  "Statistic": "Average",
  "Threshold": 120000,
  "ComparisonOperator": "GreaterThanThreshold",
  "EvaluationPeriods": 5,
  "AlarmDescription": "Spark micro-batch taking >2 min — check EMR cluster resources"
}
```

### 3. S3 Data Lake Freshness

```json
{
  "AlarmName": "silver-layer-no-new-data",
  "Namespace": "FANGDataPlatform/DataLake",
  "MetricName": "SilverLayerLastWriteAgeSeconds",
  "Statistic": "Maximum",
  "Threshold": 300,
  "ComparisonOperator": "GreaterThanThreshold",
  "EvaluationPeriods": 3,
  "AlarmDescription": "No new Silver records for >5 min — streaming pipeline may be down"
}
```

### 4. Data Quality Alerts

```json
{
  "AlarmName": "dq-null-rate-breach",
  "Namespace": "FANGDataPlatform/DataQuality",
  "MetricName": "NullEventIdPct",
  "Statistic": "Maximum",
  "Threshold": 0.5,
  "ComparisonOperator": "GreaterThanThreshold",
  "EvaluationPeriods": 1,
  "AlarmDescription": "Null event_id rate exceeded 0.5% — investigate upstream schema change"
}
```

### 5. Airflow DAG Failures

```json
{
  "AlarmName": "airflow-dag-failure",
  "Namespace": "AmazonMWAA",
  "MetricName": "DAGTaskFailures",
  "Statistic": "Sum",
  "Threshold": 1,
  "ComparisonOperator": "GreaterThanOrEqualToThreshold",
  "EvaluationPeriods": 1,
  "Period": 300,
  "AlarmDescription": "Airflow task failed — check DAG faang_data_platform_daily"
}
```

---

## Metric Filters (Log → Metric)

Add these to the CloudWatch Log Groups:

### ERROR rate from producer logs
```
Filter pattern: [timestamp, level="ERROR", ...]
Metric name:    ProducerErrorCount
Metric value:   1
```

### Batch job completion signal
```
Filter pattern: "Batch job completed successfully"
Metric name:    BatchJobSuccess
Metric value:   1
```

### Data quality FAIL
```
Filter pattern: "Suite FAILED"
Metric name:    DataQualityFailures
Metric value:   1
```

---

## Dashboard Configuration

Create a CloudWatch Dashboard named `FAANG-DataPlatform`:

```json
{
  "widgets": [
    {
      "type": "metric",
      "title": "Kinesis Throughput",
      "properties": {
        "metrics": [
          ["AWS/Kinesis", "IncomingRecords", "StreamName", "faang-ecommerce-events"],
          ["AWS/Kinesis", "GetRecords.Records"]
        ],
        "period": 60,
        "stat": "Sum",
        "view": "timeSeries"
      }
    },
    {
      "type": "metric",
      "title": "Iterator Age (Consumer Lag)",
      "properties": {
        "metrics": [
          ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", "faang-ecommerce-events"]
        ],
        "period": 60,
        "stat": "Maximum"
      }
    },
    {
      "type": "metric",
      "title": "Spark Records/Second",
      "properties": {
        "metrics": [
          ["FANGDataPlatform/Streaming", "RecordsProcessedPerSecond"]
        ],
        "period": 60,
        "stat": "Average"
      }
    },
    {
      "type": "alarm",
      "title": "Active Alarms",
      "properties": {
        "alarms": [
          "arn:aws:cloudwatch:us-east-1:ACCOUNT:alarm:kinesis-iterator-age-high",
          "arn:aws:cloudwatch:us-east-1:ACCOUNT:alarm:spark-batch-processing-slow",
          "arn:aws:cloudwatch:us-east-1:ACCOUNT:alarm:dq-null-rate-breach"
        ]
      }
    }
  ]
}
```

---

## SNS Notification Setup

```bash
# Create SNS topic
aws sns create-topic --name faang-platform-alerts

# Subscribe your email
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT:faang-platform-alerts \
  --protocol email \
  --notification-endpoint your@email.com

# Subscribe Slack (via AWS Chatbot or Lambda webhook)
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT:faang-platform-alerts \
  --protocol https \
  --notification-endpoint https://hooks.slack.com/services/YOUR/WEBHOOK
```

---

## Runbook: Responding to Alerts

### Alert: `kinesis-iterator-age-high`
1. Check Spark streaming logs: `aws logs tail /faang-data-platform/spark-streaming`
2. Verify EMR cluster is running: `aws emr list-clusters --cluster-states RUNNING`
3. Check for throughput exceptions in producer logs
4. If cluster down: restart streaming job or trigger new EMR cluster via Step Functions

### Alert: `dq-null-rate-breach`
1. Query DQ report: `SELECT * FROM gold_dq_reports ORDER BY report_date DESC LIMIT 1`
2. Check Bronze raw payload for schema changes: `SELECT raw_payload FROM bronze_events LIMIT 10`
3. Review producer schema version field
4. Update Silver parser if upstream schema evolved
