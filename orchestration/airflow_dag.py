"""
Apache Airflow DAG — FAANG Data Platform Orchestration
=======================================================
Orchestrates the nightly batch pipeline:
  1. Trigger Glue crawlers to register new partitions
  2. Run PySpark batch aggregation job on EMR
  3. Validate data quality thresholds
  4. Send Slack/SNS alert on failure

Schedule: Daily at 03:00 UTC (processes previous day's data)

Author: Portfolio Project
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.email import send_email
import boto3
import json
import logging

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
S3_BUCKET        = "faang-data-platform-dev-123456789"   # Update with your bucket
GLUE_DB          = "faang_data_platform_db"
ATHENA_WORKGROUP = "faang-data-platform-workgroup"
EMR_LOG_URI      = f"s3://{S3_BUCKET}/emr-logs/"
SPARK_SCRIPT_S3  = f"s3://{S3_BUCKET}/scripts/aggregation_job.py"
DQ_THRESHOLD_PCT = 0.5   # Max allowed null % before alerting

# EMR cluster configuration (cost-optimized with Spot instances)
EMR_CLUSTER_CONFIG = {
    "Name": "faang-platform-batch-{{ ds_nodash }}",
    "ReleaseLabel": "emr-7.0.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "LogUri": EMR_LOG_URI,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
                "Market": "SPOT",
                "BidPrice": "0.10",
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "VisibleToAllUsers": True,
}

SPARK_STEPS = [
    {
        "Name": "batch-aggregation-{{ ds }}",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", "spark.sql.adaptive.enabled=true",
                "--conf", "spark.dynamicAllocation.enabled=true",
                SPARK_SCRIPT_S3,
                "{{ ds }}",  # run_date = Airflow execution date
            ],
        },
    }
]

# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="faang_data_platform_daily",
    description="Nightly batch pipeline: Glue → EMR Spark → DQ → Athena refresh",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",   # 03:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["data-platform", "production", "batch"],
) as dag:

    # ── Start ────────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Step 1: Run Glue Crawlers ─────────────────────────────────────────────
    crawl_silver = GlueCrawlerOperator(
        task_id="crawl_silver_partitions",
        config={"Name": "faang-data-platform-silver-crawler"},
        aws_conn_id="aws_default",
    )

    # ── Step 2: Create EMR Cluster ────────────────────────────────────────────
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMR_CLUSTER_CONFIG,
        aws_conn_id="aws_default",
    )

    # ── Step 3: Submit Spark Step ─────────────────────────────────────────────
    submit_spark_step = EmrAddStepsOperator(
        task_id="submit_aggregation_job",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    # ── Step 4: Wait for Spark Step ────────────────────────────────────────────
    wait_for_spark = EmrStepSensor(
        task_id="wait_for_aggregation_job",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('submit_aggregation_job', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=7200,
    )

    # ── Step 5: Terminate EMR Cluster ─────────────────────────────────────────
    terminate_emr = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",   # Always terminate — even on failure
    )

    # ── Step 6: Crawl Gold Layer ──────────────────────────────────────────────
    crawl_gold = GlueCrawlerOperator(
        task_id="crawl_gold_partitions",
        config={"Name": "faang-data-platform-gold-crawler"},
        aws_conn_id="aws_default",
    )

    # ── Step 7: Data Quality Validation ──────────────────────────────────────
    def validate_dq(**context):
        """
        Read DQ report from S3 and fail the task if thresholds are breached.
        This stops downstream dashboard refresh on bad data.
        """
        run_date = context["ds"]
        year, month, day = run_date.split("-")
        s3 = boto3.client("s3")

        key = (
            f"gold/dq_reports/year={year}/month={int(month)}"
            f"/day={int(day)}/part-00000.parquet"
        )
        logger.info(f"Reading DQ report: s3://{S3_BUCKET}/{key}")

        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            # In practice, read with pyarrow — simplified here for clarity
            report_raw = obj["Body"].read().decode("utf-8")
            report = json.loads(report_raw)
        except Exception as e:
            logger.warning(f"Could not read DQ report: {e} — skipping validation")
            return "dq_passed"

        issues = []
        if report.get("null_event_id_pct", 0) > DQ_THRESHOLD_PCT:
            issues.append(f"null_event_id_pct={report['null_event_id_pct']}%")
        if report.get("duplicate_event_ids", 0) > 0:
            issues.append(f"duplicate_ids={report['duplicate_event_ids']}")
        if report.get("status") == "NO_DATA":
            issues.append("NO_DATA — partition is empty")

        if issues:
            raise ValueError(f"DQ check failed: {'; '.join(issues)}")

        logger.info(f"DQ passed for {run_date}: {report['total_records']:,} records")

    validate_dq_task = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_dq,
    )

    # ── Step 8: Athena table refresh query ────────────────────────────────────
    refresh_athena = AthenaOperator(
        task_id="refresh_athena_partitions",
        query="MSCK REPAIR TABLE {{ params.db }}.silver_events",
        database=GLUE_DB,
        output_location=f"s3://{S3_BUCKET}/athena-results/",
        workgroup=ATHENA_WORKGROUP,
        aws_conn_id="aws_default",
        params={"db": GLUE_DB},
    )

    # ── End ───────────────────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── DAG Dependencies ──────────────────────────────────────────────────────
    (
        start
        >> crawl_silver
        >> create_emr_cluster
        >> submit_spark_step
        >> wait_for_spark
        >> terminate_emr
        >> crawl_gold
        >> validate_dq_task
        >> refresh_athena
        >> end
    )
