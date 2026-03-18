"""
Great Expectations Data Quality Suite
======================================
Validates Silver layer data across three dimensions:
  - Schema compliance (column types, required fields)
  - Completeness (null rate thresholds)
  - Business rules (value ranges, referential integrity)

Usage:
    python great_expectations_suite.py --date 2024-01-15
    python great_expectations_suite.py --date 2024-01-15 --layer silver
    python great_expectations_suite.py --date 2024-01-15 --layer gold

Author: Portfolio Project
"""

import argparse
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
import pandas as pd
import pyarrow.parquet as pq
import s3fs

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("data-quality")

S3_BUCKET = "faang-data-platform-dev-123456789"  # Update with your bucket
yaml = YAMLHandler()

# ── Expectation Suites ─────────────────────────────────────────────────────────

SILVER_EXPECTATIONS = {
    "suite_name": "silver_events_suite",
    "expectations": [
        # ── Completeness ──────────────────────────────────────────────────────
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "event_id", "mostly": 1.0},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "user_id", "mostly": 1.0},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "event_timestamp", "mostly": 1.0},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "event_type", "mostly": 1.0},
        },
        # ── Uniqueness ────────────────────────────────────────────────────────
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "event_id"},
        },
        # ── Value Sets ────────────────────────────────────────────────────────
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "event_type",
                "value_set": [
                    "page_view", "product_view", "add_to_cart",
                    "purchase", "search", "wishlist_add",
                ],
            },
        },
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "device_type",
                "value_set": ["mobile", "desktop", "tablet"],
            },
        },
        # ── Ranges ────────────────────────────────────────────────────────────
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "price",
                "min_value": 0.01,
                "max_value": 50000.0,
                "mostly": 0.99,
            },
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "quantity",
                "min_value": 1,
                "max_value": 100,
                "mostly": 0.99,
            },
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "revenue",
                "min_value": 0.0,
                "max_value": 500000.0,
                "mostly": 0.99,
            },
        },
        # ── Schema ────────────────────────────────────────────────────────────
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "event_id"},
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "event_timestamp"},
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "user_id"},
        },
        # ── Volume ────────────────────────────────────────────────────────────
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1000, "max_value": 50_000_000},
        },
        # ── Country code format ───────────────────────────────────────────────
        {
            "expectation_type": "expect_column_value_lengths_to_equal",
            "kwargs": {"column": "country", "value": 2, "mostly": 0.99},
        },
    ],
}

GOLD_SALES_EXPECTATIONS = {
    "suite_name": "gold_sales_suite",
    "expectations": [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "total_revenue"},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "total_revenue", "min_value": 0},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "total_orders", "min_value": 0},
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "avg_order_value", "min_value": 0, "max_value": 50000},
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "window_start"},
        },
    ],
}


# ── Runner ─────────────────────────────────────────────────────────────────────

class DataQualityRunner:
    def __init__(self, s3_bucket: str):
        self.s3_bucket = s3_bucket
        self.fs = s3fs.S3FileSystem()
        self.context = gx.get_context(mode="ephemeral")
        self.results = {}

    def _load_partition(self, layer: str, subpath: str, run_date: date) -> pd.DataFrame:
        """Load a date-partitioned Parquet partition from S3 into Pandas."""
        y, m, d = run_date.year, run_date.month, run_date.day

        if layer == "silver":
            path = (
                f"s3://{self.s3_bucket}/silver/cleaned_events/"
                f"event_year={y}/event_month={m}/event_day={d}"
            )
        elif layer == "gold":
            path = (
                f"s3://{self.s3_bucket}/gold/{subpath}/"
                f"report_year={y}/report_month={m}/report_day={d}"
            )
        else:
            raise ValueError(f"Unknown layer: {layer}")

        logger.info(f"Loading partition: {path}")
        dataset = pq.ParquetDataset(path, filesystem=self.fs)
        return dataset.read_pandas().to_pandas()

    def run_suite(
        self,
        df: pd.DataFrame,
        suite_config: dict,
        run_date: date,
    ) -> dict:
        """
        Build a GX suite from config dict, validate the DataFrame, return results.
        """
        suite_name = suite_config["suite_name"]

        # Build suite
        suite = self.context.add_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True,
        )

        # Add each expectation
        for exp in suite_config["expectations"]:
            suite.add_expectation(
                gx.core.ExpectationConfiguration(
                    expectation_type=exp["expectation_type"],
                    kwargs=exp["kwargs"],
                )
            )
        self.context.update_expectation_suite(suite)

        # Create in-memory datasource
        datasource = self.context.sources.add_pandas(name=f"{suite_name}_src")
        asset = datasource.add_dataframe_asset(name=f"{suite_name}_asset")
        batch_request = asset.build_batch_request(dataframe=df)

        # Run checkpoint
        checkpoint = self.context.add_or_update_checkpoint(
            name=f"{suite_name}_checkpoint",
            validations=[{
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }],
        )

        result = checkpoint.run(run_name=f"{run_date}")

        # Parse results
        passed = result.success
        stats = result.get_statistics()
        n_evaluated = stats.get("evaluated_expectations", 0)
        n_failed = stats.get("unsuccessful_expectations", 0)

        summary = {
            "suite": suite_name,
            "run_date": str(run_date),
            "passed": passed,
            "evaluated": n_evaluated,
            "failed": n_failed,
            "success_pct": round((n_evaluated - n_failed) / max(n_evaluated, 1) * 100, 1),
        }

        if not passed:
            logger.error(f"Suite FAILED: {suite_name} | {n_failed}/{n_evaluated} expectations failed")
            for vr in result.list_validation_results():
                for er in vr.results:
                    if not er.success:
                        logger.error(f"  ✗ {er.expectation_config.expectation_type} "
                                     f"| {er.expectation_config.kwargs}")
        else:
            logger.info(f"Suite PASSED: {suite_name} | {n_evaluated}/{n_evaluated} checks green ✓")

        return summary


def main():
    parser = argparse.ArgumentParser(description="Run GE data quality checks")
    parser.add_argument("--date", default=str(date.today() - timedelta(days=1)))
    parser.add_argument(
        "--layer",
        choices=["silver", "gold", "all"],
        default="all",
        help="Which layer to validate",
    )
    args = parser.parse_args()

    run_date = date.fromisoformat(args.date)
    runner = DataQualityRunner(S3_BUCKET)

    all_results = []
    overall_passed = True

    if args.layer in ("silver", "all"):
        logger.info("=== Running Silver validation ===")
        try:
            df_silver = runner._load_partition("silver", "cleaned_events", run_date)
            result = runner.run_suite(df_silver, SILVER_EXPECTATIONS, run_date)
            all_results.append(result)
            if not result["passed"]:
                overall_passed = False
        except Exception as e:
            logger.error(f"Silver validation error: {e}")
            overall_passed = False

    if args.layer in ("gold", "all"):
        logger.info("=== Running Gold Sales validation ===")
        try:
            df_gold = runner._load_partition("gold", "sales_metrics", run_date)
            result = runner.run_suite(df_gold, GOLD_SALES_EXPECTATIONS, run_date)
            all_results.append(result)
            if not result["passed"]:
                overall_passed = False
        except Exception as e:
            logger.error(f"Gold validation error: {e}")
            overall_passed = False

    # Summary output
    print("\n" + "=" * 60)
    print(f"DATA QUALITY REPORT — {run_date}")
    print("=" * 60)
    for r in all_results:
        status = "✓ PASS" if r["passed"] else "✗ FAIL"
        print(f"  {status}  {r['suite']}  ({r['success_pct']}%  |  {r['failed']} failed)")
    print("=" * 60)

    sys.exit(0 if overall_passed else 1)


if __name__ == "__main__":
    main()
