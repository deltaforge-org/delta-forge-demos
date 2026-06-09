#!/usr/bin/env python3
"""
Delta Vacuum Storage Savings -- Delta Data Verification (PySpark)

Verifies the billing_transactions table: 32 rows (30 - 3 deleted + 5 inserted),
3 plans (Enterprise, Pro, Starter), 5 refunded, 27 active.

Usage:
    python verify_df_output.py <data_root_path> [--verbose]

Requirements:
    pip install pyspark delta-spark
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import ok, fail, info, print_header, print_section, print_summary, exit_with_status
from verify_lib.spark_session import get_spark, resolve_data_root

def verify_billing_transactions(spark, data_root, verbose=False):
    print_section("billing_transactions -- Final State")

    table_path = os.path.join(data_root, "billing_transactions")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 32:
        ok(f"Row count is 32")
    else:
        fail(f"Expected 32 rows, got {row_count}")

    distinct_plan = df.select("plan").distinct().count()
    if distinct_plan == 3:
        ok(f"Distinct plan count is 3")
    else:
        fail(f"Expected 3 distinct plan values, got {distinct_plan}")

    active_count = df.filter(df["status"] == "active").count()
    if active_count == 27:
        ok(f"status='active' count is 27")
    else:
        fail(f"Expected 27 rows with status='active', got {active_count}")

    refunded_count = df.filter(df["status"] == "refunded").count()
    if refunded_count == 5:
        ok(f"status='refunded' count is 5")
    else:
        fail(f"Expected 5 rows with status='refunded', got {refunded_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Vacuum Storage Savings -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "billing_transactions")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_billing_transactions(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
