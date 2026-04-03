#!/usr/bin/env python3
"""
Delta Merge Computed Columns -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-computed-columns demo.
subscriptions: 15 rows (12 original + 3 new).
Tier distribution: 5 platinum, 4 gold, 4 silver, 2 bronze.
subscription_changes: 10 source rows (read-only).

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
from pyspark.sql.functions import col

def verify_subscriptions(spark, data_root, verbose=False):
    print_section("subscriptions -- Final State")

    table_path = os.path.join(data_root, "subscriptions")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 15
    if row_count == 15:
        ok(f"Row count is {row_count} (expected 15)")
    else:
        fail(f"Row count is {row_count} (expected 15)")

    # Assert count where tier = 'platinum' is 5
    platinum_count = df.filter(col("tier") == "platinum").count()
    if platinum_count == 5:
        ok(f"Count where 'tier' == 'platinum' is {platinum_count} (expected 5)")
    else:
        fail(f"Count where 'tier' == 'platinum' is {platinum_count} (expected 5)")

def verify_subscription_changes(spark, data_root, verbose=False):
    print_section("subscription_changes -- Source (read-only)")

    table_path = os.path.join(data_root, "subscription_changes")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 10
    if row_count == 10:
        ok(f"Row count is {row_count} (expected 10)")
    else:
        fail(f"Row count is {row_count} (expected 10)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge Computed Columns -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("subscriptions", "subscription_changes"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_subscriptions(spark, data_root, verbose=verbose)
        verify_subscription_changes(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
