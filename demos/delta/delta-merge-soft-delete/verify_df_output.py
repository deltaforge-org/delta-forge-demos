#!/usr/bin/env python3
"""
Delta Merge Soft Delete -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-soft-delete demo.
vendors: 16 rows (14 original + 2 new).
11 active (is_active=1), 5 inactive (is_active=0).
vendor_feed: 8 source rows (read-only).

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

def verify_vendors(spark, data_root, verbose=False):
    print_section("vendors -- Final State")

    table_path = os.path.join(data_root, "vendors")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 16
    if row_count == 16:
        ok(f"Row count is {row_count} (expected 16)")
    else:
        fail(f"Row count is {row_count} (expected 16)")

    # Assert count where is_active = 1 is 11
    active_count = df.filter(col("is_active") == 1).count()
    if active_count == 11:
        ok(f"Count where 'is_active' == 1 is {active_count} (expected 11)")
    else:
        fail(f"Count where 'is_active' == 1 is {active_count} (expected 11)")

def verify_vendor_feed(spark, data_root, verbose=False):
    print_section("vendor_feed -- Source (read-only)")

    table_path = os.path.join(data_root, "vendor_feed")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 8
    if row_count == 8:
        ok(f"Row count is {row_count} (expected 8)")
    else:
        fail(f"Row count is {row_count} (expected 8)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge Soft Delete -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("vendors", "vendor_feed"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_vendors(spark, data_root, verbose=verbose)
        verify_vendor_feed(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
