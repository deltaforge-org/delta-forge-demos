#!/usr/bin/env python3
"""
Delta Partition Pitfalls -- Delta Data Verification (PySpark)

Verifies two tables with different partition schemes:
  events_by_customer: 60 rows
  events_by_month: 60 rows

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

def verify_events_by_customer(spark, data_root, verbose=False):
    print_section("events_by_customer -- Final State")

    table_path = os.path.join(data_root, "events_by_customer")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 60:
        ok(f"Row count is 60")
    else:
        fail(f"Expected 60 rows, got {row_count}")

def verify_events_by_month(spark, data_root, verbose=False):
    print_section("events_by_month -- Final State")

    table_path = os.path.join(data_root, "events_by_month")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 60:
        ok(f"Row count is 60")
    else:
        fail(f"Expected 60 rows, got {row_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Partition Pitfalls -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("events_by_customer", "events_by_month"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_events_by_customer(spark, data_root, verbose=verbose)
        verify_events_by_month(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
