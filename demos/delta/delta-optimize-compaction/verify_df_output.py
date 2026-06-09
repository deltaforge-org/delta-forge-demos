#!/usr/bin/env python3
"""
Delta Optimize Compaction -- Delta Data Verification (PySpark)

Verifies the daily_orders table: 80 rows,
72 completed, 5 cancelled, 3 refunded.

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

def verify_daily_orders(spark, data_root, verbose=False):
    print_section("daily_orders -- Final State")

    table_path = os.path.join(data_root, "daily_orders")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 80:
        ok(f"Row count is 80")
    else:
        fail(f"Expected 80 rows, got {row_count}")

    completed = df.filter(col("status") == "completed").count()
    if completed == 72:
        ok(f"status='completed' count is 72")
    else:
        fail(f"Expected 72 rows with status='completed', got {completed}")

    cancelled = df.filter(col("status") == "cancelled").count()
    if cancelled == 5:
        ok(f"status='cancelled' count is 5")
    else:
        fail(f"Expected 5 rows with status='cancelled', got {cancelled}")

    refunded = df.filter(col("status") == "refunded").count()
    if refunded == 3:
        ok(f"status='refunded' count is 3")
    else:
        fail(f"Expected 3 rows with status='refunded', got {refunded}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Optimize Compaction -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "daily_orders")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_daily_orders(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
