#!/usr/bin/env python3
"""
Delta Merge Subquery -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-subquery demo.
daily_revenue: 20 rows (15 original + 5 new).
5 products, 4 dates.
order_events: 40 source rows (read-only).

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

def verify_daily_revenue(spark, data_root, verbose=False):
    print_section("daily_revenue -- Final State")

    table_path = os.path.join(data_root, "daily_revenue")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 20
    if row_count == 20:
        ok(f"Row count is {row_count} (expected 20)")
    else:
        fail(f"Row count is {row_count} (expected 20)")

    # Assert distinct count for product_id = 5
    distinct_products = df.select("product_id").distinct().count()
    if distinct_products == 5:
        ok(f"Distinct 'product_id' count is {distinct_products} (expected 5)")
    else:
        fail(f"Distinct 'product_id' count is {distinct_products} (expected 5)")

def verify_order_events(spark, data_root, verbose=False):
    print_section("order_events -- Source (read-only)")

    table_path = os.path.join(data_root, "order_events")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 40
    if row_count == 40:
        ok(f"Row count is {row_count} (expected 40)")
    else:
        fail(f"Row count is {row_count} (expected 40)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge Subquery -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("daily_revenue", "order_events"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_daily_revenue(spark, data_root, verbose=verbose)
        verify_order_events(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
