#!/usr/bin/env python3
"""
Delta Z-Order vs Partition -- Delta Data Verification (PySpark)

Verifies two tables:
  - orders_partitioned: 100 rows, 5 regions, 4 categories
  - orders_zorder: 100 rows, 5 regions, 4 categories

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

def verify_orders_partitioned(spark, data_root, verbose=False):
    print_section("orders_partitioned -- Final State")

    table_path = os.path.join(data_root, "orders_partitioned")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 100:
        ok(f"Row count is 100")
    else:
        fail(f"Expected 100 rows, got {row_count}")

    distinct_region = df.select("customer_region").distinct().count()
    if distinct_region == 5:
        ok(f"Distinct customer_region count is 5")
    else:
        fail(f"Expected 5 distinct customer_region values, got {distinct_region}")

def verify_orders_zorder(spark, data_root, verbose=False):
    print_section("orders_zorder -- Final State")

    table_path = os.path.join(data_root, "orders_zorder")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 100:
        ok(f"Row count is 100")
    else:
        fail(f"Expected 100 rows, got {row_count}")

    distinct_region = df.select("customer_region").distinct().count()
    if distinct_region == 5:
        ok(f"Distinct customer_region count is 5")
    else:
        fail(f"Expected 5 distinct customer_region values, got {distinct_region}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Z-Order vs Partition -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ["orders_partitioned", "orders_zorder"]:
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_orders_partitioned(spark, data_root, verbose=verbose)
        verify_orders_zorder(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
