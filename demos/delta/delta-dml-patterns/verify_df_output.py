#!/usr/bin/env python3
"""
Delta DML Patterns -- Delta Data Verification (PySpark)

Verifies the order_history and order_archive tables:
  - order_history: 47 rows (60 - 8 archived - 5 deleted), 4 regions
  - order_archive: 8 rows

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

def verify_order_history(spark, data_root, verbose=False):
    print_section("order_history -- Final State")

    table_path = os.path.join(data_root, "order_history")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 47 rows (60 - 8 - 5)
    if row_count == 47:
        ok("Row count = 47")
    else:
        fail(f"Row count = {row_count}, expected 47")

    # 4 regions represented
    distinct_regions = df.select("region").distinct().count()
    if distinct_regions == 4:
        ok("Distinct region count = 4")
    else:
        fail(f"Distinct region count = {distinct_regions}, expected 4")

def verify_order_archive(spark, data_root, verbose=False):
    print_section("order_archive -- Final State")

    table_path = os.path.join(data_root, "order_archive")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 8 rows
    if row_count == 8:
        ok("Row count = 8")
    else:
        fail(f"Row count = {row_count}, expected 8")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta DML Patterns -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("order_history", "order_archive"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_order_history(spark, data_root, verbose=verbose)
        verify_order_archive(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
