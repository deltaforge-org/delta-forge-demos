#!/usr/bin/env python3
"""
Delta Multi-Level Partitioning -- Delta Data Verification (PySpark)

Verifies three tables:
  sales_ml: 60 rows, 4 regions x 4 quarters x 15 per partition combination
  region_dim: 4 rows (dimension table)
  quarter_dim: 4 rows (dimension table)

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

def verify_sales_ml(spark, data_root, verbose=False):
    print_section("sales_ml -- Final State")

    table_path = os.path.join(data_root, "sales_ml")
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

    distinct_regions = df.select("region").distinct().count()
    if distinct_regions == 4:
        ok(f"Distinct region count is 4")
    else:
        fail(f"Expected 4 distinct regions, got {distinct_regions}")

    distinct_quarters = df.select("quarter").distinct().count()
    if distinct_quarters == 4:
        ok(f"Distinct quarter count is 4")
    else:
        fail(f"Expected 4 distinct quarters, got {distinct_quarters}")

def verify_region_dim(spark, data_root, verbose=False):
    print_section("region_dim -- Final State")

    table_path = os.path.join(data_root, "region_dim")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 4:
        ok(f"Row count is 4")
    else:
        fail(f"Expected 4 rows, got {row_count}")

def verify_quarter_dim(spark, data_root, verbose=False):
    print_section("quarter_dim -- Final State")

    table_path = os.path.join(data_root, "quarter_dim")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 4:
        ok(f"Row count is 4")
    else:
        fail(f"Expected 4 rows, got {row_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Multi-Level Partitioning -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("sales_ml", "region_dim", "quarter_dim"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_sales_ml(spark, data_root, verbose=verbose)
        verify_region_dim(spark, data_root, verbose=verbose)
        verify_quarter_dim(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
