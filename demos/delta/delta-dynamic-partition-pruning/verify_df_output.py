#!/usr/bin/env python3
"""
Delta Dynamic Partition Pruning -- Delta Data Verification (PySpark)

Verifies the sales_facts and region_targets tables:
  - sales_facts: 55 rows (60 - 5), 4 regions
  - region_targets: 4 rows

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

def verify_sales_facts(spark, data_root, verbose=False):
    print_section("sales_facts -- Final State")

    table_path = os.path.join(data_root, "sales_facts")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 55 rows (60 - 5)
    if row_count == 55:
        ok("Row count = 55")
    else:
        fail(f"Row count = {row_count}, expected 55")

    # 4 regions
    distinct_regions = df.select("region").distinct().count()
    if distinct_regions == 4:
        ok("Distinct region count = 4")
    else:
        fail(f"Distinct region count = {distinct_regions}, expected 4")

    # Region breakdown
    for region, expected in [("us-east", 14), ("us-west", 14), ("eu-west", 14), ("ap-south", 13)]:
        actual = df.filter(df.region == region).count()
        if actual == expected:
            ok(f"region='{region}' count = {expected}")
        else:
            fail(f"region='{region}' count = {actual}, expected {expected}")

def verify_region_targets(spark, data_root, verbose=False):
    print_section("region_targets -- Final State")

    table_path = os.path.join(data_root, "region_targets")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 4 rows
    if row_count == 4:
        ok("Row count = 4")
    else:
        fail(f"Row count = {row_count}, expected 4")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Dynamic Partition Pruning -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("sales_facts", "region_targets"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_sales_facts(spark, data_root, verbose=verbose)
        verify_region_targets(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
