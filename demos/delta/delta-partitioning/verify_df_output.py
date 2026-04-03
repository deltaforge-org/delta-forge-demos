#!/usr/bin/env python3
"""
Delta Partitioning -- Delta Data Verification (PySpark)

Verifies the orders table: 76 rows (80 - 4),
4 regions: North 20, South 20, East 20, West 16.

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

def verify_orders(spark, data_root, verbose=False):
    print_section("orders -- Final State")

    table_path = os.path.join(data_root, "orders")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 76:
        ok(f"Row count is 76")
    else:
        fail(f"Expected 76 rows, got {row_count}")

    distinct_regions = df.select("region").distinct().count()
    if distinct_regions == 4:
        ok(f"Distinct region count is 4")
    else:
        fail(f"Expected 4 distinct regions, got {distinct_regions}")

    for region_name, expected in [("North", 20), ("South", 20), ("East", 20), ("West", 16)]:
        actual = df.filter(col("region") == region_name).count()
        if actual == expected:
            ok(f"region='{region_name}' count is {expected}")
        else:
            fail(f"Expected {expected} rows with region='{region_name}', got {actual}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Partitioning -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "orders")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_orders(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
