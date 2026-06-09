#!/usr/bin/env python3
"""
Delta Table Properties Lifecycle -- Delta Data Verification (PySpark)
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

def verify_inventory_items(spark, data_root, verbose=False):
    print_section("inventory_items -- Final State")

    table_path = os.path.join(data_root, "inventory_items")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 13
    if row_count == 13:
        ok(f"Row count is 13")
    else:
        fail(f"Expected 13 rows, got {row_count}")

    # Assert 5 distinct categories
    distinct_categories = df.select("category").distinct().count()
    if distinct_categories == 5:
        ok(f"Distinct 'category' count is 5")
    else:
        fail(f"Expected 5 distinct 'category', got {distinct_categories}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Table Properties Lifecycle -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "inventory_items")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_inventory_items(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
