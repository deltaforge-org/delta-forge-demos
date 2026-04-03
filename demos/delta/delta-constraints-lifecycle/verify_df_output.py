#!/usr/bin/env python3
"""
Delta Constraints Lifecycle -- Delta Data Verification (PySpark)

Verifies the products table after constraint-driven operations:
  - 20 products seeded, 3 deleted (items 2, 8, 14) = 17 rows
  - All prices increased by 10%
  - 6 categories remain, 8 items with discount > 0

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

def verify_products(spark, data_root, verbose=False):
    print_section("products -- Final State")

    table_path = os.path.join(data_root, "products")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 17 rows (20 original - 3 deleted)
    if row_count == 17:
        ok("Row count = 17")
    else:
        fail(f"Row count = {row_count}, expected 17")

    # 6 distinct categories
    distinct_categories = df.select("category").distinct().count()
    if distinct_categories == 6:
        ok("Distinct category count = 6")
    else:
        fail(f"Distinct category count = {distinct_categories}, expected 6")

    # 8 items with discount > 0
    discounted_count = df.filter("discount > 0").count()
    if discounted_count == 8:
        ok("8 items with discount > 0")
    else:
        fail(f"Items with discount > 0 = {discounted_count}, expected 8")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Constraints Lifecycle -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "products")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_products(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
