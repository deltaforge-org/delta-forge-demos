#!/usr/bin/env python3
"""
Delta Merge Upsert -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-upsert demo.
products: 16 rows (15 original - 3 removed + 4 new).
Categories: 8 electronics, 5 accessories, 3 furniture.
product_feed: 12 source rows (read-only).

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

def verify_products(spark, data_root, verbose=False):
    print_section("products -- Final State")

    table_path = os.path.join(data_root, "products")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 16
    if row_count == 16:
        ok(f"Row count is {row_count} (expected 16)")
    else:
        fail(f"Row count is {row_count} (expected 16)")

    # Assert count where category = 'electronics' is 8
    electronics_count = df.filter(col("category") == "electronics").count()
    if electronics_count == 8:
        ok(f"Count where 'category' == 'electronics' is {electronics_count} (expected 8)")
    else:
        fail(f"Count where 'category' == 'electronics' is {electronics_count} (expected 8)")

def verify_product_feed(spark, data_root, verbose=False):
    print_section("product_feed -- Source (read-only)")

    table_path = os.path.join(data_root, "product_feed")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 12
    if row_count == 12:
        ok(f"Row count is {row_count} (expected 12)")
    else:
        fail(f"Row count is {row_count} (expected 12)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge Upsert -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("products", "product_feed"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_products(spark, data_root, verbose=verbose)
        verify_product_feed(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
