#!/usr/bin/env python3
"""
Delta Partition Merge -- Delta Data Verification (PySpark)

Verifies two tables:
  product_catalog: 65 rows (60 + 5 from merge),
    4 categories: Electronics 18, Clothing 17, Home 15, Sports 15
  supplier_feed: 18 rows (source table)

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

def verify_product_catalog(spark, data_root, verbose=False):
    print_section("product_catalog -- Final State")

    table_path = os.path.join(data_root, "product_catalog")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 65:
        ok(f"Row count is 65")
    else:
        fail(f"Expected 65 rows, got {row_count}")

    distinct_cats = df.select("category").distinct().count()
    if distinct_cats == 4:
        ok(f"Distinct category count is 4")
    else:
        fail(f"Expected 4 distinct categories, got {distinct_cats}")

    for cat_name, expected in [("Electronics", 18), ("Clothing", 17), ("Home", 15), ("Sports", 15)]:
        actual = df.filter(col("category") == cat_name).count()
        if actual == expected:
            ok(f"category='{cat_name}' count is {expected}")
        else:
            fail(f"Expected {expected} rows with category='{cat_name}', got {actual}")

def verify_supplier_feed(spark, data_root, verbose=False):
    print_section("supplier_feed -- Final State")

    table_path = os.path.join(data_root, "supplier_feed")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 18:
        ok(f"Row count is 18")
    else:
        fail(f"Expected 18 rows, got {row_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Partition Merge -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("product_catalog", "supplier_feed"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_product_catalog(spark, data_root, verbose=verbose)
        verify_supplier_feed(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
