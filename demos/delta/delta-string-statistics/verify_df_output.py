#!/usr/bin/env python3
"""
Delta String Statistics -- Delta Data Verification (PySpark)
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

    # Assert row count = 20
    if row_count == 20:
        ok(f"Row count is 20")
    else:
        fail(f"Expected 20 rows, got {row_count}")

    # Assert 4 distinct categories
    distinct_categories = df.select("category").distinct().count()
    if distinct_categories == 4:
        ok(f"Distinct 'category' count is 4")
    else:
        fail(f"Expected 4 distinct 'category', got {distinct_categories}")

    # Assert category counts
    for category, expected in [("Accessories", 6), ("Audio", 3), ("Furniture", 6), ("Peripherals", 5)]:
        actual = df.filter(col("category") == category).count()
        if actual == expected:
            ok(f"category='{category}' count is {expected}")
        else:
            fail(f"Expected {expected} rows where category='{category}', got {actual}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta String Statistics -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "product_catalog")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_product_catalog(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
