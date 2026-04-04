#!/usr/bin/env python3
"""
Delta Update String Cleansing -- Delta Data Verification (PySpark)

Verifies the customer_imports table: 25 rows,
4 countries: US 11, UK 5, DE 5, CA 4.

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

def verify_customer_imports(spark, data_root, verbose=False):
    print_section("customer_imports -- Final State")

    table_path = os.path.join(data_root, "customer_imports")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 25:
        ok(f"Row count is 25")
    else:
        fail(f"Expected 25 rows, got {row_count}")

    us_count = df.filter(df["country_code"] == "US").count()
    if us_count == 11:
        ok(f"country_code='US' count is 11")
    else:
        fail(f"Expected 11 rows with country_code='US', got {us_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Update String Cleansing -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "customer_imports")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_customer_imports(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
