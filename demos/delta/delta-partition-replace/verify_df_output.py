#!/usr/bin/env python3
"""
Delta Partition Replace -- Delta Data Verification (PySpark)

Verifies the monthly_sales table: 80 rows (20 + 20 + 20 + 20,
Feb corrected, Apr new), 4 distinct sale months.

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

def verify_monthly_sales(spark, data_root, verbose=False):
    print_section("monthly_sales -- Final State")

    table_path = os.path.join(data_root, "monthly_sales")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 80:
        ok(f"Row count is 80")
    else:
        fail(f"Expected 80 rows, got {row_count}")

    distinct_months = df.select("sale_month").distinct().count()
    if distinct_months == 4:
        ok(f"Distinct sale_month count is 4")
    else:
        fail(f"Expected 4 distinct sale_months, got {distinct_months}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Partition Replace -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "monthly_sales")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_monthly_sales(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
