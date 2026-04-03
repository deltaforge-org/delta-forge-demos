#!/usr/bin/env python3
"""
Delta Partition Replace DV -- Delta Data Verification (PySpark)

Verifies the monthly_settlements table: 58 rows (18 + 20 + 20,
Jan corrected), 3 distinct settlement months.

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

def verify_monthly_settlements(spark, data_root, verbose=False):
    print_section("monthly_settlements -- Final State")

    table_path = os.path.join(data_root, "monthly_settlements")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 58:
        ok(f"Row count is 58")
    else:
        fail(f"Expected 58 rows, got {row_count}")

    distinct_months = df.select("settlement_month").distinct().count()
    if distinct_months == 3:
        ok(f"Distinct settlement_month count is 3")
    else:
        fail(f"Expected 3 distinct settlement_months, got {distinct_months}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Partition Replace DV -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "monthly_settlements")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_monthly_settlements(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
