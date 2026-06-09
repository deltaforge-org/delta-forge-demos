#!/usr/bin/env python3
"""
Delta Restore Advanced -- Delta Data Verification (PySpark)

Verifies the config_settings table: 35 rows (restored to V2),
5 distinct categories.

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

def verify_config_settings(spark, data_root, verbose=False):
    print_section("config_settings -- Final State")

    table_path = os.path.join(data_root, "config_settings")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 35:
        ok(f"Row count is 35")
    else:
        fail(f"Expected 35 rows, got {row_count}")

    distinct_cats = df.select("category").distinct().count()
    if distinct_cats == 5:
        ok(f"Distinct category count is 5")
    else:
        fail(f"Expected 5 distinct categories, got {distinct_cats}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Restore Advanced -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "config_settings")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_config_settings(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
