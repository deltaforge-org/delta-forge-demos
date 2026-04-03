#!/usr/bin/env python3
"""
Delta Unicode Roundtrip -- Delta Data Verification (PySpark)

Verifies the global_bazaar table: 26 rows (30 - 4),
3 regions: Asia, Europe, MENA.

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

def verify_global_bazaar(spark, data_root, verbose=False):
    print_section("global_bazaar -- Final State")

    table_path = os.path.join(data_root, "global_bazaar")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 26:
        ok(f"Row count is 26")
    else:
        fail(f"Expected 26 rows, got {row_count}")

    distinct_region = df.select("region").distinct().count()
    if distinct_region == 3:
        ok(f"Distinct region count is 3")
    else:
        fail(f"Expected 3 distinct region values, got {distinct_region}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Unicode Roundtrip -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "global_bazaar")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_global_bazaar(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
