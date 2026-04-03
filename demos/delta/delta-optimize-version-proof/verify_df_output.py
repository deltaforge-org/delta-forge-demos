#!/usr/bin/env python3
"""
Delta Optimize Version Proof -- Delta Data Verification (PySpark)

Verifies the shipments table: 25 rows (20 - 3 + 8),
3 distinct carriers.

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

def verify_shipments(spark, data_root, verbose=False):
    print_section("shipments -- Final State")

    table_path = os.path.join(data_root, "shipments")
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

    distinct_carriers = df.select("carrier").distinct().count()
    if distinct_carriers == 3:
        ok(f"Distinct carrier count is 3")
    else:
        fail(f"Expected 3 distinct carriers, got {distinct_carriers}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Optimize Version Proof -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "shipments")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_shipments(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
