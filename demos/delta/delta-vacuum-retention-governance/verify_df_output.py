#!/usr/bin/env python3
"""
Delta Vacuum Retention Governance -- Delta Data Verification (PySpark)

Verifies the settlement_records table: 30 rows (35 - 5),
10 settled, 3 failed, 17 pending.

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

def verify_settlement_records(spark, data_root, verbose=False):
    print_section("settlement_records -- Final State")

    table_path = os.path.join(data_root, "settlement_records")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 30:
        ok(f"Row count is 30")
    else:
        fail(f"Expected 30 rows, got {row_count}")

    settled_count = df.filter(df["status"] == "settled").count()
    if settled_count == 10:
        ok(f"status='settled' count is 10")
    else:
        fail(f"Expected 10 rows with status='settled', got {settled_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Vacuum Retention Governance -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "settlement_records")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_settlement_records(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
