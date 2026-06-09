#!/usr/bin/env python3
"""
Delta JSON Event Log -- Delta Data Verification (PySpark)

Verifies the payment_events table produced by the delta-json-event-log demo.
40 rows: 15 charge, 10 refund, 8 auth, 7 payout.

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

def verify_payment_events(spark, data_root, verbose=False):
    print_section("payment_events -- Final State")

    table_path = os.path.join(data_root, "payment_events")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 40
    if row_count == 40:
        ok(f"Row count is {row_count} (expected 40)")
    else:
        fail(f"Row count is {row_count} (expected 40)")

    # Assert count where txn_type = 'charge' is 15
    charge_count = df.filter(col("txn_type") == "charge").count()
    if charge_count == 15:
        ok(f"Count where 'txn_type' == 'charge' is {charge_count} (expected 15)")
    else:
        fail(f"Count where 'txn_type' == 'charge' is {charge_count} (expected 15)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta JSON Event Log -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "payment_events")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_payment_events(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
