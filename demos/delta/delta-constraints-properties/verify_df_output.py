#!/usr/bin/env python3
"""
Delta Constraints Properties -- Delta Data Verification (PySpark)

Verifies the invoices and event_log tables:
  - invoices: 30 rows (18 paid, 8 pending, 4 overdue)
  - event_log: 50 rows (append-only)

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

def verify_invoices(spark, data_root, verbose=False):
    print_section("invoices -- Final State")

    table_path = os.path.join(data_root, "invoices")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 30:
        ok("Row count = 30")
    else:
        fail(f"Row count = {row_count}, expected 30")

    for status, expected in [("paid", 18), ("pending", 8), ("overdue", 4)]:
        actual = df.filter(df.status == status).count()
        if actual == expected:
            ok(f"status='{status}' count = {expected}")
        else:
            fail(f"status='{status}' count = {actual}, expected {expected}")

def verify_event_log(spark, data_root, verbose=False):
    print_section("event_log -- Final State")

    table_path = os.path.join(data_root, "event_log")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 50:
        ok("Row count = 50")
    else:
        fail(f"Row count = {row_count}, expected 50")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Constraints Properties -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("invoices", "event_log"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_invoices(spark, data_root, verbose=verbose)
        verify_event_log(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
