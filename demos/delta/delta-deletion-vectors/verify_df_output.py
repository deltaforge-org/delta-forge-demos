#!/usr/bin/env python3
"""
Delta Deletion Vectors -- Delta Data Verification (PySpark)

Verifies the web_sessions table:
  - 42 rows (60 - 10 bounced - 8 expired)
  - 0 bounced, 6 expired remaining, 21 completed, ~15 active

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

def verify_web_sessions(spark, data_root, verbose=False):
    print_section("web_sessions -- Final State")

    table_path = os.path.join(data_root, "web_sessions")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 42 rows total
    if row_count == 42:
        ok("Row count = 42")
    else:
        fail(f"Row count = {row_count}, expected 42")

    # 0 bounced sessions
    bounced = df.filter(df.status == "bounced").count()
    if bounced == 0:
        ok("status='bounced' count = 0")
    else:
        fail(f"status='bounced' count = {bounced}, expected 0")

    # 21 completed sessions
    completed = df.filter(df.status == "completed").count()
    if completed == 21:
        ok("status='completed' count = 21")
    else:
        fail(f"status='completed' count = {completed}, expected 21")

    # 6 expired remaining
    expired = df.filter(df.status == "expired").count()
    if expired == 6:
        ok("status='expired' count = 6")
    else:
        fail(f"status='expired' count = {expired}, expected 6")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Deletion Vectors -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "web_sessions")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_web_sessions(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
