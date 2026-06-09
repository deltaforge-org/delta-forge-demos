#!/usr/bin/env python3
"""
Delta Merge SCD2 -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-scd2 demo.
policy_dim: 23 rows (15 original + 8 new versioned rows).
8 expired (is_current=0), 15 current (is_current=1).
policy_changes: 8 source rows (read-only).

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

def verify_policy_dim(spark, data_root, verbose=False):
    print_section("policy_dim -- Final State")

    table_path = os.path.join(data_root, "policy_dim")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 23
    if row_count == 23:
        ok(f"Row count is {row_count} (expected 23)")
    else:
        fail(f"Row count is {row_count} (expected 23)")

    # Assert count where is_current = 1 is 15
    current_count = df.filter(col("is_current") == 1).count()
    if current_count == 15:
        ok(f"Count where 'is_current' == 1 is {current_count} (expected 15)")
    else:
        fail(f"Count where 'is_current' == 1 is {current_count} (expected 15)")

def verify_policy_changes(spark, data_root, verbose=False):
    print_section("policy_changes -- Source (read-only)")

    table_path = os.path.join(data_root, "policy_changes")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 8
    if row_count == 8:
        ok(f"Row count is {row_count} (expected 8)")
    else:
        fail(f"Row count is {row_count} (expected 8)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge SCD2 -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("policy_dim", "policy_changes"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_policy_dim(spark, data_root, verbose=verbose)
        verify_policy_changes(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
