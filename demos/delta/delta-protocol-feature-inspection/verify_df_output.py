#!/usr/bin/env python3
"""
Delta Protocol Feature Inspection -- Delta Data Verification (PySpark)

Verifies three tables (read-only inspection):
  inherited_plain: 15 rows
  inherited_cdc: 12 rows
  inherited_constrained: 10 rows

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

def verify_inherited_plain(spark, data_root, verbose=False):
    print_section("inherited_plain -- Final State")

    table_path = os.path.join(data_root, "inherited_plain")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 15:
        ok(f"Row count is 15")
    else:
        fail(f"Expected 15 rows, got {row_count}")

def verify_inherited_cdc(spark, data_root, verbose=False):
    print_section("inherited_cdc -- Final State")

    table_path = os.path.join(data_root, "inherited_cdc")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 12:
        ok(f"Row count is 12")
    else:
        fail(f"Expected 12 rows, got {row_count}")

def verify_inherited_constrained(spark, data_root, verbose=False):
    print_section("inherited_constrained -- Final State")

    table_path = os.path.join(data_root, "inherited_constrained")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 10:
        ok(f"Row count is 10")
    else:
        fail(f"Expected 10 rows, got {row_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Protocol Feature Inspection -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("inherited_plain", "inherited_cdc", "inherited_constrained"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_inherited_plain(spark, data_root, verbose=verbose)
        verify_inherited_cdc(spark, data_root, verbose=verbose)
        verify_inherited_constrained(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
