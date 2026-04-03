#!/usr/bin/env python3
"""
Delta Binary Geometry Advanced -- Delta Data Verification (PySpark)

Verifies three tables:
  documents: 27 rows (30 - 3 deleted)
  locations: 25 rows (unchanged)
  audit_log: 40 rows (unchanged)

Usage:
    python verify_df_output.py <data_root_path> [--verbose]

Requirements:
    pip install pyspark delta-spark
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (ok, fail, info,
    print_header, print_section, print_summary, exit_with_status)
from verify_lib.spark_session import get_spark, resolve_data_root

def verify_documents(spark, data_root, verbose=False):
    print_section("documents -- Final State")

    table_path = os.path.join(data_root, "documents")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    if row_count == 27:
        ok("ROW_COUNT = 27")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 27")

def verify_locations(spark, data_root, verbose=False):
    print_section("locations -- Final State")

    table_path = os.path.join(data_root, "locations")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    if row_count == 25:
        ok("ROW_COUNT = 25")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 25")

def verify_audit_log(spark, data_root, verbose=False):
    print_section("audit_log -- Final State")

    table_path = os.path.join(data_root, "audit_log")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    if row_count == 40:
        ok("ROW_COUNT = 40")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 40")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Binary Geometry Advanced -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("documents", "locations", "audit_log"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_documents(spark, data_root, verbose=verbose)
        verify_locations(spark, data_root, verbose=verbose)
        verify_audit_log(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
