#!/usr/bin/env python3
"""
Delta Binary Geometry -- Delta Data Verification (PySpark)

Verifies two tables:
  document_store: 23 rows (25 - 2 deleted)
  geo_locations: 30 rows (unchanged)

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

def verify_document_store(spark, data_root, verbose=False):
    print_section("document_store -- Final State")

    table_path = os.path.join(data_root, "document_store")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    if row_count == 23:
        ok("ROW_COUNT = 23")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 23")

def verify_geo_locations(spark, data_root, verbose=False):
    print_section("geo_locations -- Final State")

    table_path = os.path.join(data_root, "geo_locations")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    if row_count == 30:
        ok("ROW_COUNT = 30")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 30")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Binary Geometry -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("document_store", "geo_locations"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_document_store(spark, data_root, verbose=verbose)
        verify_geo_locations(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
