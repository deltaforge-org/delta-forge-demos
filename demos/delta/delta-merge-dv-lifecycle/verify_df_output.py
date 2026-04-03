#!/usr/bin/env python3
"""
Delta Merge DV Lifecycle -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-dv-lifecycle demo.
product_catalog: 40 rows (40 original - 5 deleted + 5 new).
supplier_feed: 20 source rows (read-only).

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

def verify_product_catalog(spark, data_root, verbose=False):
    print_section("product_catalog -- Final State")

    table_path = os.path.join(data_root, "product_catalog")
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

def verify_supplier_feed(spark, data_root, verbose=False):
    print_section("supplier_feed -- Source (read-only)")

    table_path = os.path.join(data_root, "supplier_feed")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 20
    if row_count == 20:
        ok(f"Row count is {row_count} (expected 20)")
    else:
        fail(f"Row count is {row_count} (expected 20)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge DV Lifecycle -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("product_catalog", "supplier_feed"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_product_catalog(spark, data_root, verbose=verbose)
        verify_supplier_feed(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
