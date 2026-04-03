#!/usr/bin/env python3
"""
Delta Basics CRUD -- Delta Data Verification (PySpark)

Verifies the products table: 22 rows (20 - 3 deleted + 5 inserted),
all active, 6 Electronics after price increase.

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

def verify_products(spark, data_root, verbose=False):
    print_section("products -- Final State")

    table_path = os.path.join(data_root, "products")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 22:
        ok("ROW_COUNT = 22")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 22")

    # Count where category='Electronics'
    electronics_count = df.filter(df.category == "Electronics").count()
    if electronics_count == 6:
        ok("COUNT WHERE category='Electronics' = 6")
    else:
        fail(f"COUNT WHERE category='Electronics' = {electronics_count}, expected 6")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Basics CRUD -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "products")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_products(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
