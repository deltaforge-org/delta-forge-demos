#!/usr/bin/env python3
"""
Delta Complex Types -- Delta Data Verification (PySpark)

Verifies the employees table: 40 rows,
11 in Engineering department (with salary increase).

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

def verify_employees(spark, data_root, verbose=False):
    print_section("employees -- Final State")

    table_path = os.path.join(data_root, "employees")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 40:
        ok("ROW_COUNT = 40")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 40")

    # Count where department='Engineering'
    eng_count = df.filter(df.department == "Engineering").count()
    if eng_count == 11:
        ok("COUNT WHERE department='Engineering' = 11")
    else:
        fail(f"COUNT WHERE department='Engineering' = {eng_count}, expected 11")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Complex Types -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "employees")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_employees(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
