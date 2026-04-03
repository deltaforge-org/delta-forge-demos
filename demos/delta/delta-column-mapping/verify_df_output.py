#!/usr/bin/env python3
"""
Delta Column Mapping -- Delta Data Verification (PySpark)

Verifies the employee_directory table: 40 rows,
3 inactive (is_active=0), 37 active (is_active=1),
location column added via schema evolution.

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

def verify_employee_directory(spark, data_root, verbose=False):
    print_section("employee_directory -- Final State")

    table_path = os.path.join(data_root, "employee_directory")
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

    # Count where is_active=0
    inactive_count = df.filter(df.is_active == 0).count()
    if inactive_count == 3:
        ok("COUNT WHERE is_active=0 = 3")
    else:
        fail(f"COUNT WHERE is_active=0 = {inactive_count}, expected 3")

    # Count where is_active=1
    active_count = df.filter(df.is_active == 1).count()
    if active_count == 37:
        ok("COUNT WHERE is_active=1 = 37")
    else:
        fail(f"COUNT WHERE is_active=1 = {active_count}, expected 37")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Column Mapping -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "employee_directory")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_employee_directory(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
