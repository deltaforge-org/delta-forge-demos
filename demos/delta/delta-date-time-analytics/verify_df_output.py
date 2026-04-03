#!/usr/bin/env python3
"""
Delta Date Time Analytics -- Delta Data Verification (PySpark)

Verifies the attendance_records table:
  - 50 rows total
  - 5 employees, is_remote boolean (16 remote, 34 onsite)

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

def verify_attendance_records(spark, data_root, verbose=False):
    print_section("attendance_records -- Final State")

    table_path = os.path.join(data_root, "attendance_records")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 50 rows total
    if row_count == 50:
        ok("Row count = 50")
    else:
        fail(f"Row count = {row_count}, expected 50")

    # 5 distinct employees
    distinct_employees = df.select("employee_name").distinct().count()
    if distinct_employees == 5:
        ok("Distinct employee_name count = 5")
    else:
        fail(f"Distinct employee_name count = {distinct_employees}, expected 5")

    # Remote vs onsite breakdown (is_remote is BOOLEAN)
    remote_count = df.filter(df.is_remote == True).count()
    if remote_count == 16:
        ok("is_remote=true count = 16")
    else:
        fail(f"is_remote=true count = {remote_count}, expected 16")

    onsite_count = df.filter(df.is_remote == False).count()
    if onsite_count == 34:
        ok("is_remote=false count = 34")
    else:
        fail(f"is_remote=false count = {onsite_count}, expected 34")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Date Time Analytics -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "attendance_records")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_attendance_records(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
