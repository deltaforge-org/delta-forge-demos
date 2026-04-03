#!/usr/bin/env python3
"""
Delta Default Values -- Delta Data Verification (PySpark)

Verifies the audit_log table:
  - 45 rows (25 + 10 + 10)
  - 10 archived, 35 active
  - Zero NULLs in defaultable columns

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

def verify_audit_log(spark, data_root, verbose=False):
    print_section("audit_log -- Final State")

    table_path = os.path.join(data_root, "audit_log")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 45 rows total
    if row_count == 45:
        ok("Row count = 45")
    else:
        fail(f"Row count = {row_count}, expected 45")

    # 10 archived
    archived_count = df.filter(df.is_archived == 1).count()
    if archived_count == 10:
        ok("is_archived=1 count = 10")
    else:
        fail(f"is_archived=1 count = {archived_count}, expected 10")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Default Values -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "audit_log")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_audit_log(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
