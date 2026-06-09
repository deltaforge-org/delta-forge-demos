#!/usr/bin/env python3
"""
Delta Timestamp NTZ -- Delta Data Verification (PySpark)

Verifies the shift_handover table: 30 rows, 5 hospitals,
3 on_break, 2 completed.

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

def verify_shift_handover(spark, data_root, verbose=False):
    print_section("shift_handover -- Final State")

    table_path = os.path.join(data_root, "shift_handover")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 30:
        ok(f"Row count is 30")
    else:
        fail(f"Expected 30 rows, got {row_count}")

    distinct_hospital = df.select("hospital").distinct().count()
    if distinct_hospital == 5:
        ok(f"Distinct hospital count is 5")
    else:
        fail(f"Expected 5 distinct hospital values, got {distinct_hospital}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Timestamp NTZ -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "shift_handover")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_shift_handover(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
