#!/usr/bin/env python3
"""
Delta Null Statistics -- Delta Data Verification (PySpark)

Verifies the patient_records table: 45 rows (3 batches of 15),
8 distinct wards.

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

def verify_patient_records(spark, data_root, verbose=False):
    print_section("patient_records -- Final State")

    table_path = os.path.join(data_root, "patient_records")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 45:
        ok(f"Row count is 45")
    else:
        fail(f"Expected 45 rows, got {row_count}")

    distinct_wards = df.select("ward").distinct().count()
    if distinct_wards > 0:
        ok(f"Distinct ward count is {distinct_wards}")
    else:
        fail(f"Expected distinct wards > 0, got {distinct_wards}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Null Statistics -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "patient_records")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_patient_records(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
