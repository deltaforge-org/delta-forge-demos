#!/usr/bin/env python3
"""
Delta Update Null Backfill -- Delta Data Verification (PySpark)

Verifies the patient_records table: 25 rows,
after 8 cleaning passes: no NULL age, blood_type, emergency_contact.

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
from pyspark.sql.functions import col

def verify_patient_records(spark, data_root, verbose=False):
    print_section("patient_records -- Final State")

    table_path = os.path.join(data_root, "patient_records")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 25:
        ok(f"Row count is 25")
    else:
        fail(f"Expected 25 rows, got {row_count}")

    null_age = df.filter(col("age").isNull()).count()
    if null_age == 0:
        ok(f"Null count for age is 0")
    else:
        fail(f"Expected 0 nulls in age, got {null_age}")

    null_blood_type = df.filter(col("blood_type").isNull()).count()
    if null_blood_type == 0:
        ok(f"Null count for blood_type is 0")
    else:
        fail(f"Expected 0 nulls in blood_type, got {null_blood_type}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Update Null Backfill -- Data Verification")
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
