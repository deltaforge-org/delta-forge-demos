#!/usr/bin/env python3
"""
Delta Schema Evolution -- Delta Data Verification (PySpark)
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

def verify_contacts(spark, data_root, verbose=False):
    print_section("contacts -- Final State")

    table_path = os.path.join(data_root, "contacts")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 50
    if row_count == 50:
        ok(f"Row count is 50")
    else:
        fail(f"Expected 50 rows, got {row_count}")

    # Assert column count = 7
    if col_count == 7:
        ok(f"Column count is 7 after schema evolution")
    else:
        fail(f"Expected 7 columns, got {col_count}: {df.columns}")

    # Assert expected columns present
    expected_cols = {"id", "first_name", "last_name", "email", "phone", "city", "signup_date"}
    actual_cols = set(df.columns)
    if expected_cols == actual_cols:
        ok("All expected columns present")
    else:
        missing = expected_cols - actual_cols
        extra = actual_cols - expected_cols
        if missing:
            fail(f"Missing columns: {missing}")
        if extra:
            info(f"Extra columns: {extra}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Schema Evolution -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "contacts")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_contacts(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
