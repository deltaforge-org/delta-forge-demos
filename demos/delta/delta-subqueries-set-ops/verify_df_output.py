#!/usr/bin/env python3
"""
Delta Subqueries Set Ops -- Delta Data Verification (PySpark)
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

def verify_students(spark, data_root, verbose=False):
    print_section("students -- Final State")

    table_path = os.path.join(data_root, "students")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 15
    if row_count == 15:
        ok(f"Row count is 15")
    else:
        fail(f"Expected 15 rows, got {row_count}")

    # Assert 4 distinct majors
    distinct_majors = df.select("major").distinct().count()
    if distinct_majors == 4:
        ok(f"Distinct 'major' count is 4")
    else:
        fail(f"Expected 4 distinct 'major', got {distinct_majors}")

def verify_enrollments(spark, data_root, verbose=False):
    print_section("enrollments -- Final State")

    table_path = os.path.join(data_root, "enrollments")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 60
    if row_count == 60:
        ok(f"Row count is 60")
    else:
        fail(f"Expected 60 rows, got {row_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Subqueries Set Ops -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("students", "enrollments"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_students(spark, data_root, verbose=verbose)
        verify_enrollments(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
