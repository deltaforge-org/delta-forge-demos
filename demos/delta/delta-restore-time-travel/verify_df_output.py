#!/usr/bin/env python3
"""
Delta Restore Time Travel -- Delta Data Verification (PySpark)
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

def verify_course_grades(spark, data_root, verbose=False):
    print_section("course_grades -- Final State")

    table_path = os.path.join(data_root, "course_grades")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 25
    if row_count == 25:
        ok(f"Row count is 25")
    else:
        fail(f"Expected 25 rows, got {row_count}")

    # Assert 5 distinct courses
    distinct_courses = df.select("course").distinct().count()
    if distinct_courses == 5:
        ok(f"Distinct 'course' count is 5")
    else:
        fail(f"Expected 5 distinct 'course', got {distinct_courses}")

    # Verify no zero grades
    zero_count = df.filter(col("grade") == 0).count()
    if zero_count == 0:
        ok("No zero grades found")
    else:
        fail(f"Expected no zero grades, found {zero_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Restore Time Travel -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "course_grades")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_course_grades(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
