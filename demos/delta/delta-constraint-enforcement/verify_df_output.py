#!/usr/bin/env python3
"""
Delta Constraint Enforcement -- Delta Data Verification (PySpark)

Verifies the validated_employees table: 45 rows (50 - 5 rejected).
All salaries > 0, all ratings 0-5, all ages 18-70.

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

def verify_validated_employees(spark, data_root, verbose=False):
    print_section("validated_employees -- Final State")

    table_path = os.path.join(data_root, "validated_employees")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 45:
        ok("ROW_COUNT = 45")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 45")

    # Verify all salaries > 0
    df.createOrReplaceTempView("validated_employees")

    bad_salary = spark.sql("SELECT COUNT(*) AS cnt FROM validated_employees WHERE salary <= 0").collect()[0]["cnt"]
    if bad_salary == 0:
        ok("All salaries > 0")
    else:
        fail(f"Found {bad_salary} rows with salary <= 0")

    # Verify all ratings in [0, 5]
    bad_rating = spark.sql("SELECT COUNT(*) AS cnt FROM validated_employees WHERE rating < 0 OR rating > 5").collect()[0]["cnt"]
    if bad_rating == 0:
        ok("All ratings in range [0, 5]")
    else:
        fail(f"Found {bad_rating} rows with rating outside [0, 5]")

    # Verify all ages in [18, 70]
    bad_age = spark.sql("SELECT COUNT(*) AS cnt FROM validated_employees WHERE age < 18 OR age > 70").collect()[0]["cnt"]
    if bad_age == 0:
        ok("All ages in range [18, 70]")
    else:
        fail(f"Found {bad_age} rows with age outside [18, 70]")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Constraint Enforcement -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "validated_employees")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_validated_employees(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
