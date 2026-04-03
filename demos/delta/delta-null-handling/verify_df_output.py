#!/usr/bin/env python3
"""
Delta Null Handling -- Delta Data Verification (PySpark)

Verifies the survey_responses table: 30 rows,
8 null phone values, 6 null company values.

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

def verify_survey_responses(spark, data_root, verbose=False):
    print_section("survey_responses -- Final State")

    table_path = os.path.join(data_root, "survey_responses")
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

    null_phone = df.filter(col("phone").isNull()).count()
    if null_phone == 8:
        ok(f"Null phone count is 8")
    else:
        fail(f"Expected 8 null phone values, got {null_phone}")

    null_company = df.filter(col("company").isNull()).count()
    if null_company == 6:
        ok(f"Null company count is 6")
    else:
        fail(f"Expected 6 null company values, got {null_company}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Null Handling -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "survey_responses")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_survey_responses(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
