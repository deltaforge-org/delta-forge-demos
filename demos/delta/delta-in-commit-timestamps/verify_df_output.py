#!/usr/bin/env python3
"""
Delta In-Commit Timestamps -- Delta Data Verification (PySpark)

Verifies the release_tracker table produced by the delta-in-commit-timestamps demo.
30 rows: 25 successful, 3 rolled_back, 2 failed.
Environments: 15 production, 10 staging, 5 development.

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

def verify_release_tracker(spark, data_root, verbose=False):
    print_section("release_tracker -- Final State")

    table_path = os.path.join(data_root, "release_tracker")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 30
    if row_count == 30:
        ok(f"Row count is {row_count} (expected 30)")
    else:
        fail(f"Row count is {row_count} (expected 30)")

    # Assert count where status = 'success' is 25
    successful_count = df.filter(col("status") == "success").count()
    if successful_count == 25:
        ok(f"Count where 'status' == 'success' is {successful_count} (expected 25)")
    else:
        fail(f"Count where 'status' == 'success' is {successful_count} (expected 25)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta In-Commit Timestamps -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "release_tracker")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_release_tracker(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
