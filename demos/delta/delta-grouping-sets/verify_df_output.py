#!/usr/bin/env python3
"""
Delta Grouping Sets -- Delta Data Verification (PySpark)

Verifies the production_runs table produced by the delta-grouping-sets demo.
36 rows: 3 production lines x 3 shifts x 4 dates.

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

def verify_production_runs(spark, data_root, verbose=False):
    print_section("production_runs -- Final State")

    table_path = os.path.join(data_root, "production_runs")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 36
    if row_count == 36:
        ok(f"Row count is {row_count} (expected 36)")
    else:
        fail(f"Row count is {row_count} (expected 36)")

    # Assert distinct count for production_line = 3
    distinct_pl = df.select("production_line").distinct().count()
    if distinct_pl == 3:
        ok(f"Distinct 'production_line' count is {distinct_pl} (expected 3)")
    else:
        fail(f"Distinct 'production_line' count is {distinct_pl} (expected 3)")

    # Assert distinct count for shift = 3
    distinct_shift = df.select("shift").distinct().count()
    if distinct_shift == 3:
        ok(f"Distinct 'shift' count is {distinct_shift} (expected 3)")
    else:
        fail(f"Distinct 'shift' count is {distinct_shift} (expected 3)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Grouping Sets -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "production_runs")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_production_runs(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
