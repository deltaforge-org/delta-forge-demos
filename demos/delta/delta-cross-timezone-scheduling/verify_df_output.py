#!/usr/bin/env python3
"""
Delta Cross-Timezone Scheduling -- Delta Data Verification (PySpark)

Verifies the conference_schedule table:
  - 39 rows (40 - 1 deleted)
  - 6 offices, 3 priority levels

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

def verify_conference_schedule(spark, data_root, verbose=False):
    print_section("conference_schedule -- Final State")

    table_path = os.path.join(data_root, "conference_schedule")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 39 rows (40 - 1 deleted)
    if row_count == 39:
        ok("Row count = 39")
    else:
        fail(f"Row count = {row_count}, expected 39")

    # 6 distinct offices
    distinct_offices = df.select("office").distinct().count()
    if distinct_offices == 6:
        ok("Distinct office count = 6")
    else:
        fail(f"Distinct office count = {distinct_offices}, expected 6")

    # 3 priority levels
    distinct_priorities = df.select("priority").distinct().count()
    if distinct_priorities == 3:
        ok("Distinct priority count = 3")
    else:
        fail(f"Distinct priority count = {distinct_priorities}, expected 3")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Cross-Timezone Scheduling -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "conference_schedule")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_conference_schedule(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
