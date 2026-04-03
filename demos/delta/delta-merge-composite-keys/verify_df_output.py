#!/usr/bin/env python3
"""
Delta Merge Composite Keys -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-composite-keys demo.
fleet_daily_summary: 25 rows (20 original + 5 new), 5 vehicles x 5 days each,
10 corrections applied via merge.
telemetry_batch: 15 source rows (read-only).

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

def verify_fleet_daily_summary(spark, data_root, verbose=False):
    print_section("fleet_daily_summary -- Final State")

    table_path = os.path.join(data_root, "fleet_daily_summary")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 25
    if row_count == 25:
        ok(f"Row count is {row_count} (expected 25)")
    else:
        fail(f"Row count is {row_count} (expected 25)")

    # Assert distinct count for vehicle_id = 5
    distinct_vehicles = df.select("vehicle_id").distinct().count()
    if distinct_vehicles == 5:
        ok(f"Distinct 'vehicle_id' count is {distinct_vehicles} (expected 5)")
    else:
        fail(f"Distinct 'vehicle_id' count is {distinct_vehicles} (expected 5)")

def verify_telemetry_batch(spark, data_root, verbose=False):
    print_section("telemetry_batch -- Source (read-only)")

    table_path = os.path.join(data_root, "telemetry_batch")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 15
    if row_count == 15:
        ok(f"Row count is {row_count} (expected 15)")
    else:
        fail(f"Row count is {row_count} (expected 15)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge Composite Keys -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("fleet_daily_summary", "telemetry_batch"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_fleet_daily_summary(spark, data_root, verbose=verbose)
        verify_telemetry_batch(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
