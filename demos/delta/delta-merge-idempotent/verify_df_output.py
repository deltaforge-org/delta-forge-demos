#!/usr/bin/env python3
"""
Delta Merge Idempotent -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-idempotent demo.
sensor_readings: 30 rows (25 original + 5 new sensors).
10 updated, 5 stale rejected, 5 new (TEMP-01 to TEMP-08 = 8 distinct sensors).
sensor_batch: 20 source rows (read-only).

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

def verify_sensor_readings(spark, data_root, verbose=False):
    print_section("sensor_readings -- Final State")

    table_path = os.path.join(data_root, "sensor_readings")
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

    # Assert distinct count for sensor_id = 8
    distinct_sensors = df.select("sensor_id").distinct().count()
    if distinct_sensors == 8:
        ok(f"Distinct 'sensor_id' count is {distinct_sensors} (expected 8)")
    else:
        fail(f"Distinct 'sensor_id' count is {distinct_sensors} (expected 8)")

def verify_sensor_batch(spark, data_root, verbose=False):
    print_section("sensor_batch -- Source (read-only)")

    table_path = os.path.join(data_root, "sensor_batch")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 20
    if row_count == 20:
        ok(f"Row count is {row_count} (expected 20)")
    else:
        fail(f"Row count is {row_count} (expected 20)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge Idempotent -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("sensor_readings", "sensor_batch"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_sensor_readings(spark, data_root, verbose=verbose)
        verify_sensor_batch(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
