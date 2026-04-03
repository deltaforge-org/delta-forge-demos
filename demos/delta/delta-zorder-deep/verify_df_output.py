#!/usr/bin/env python3
"""
Delta Z-Order Deep -- Delta Data Verification (PySpark)

Verifies the sensor_telemetry table: 80 rows,
4 regions, 4 sensor types.

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

def verify_sensor_telemetry(spark, data_root, verbose=False):
    print_section("sensor_telemetry -- Final State")

    table_path = os.path.join(data_root, "sensor_telemetry")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 80:
        ok(f"Row count is 80")
    else:
        fail(f"Expected 80 rows, got {row_count}")

    distinct_region = df.select("region").distinct().count()
    if distinct_region == 4:
        ok(f"Distinct region count is 4")
    else:
        fail(f"Expected 4 distinct region values, got {distinct_region}")

    distinct_sensor_type = df.select("sensor_type").distinct().count()
    if distinct_sensor_type == 4:
        ok(f"Distinct sensor_type count is 4")
    else:
        fail(f"Expected 4 distinct sensor_type values, got {distinct_sensor_type}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Z-Order Deep -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "sensor_telemetry")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_sensor_telemetry(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
