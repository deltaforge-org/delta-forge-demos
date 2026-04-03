#!/usr/bin/env python3
"""
Delta Schema Flexible Ingestion -- Delta Data Verification (PySpark)
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

    # Assert row count = 20
    if row_count == 20:
        ok(f"Row count is 20")
    else:
        fail(f"Expected 20 rows, got {row_count}")

    # Assert 5 distinct sensor_ids
    distinct_sensors = df.select("sensor_id").distinct().count()
    if distinct_sensors == 5:
        ok(f"Distinct 'sensor_id' count is 5")
    else:
        fail(f"Expected 5 distinct 'sensor_id', got {distinct_sensors}")

    # Verify promoted columns exist
    for col_name in ("location", "alert_flag"):
        if col_name in df.columns:
            ok(f"Column '{col_name}' promoted from JSON")
        else:
            fail(f"Expected column '{col_name}' not found in schema")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Schema Flexible Ingestion -- Data Verification")
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
