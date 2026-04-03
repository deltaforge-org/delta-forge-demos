#!/usr/bin/env python3
"""
Delta Restore Vacuum Lifecycle -- Delta Data Verification (PySpark)
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

def verify_sensor_readings(spark, data_root, verbose=False):
    print_section("sensor_readings -- Final State")

    table_path = os.path.join(data_root, "sensor_readings")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 35
    if row_count == 35:
        ok(f"Row count is 35")
    else:
        fail(f"Expected 35 rows, got {row_count}")

    # Assert all status='normal'
    normal_count = df.filter(col("status") == "normal").count()
    if normal_count == 35:
        ok(f"status='normal' count is 35")
    else:
        fail(f"Expected 35 rows where status='normal', got {normal_count}")

    # Assert 3 distinct rooms
    distinct_rooms = df.select("room").distinct().count()
    if distinct_rooms == 3:
        ok(f"Distinct 'room' count is 3")
    else:
        fail(f"Expected 3 distinct 'room', got {distinct_rooms}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Restore Vacuum Lifecycle -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "sensor_readings")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_sensor_readings(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
