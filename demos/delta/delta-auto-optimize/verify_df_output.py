#!/usr/bin/env python3
"""
Delta Auto-Optimize -- Delta Data Verification (PySpark)

Verifies the iot_readings table: 70 rows,
7 metric types, 10 devices, 8 rows with quality='poor'.

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

def verify_iot_readings(spark, data_root, verbose=False):
    print_section("iot_readings -- Final State")

    table_path = os.path.join(data_root, "iot_readings")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 70:
        ok("ROW_COUNT = 70")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 70")

    # Distinct metric_type count
    distinct_metrics = df.select("metric_type").distinct().count()
    if distinct_metrics == 7:
        ok("DISTINCT metric_type = 7")
    else:
        fail(f"DISTINCT metric_type = {distinct_metrics}, expected 7")

    # Distinct device_id count
    distinct_devices = df.select("device_id").distinct().count()
    if distinct_devices == 10:
        ok("DISTINCT device_id = 10")
    else:
        fail(f"DISTINCT device_id = {distinct_devices}, expected 10")

    # Count where quality='poor'
    poor_count = df.filter(df.quality == "poor").count()
    if poor_count == 8:
        ok("COUNT WHERE quality='poor' = 8")
    else:
        fail(f"COUNT WHERE quality='poor' = {poor_count}, expected 8")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Auto-Optimize -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "iot_readings")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_iot_readings(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
