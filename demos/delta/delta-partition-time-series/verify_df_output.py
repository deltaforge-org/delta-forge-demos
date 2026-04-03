#!/usr/bin/env python3
"""
Delta Partition Time Series -- Delta Data Verification (PySpark)

Verifies the line_metrics table: 102 rows (90 + 15 - 3),
7 months, 5 sensors.

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

def verify_line_metrics(spark, data_root, verbose=False):
    print_section("line_metrics -- Final State")

    table_path = os.path.join(data_root, "line_metrics")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 102:
        ok(f"Row count is 102")
    else:
        fail(f"Expected 102 rows, got {row_count}")

    distinct_sensors = df.select("sensor_id").distinct().count()
    if distinct_sensors == 5:
        ok(f"Distinct sensor_id count is 5")
    else:
        fail(f"Expected 5 distinct sensor_ids, got {distinct_sensors}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Partition Time Series -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "line_metrics")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_line_metrics(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
