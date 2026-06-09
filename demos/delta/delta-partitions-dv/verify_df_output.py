#!/usr/bin/env python3
"""
Delta Partitions DV -- Delta Data Verification (PySpark)

Verifies the cloud_events table: 85 rows (90 - 15 + 10),
21 critical severity events (11 escalated + 10 new inserts).

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

def verify_cloud_events(spark, data_root, verbose=False):
    print_section("cloud_events -- Final State")

    table_path = os.path.join(data_root, "cloud_events")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 85:
        ok(f"Row count is 85")
    else:
        fail(f"Expected 85 rows, got {row_count}")

    critical = df.filter(col("severity") == "critical").count()
    if critical == 21:
        ok(f"severity='critical' count is 21")
    else:
        fail(f"Expected 21 rows with severity='critical', got {critical}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Partitions DV -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "cloud_events")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_cloud_events(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
