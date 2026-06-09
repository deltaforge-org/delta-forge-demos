#!/usr/bin/env python3
"""
Delta Merge Dedup -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-dedup demo.
events: 20 raw rows (unchanged).
events_deduped: 12 deduplicated rows (latest versions kept),
12 unique event_ids.

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

def verify_events(spark, data_root, verbose=False):
    print_section("events -- Raw Table")

    table_path = os.path.join(data_root, "events")
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

def verify_events_deduped(spark, data_root, verbose=False):
    print_section("events_deduped -- Deduplicated Table")

    table_path = os.path.join(data_root, "events_deduped")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 12
    if row_count == 12:
        ok(f"Row count is {row_count} (expected 12)")
    else:
        fail(f"Row count is {row_count} (expected 12)")

    # Assert distinct count for event_id = 12
    distinct_events = df.select("event_id").distinct().count()
    if distinct_events == 12:
        ok(f"Distinct 'event_id' count is {distinct_events} (expected 12)")
    else:
        fail(f"Distinct 'event_id' count is {distinct_events} (expected 12)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge Dedup -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("events", "events_deduped"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_events(spark, data_root, verbose=verbose)
        verify_events_deduped(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
