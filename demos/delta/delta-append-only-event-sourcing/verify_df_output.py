#!/usr/bin/env python3
"""
Delta Append-Only Event Sourcing -- Delta Data Verification (PySpark)

Verifies the order_events table: 65 rows, append-only,
14 distinct order_ids, 7 event types.

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

def verify_order_events(spark, data_root, verbose=False):
    print_section("order_events -- Final State")

    table_path = os.path.join(data_root, "order_events")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 65:
        ok("ROW_COUNT = 65")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 65")

    # Distinct order_id count
    distinct_orders = df.select("order_id").distinct().count()
    if distinct_orders == 14:
        ok("DISTINCT order_id = 14")
    else:
        fail(f"DISTINCT order_id = {distinct_orders}, expected 14")

    # Distinct event_type count
    distinct_events = df.select("event_type").distinct().count()
    if distinct_events == 7:
        ok("DISTINCT event_type = 7")
    else:
        fail(f"DISTINCT event_type = {distinct_events}, expected 7")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Append-Only Event Sourcing -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "order_events")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_order_events(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
