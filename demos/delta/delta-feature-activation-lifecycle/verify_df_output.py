#!/usr/bin/env python3
"""
Delta Feature Activation Lifecycle -- Delta Data Verification (PySpark)

Verifies the customer_events table after CDC activation and web discount:
  - 25 rows (unchanged count -- features don't add/remove rows)
  - 8 distinct customers, 3 event types, 5 channels
  - Total revenue = 3646.5 (after 10% web purchase discount)
  - Store revenue = 925.0 (unchanged)
  - 17 purchases, 4 signups, 4 refunds

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
from pyspark.sql import functions as F

def verify_customer_events(spark, data_root, verbose=False):
    print_section("customer_events -- Final State")

    table_path = os.path.join(data_root, "customer_events")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 25 rows unchanged
    if row_count == 25:
        ok("Row count = 25")
    else:
        fail(f"Row count = {row_count}, expected 25")

    # 8 distinct customers
    distinct_customers = df.select("customer_id").distinct().count()
    if distinct_customers == 8:
        ok("Distinct customer_id count = 8")
    else:
        fail(f"Distinct customer_id count = {distinct_customers}, expected 8")

    # 3 event types
    distinct_events = df.select("event_type").distinct().count()
    if distinct_events == 3:
        ok("Distinct event_type count = 3")
    else:
        fail(f"Distinct event_type count = {distinct_events}, expected 3")

    # Event type counts
    for event_type, expected in [("purchase", 17), ("signup", 4), ("refund", 4)]:
        actual = df.filter(df.event_type == event_type).count()
        if actual == expected:
            ok(f"event_type='{event_type}' count = {expected}")
        else:
            fail(f"event_type='{event_type}' count = {actual}, expected {expected}")

    # Total revenue after web discount = 3646.5
    total_rev = df.agg(F.sum("revenue")).first()[0]
    total_rev = round(float(total_rev), 2) if total_rev is not None else 0.0
    if total_rev == 3646.5:
        ok("Total revenue = 3646.5")
    else:
        fail(f"Total revenue = {total_rev}, expected 3646.5")

    # Store revenue unchanged = 925.0
    store_rev = df.filter(df.channel == "store").agg(F.sum("revenue")).first()[0]
    store_rev = round(float(store_rev), 2) if store_rev is not None else 0.0
    if store_rev == 925.0:
        ok("Store revenue = 925.0 (unchanged)")
    else:
        fail(f"Store revenue = {store_rev}, expected 925.0")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Feature Activation Lifecycle -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "customer_events")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_customer_events(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
