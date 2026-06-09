#!/usr/bin/env python3
"""
Delta Funnel Analysis -- Delta Data Verification (PySpark)

Verifies the user_events table (SaaS conversion funnel):
  - 100 rows total (40 trial_start + 20 activation + 20 subscription
    + 10 renewal + 10 churned)
  - 40 distinct users
  - Total revenue = 2000
  - 10 churned users (activated but never subscribed, relabeled)

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

def verify_user_events(spark, data_root, verbose=False):
    print_section("user_events -- Final State")

    table_path = os.path.join(data_root, "user_events")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 100 rows total
    if row_count == 100:
        ok("Row count = 100")
    else:
        fail(f"Row count = {row_count}, expected 100")

    # 40 distinct users
    distinct_users = df.select("user_id").distinct().count()
    if distinct_users == 40:
        ok("Distinct user_id count = 40")
    else:
        fail(f"Distinct user_id count = {distinct_users}, expected 40")

    # Total revenue = 2000
    total_rev = df.agg(F.sum("revenue")).first()[0]
    total_rev = round(float(total_rev), 2) if total_rev is not None else 0.0
    if total_rev == 2000.0:
        ok("SUM(revenue) = 2000")
    else:
        fail(f"SUM(revenue) = {total_rev}, expected 2000")

    # Event type breakdown after UPDATE (churned relabeling)
    for event_type, expected in [("trial_start", 40), ("activation", 20), ("subscription", 20), ("renewal", 10), ("churned", 10)]:
        actual = df.filter(df.event_type == event_type).count()
        if actual == expected:
            ok(f"event_type='{event_type}' count = {expected}")
        else:
            fail(f"event_type='{event_type}' count = {actual}, expected {expected}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Funnel Analysis -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "user_events")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_user_events(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
