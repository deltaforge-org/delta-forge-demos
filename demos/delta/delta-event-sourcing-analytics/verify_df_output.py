#!/usr/bin/env python3
"""
Delta Event Sourcing Analytics -- Delta Data Verification (PySpark)

Verifies the ed_events table (emergency department patient flow):
  - 35 rows total (14 + 10 + 11 events)
  - 10 distinct patients (P001-P010)
  - 4 event types: 10 triage, 10 admit, 5 transfer, 10 discharge
  - 7 distinct departments

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

def verify_ed_events(spark, data_root, verbose=False):
    print_section("ed_events -- Final State")

    table_path = os.path.join(data_root, "ed_events")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 35 total events
    if row_count == 35:
        ok("Row count = 35")
    else:
        fail(f"Row count = {row_count}, expected 35")

    # 10 distinct patients
    distinct_patients = df.select("patient_id").distinct().count()
    if distinct_patients == 10:
        ok("Distinct patient_id count = 10")
    else:
        fail(f"Distinct patient_id count = {distinct_patients}, expected 10")

    # Event type breakdown
    for event_type, expected in [("triage", 10), ("admit", 10), ("transfer", 5), ("discharge", 10)]:
        actual = df.filter(df.event_type == event_type).count()
        if actual == expected:
            ok(f"event_type='{event_type}' count = {expected}")
        else:
            fail(f"event_type='{event_type}' count = {actual}, expected {expected}")

    # 7 distinct departments
    distinct_depts = df.select("department").distinct().count()
    if distinct_depts == 7:
        ok("Distinct department count = 7")
    else:
        fail(f"Distinct department count = {distinct_depts}, expected 7")

    # All events have non-null payloads
    null_count = df.filter(F.col("payload").isNull()).count()
    if null_count == 0:
        ok("All payload values are non-null")
    else:
        fail(f"{null_count} NULL payload values found, expected 0")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Event Sourcing Analytics -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "ed_events")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_ed_events(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
