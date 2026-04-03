#!/usr/bin/env python3
"""
Delta Audit Trail Versioning -- Delta Data Verification (PySpark)

Verifies the compliance_events table: 42 rows,
10 distinct account_ids, 6 event_types.

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

def verify_compliance_events(spark, data_root, verbose=False):
    print_section("compliance_events -- Final State")

    table_path = os.path.join(data_root, "compliance_events")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 42:
        ok("ROW_COUNT = 42")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 42")

    # Distinct account_id count
    distinct_accounts = df.select("account_id").distinct().count()
    if distinct_accounts == 10:
        ok("DISTINCT account_id = 10")
    else:
        fail(f"DISTINCT account_id = {distinct_accounts}, expected 10")

    # Distinct event_type count
    distinct_events = df.select("event_type").distinct().count()
    if distinct_events == 6:
        ok("DISTINCT event_type = 6")
    else:
        fail(f"DISTINCT event_type = {distinct_events}, expected 6")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Audit Trail Versioning -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "compliance_events")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_compliance_events(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
