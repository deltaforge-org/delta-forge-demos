#!/usr/bin/env python3
"""
Delta Duration Arithmetic -- Delta Data Verification (PySpark)

Verifies the vessel_transit table:
  - 35 rows total
  - 30 completed, 3 in_transit, 2 delayed
  - 4 vessel types

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

def verify_vessel_transit(spark, data_root, verbose=False):
    print_section("vessel_transit -- Final State")

    table_path = os.path.join(data_root, "vessel_transit")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 35 rows total
    if row_count == 35:
        ok("Row count = 35")
    else:
        fail(f"Row count = {row_count}, expected 35")

    # Status breakdown
    for status, expected in [("completed", 30), ("in_transit", 3), ("delayed", 2)]:
        actual = df.filter(df.status == status).count()
        if actual == expected:
            ok(f"status='{status}' count = {expected}")
        else:
            fail(f"status='{status}' count = {actual}, expected {expected}")

    # 4 vessel types
    distinct_types = df.select("vessel_type").distinct().count()
    if distinct_types == 4:
        ok("Distinct vessel_type count = 4")
    else:
        fail(f"Distinct vessel_type count = {distinct_types}, expected 4")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Duration Arithmetic -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "vessel_transit")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_vessel_transit(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
