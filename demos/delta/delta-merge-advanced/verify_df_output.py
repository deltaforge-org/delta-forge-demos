#!/usr/bin/env python3
"""
Delta Merge Advanced -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-advanced demo.
inventory_master: 52 rows (40 original - 3 discontinued + 15 new).
inventory_updates: 30 source rows (read-only).

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

def verify_inventory_master(spark, data_root, verbose=False):
    print_section("inventory_master -- Final State")

    table_path = os.path.join(data_root, "inventory_master")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 52
    if row_count == 52:
        ok(f"Row count is {row_count} (expected 52)")
    else:
        fail(f"Row count is {row_count} (expected 52)")

    # Verify discontinued SKUs were deleted
    discontinued = ["DISC-001", "DISC-002", "DISC-003"]
    found_rows = df.filter(col("sku").isin(discontinued)).collect()
    found = [row["sku"] for row in found_rows]
    if len(found) == 0:
        ok("Discontinued SKUs correctly absent from inventory_master")
    else:
        fail(f"Discontinued SKUs still present: {found}")

def verify_inventory_updates(spark, data_root, verbose=False):
    print_section("inventory_updates -- Source (read-only)")

    table_path = os.path.join(data_root, "inventory_updates")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 30
    if row_count == 30:
        ok(f"Row count is {row_count} (expected 30)")
    else:
        fail(f"Row count is {row_count} (expected 30)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge Advanced -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("inventory_master", "inventory_updates"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_inventory_master(spark, data_root, verbose=verbose)
        verify_inventory_updates(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
