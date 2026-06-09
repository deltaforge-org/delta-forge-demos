#!/usr/bin/env python3
"""
Delta Merge Multi-Source -- Delta Data Verification (PySpark)

Verifies tables produced by the delta-merge-multi-source demo.
order_status: 20 rows after merges from three source tables.
Shipping: 4 delivered, 6 shipped, 6 pending, 4 returned.
Payment: 12 captured, 4 authorized, 4 refunded.
shipping_updates: 14 source rows, payment_updates: 16 source rows,
return_updates: 4 source rows (all read-only).

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

def verify_order_status(spark, data_root, verbose=False):
    print_section("order_status -- Final State")

    table_path = os.path.join(data_root, "order_status")
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

    # Assert count where shipping_status = 'delivered' is 4
    delivered_count = df.filter(col("shipping_status") == "delivered").count()
    if delivered_count == 4:
        ok(f"Count where 'shipping_status' == 'delivered' is {delivered_count} (expected 4)")
    else:
        fail(f"Count where 'shipping_status' == 'delivered' is {delivered_count} (expected 4)")

def verify_shipping_updates(spark, data_root, verbose=False):
    print_section("shipping_updates -- Source (read-only)")

    table_path = os.path.join(data_root, "shipping_updates")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 14
    if row_count == 14:
        ok(f"Row count is {row_count} (expected 14)")
    else:
        fail(f"Row count is {row_count} (expected 14)")

def verify_payment_updates(spark, data_root, verbose=False):
    print_section("payment_updates -- Source (read-only)")

    table_path = os.path.join(data_root, "payment_updates")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 16
    if row_count == 16:
        ok(f"Row count is {row_count} (expected 16)")
    else:
        fail(f"Row count is {row_count} (expected 16)")

def verify_return_updates(spark, data_root, verbose=False):
    print_section("return_updates -- Source (read-only)")

    table_path = os.path.join(data_root, "return_updates")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 4
    if row_count == 4:
        ok(f"Row count is {row_count} (expected 4)")
    else:
        fail(f"Row count is {row_count} (expected 4)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Merge Multi-Source -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("order_status", "shipping_updates", "payment_updates", "return_updates"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_order_status(spark, data_root, verbose=verbose)
        verify_shipping_updates(spark, data_root, verbose=verbose)
        verify_payment_updates(spark, data_root, verbose=verbose)
        verify_return_updates(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
