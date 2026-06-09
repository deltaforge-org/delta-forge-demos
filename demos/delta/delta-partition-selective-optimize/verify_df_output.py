#!/usr/bin/env python3
"""
Delta Partition Selective Optimize -- Delta Data Verification (PySpark)

Verifies the warehouse_orders table: 65 rows (75 - 10),
3 warehouses: us-east-dc 20, eu-central 20, ap-south 25.

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

def verify_warehouse_orders(spark, data_root, verbose=False):
    print_section("warehouse_orders -- Final State")

    table_path = os.path.join(data_root, "warehouse_orders")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 65:
        ok(f"Row count is 65")
    else:
        fail(f"Expected 65 rows, got {row_count}")

    distinct_wh = df.select("warehouse").distinct().count()
    if distinct_wh == 3:
        ok(f"Distinct warehouse count is 3")
    else:
        fail(f"Expected 3 distinct warehouses, got {distinct_wh}")

    for wh_name, expected in [("us-east-dc", 20), ("eu-central-dc", 20), ("ap-south-dc", 25)]:
        actual = df.filter(col("warehouse") == wh_name).count()
        if actual == expected:
            ok(f"warehouse='{wh_name}' count is {expected}")
        else:
            fail(f"Expected {expected} rows with warehouse='{wh_name}', got {actual}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Partition Selective Optimize -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "warehouse_orders")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_warehouse_orders(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
