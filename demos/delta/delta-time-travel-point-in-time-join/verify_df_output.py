#!/usr/bin/env python3
"""
Delta Time Travel Point-in-Time Join -- Delta Data Verification (PySpark)
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

def verify_fx_trades(spark, data_root, verbose=False):
    print_section("fx_trades -- Final State")

    table_path = os.path.join(data_root, "fx_trades")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 12
    if row_count == 12:
        ok(f"Row count is 12")
    else:
        fail(f"Expected 12 rows, got {row_count}")

    # Assert 3 distinct pairs
    distinct_pairs = df.select("pair").distinct().count()
    if distinct_pairs == 3:
        ok(f"Distinct 'pair' count is 3")
    else:
        fail(f"Expected 3 distinct 'pair', got {distinct_pairs}")

def verify_fx_rates(spark, data_root, verbose=False):
    print_section("fx_rates -- Final State")

    table_path = os.path.join(data_root, "fx_rates")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 3
    if row_count == 3:
        ok(f"Row count is 3")
    else:
        fail(f"Expected 3 rows, got {row_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Time Travel Point-in-Time Join -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("fx_trades", "fx_rates"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_fx_trades(spark, data_root, verbose=verbose)
        verify_fx_rates(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
