#!/usr/bin/env python3
"""
Delta Restore Selective Recovery -- Delta Data Verification (PySpark)
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

def verify_portfolio_positions(spark, data_root, verbose=False):
    print_section("portfolio_positions -- Final State")

    table_path = os.path.join(data_root, "portfolio_positions")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 24
    if row_count == 24:
        ok(f"Row count is 24")
    else:
        fail(f"Expected 24 rows, got {row_count}")

    # Assert 3 distinct portfolios
    distinct_portfolios = df.select("portfolio").distinct().count()
    if distinct_portfolios == 3:
        ok(f"Distinct 'portfolio' count is 3")
    else:
        fail(f"Expected 3 distinct 'portfolio', got {distinct_portfolios}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Restore Selective Recovery -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "portfolio_positions")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_portfolio_positions(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
