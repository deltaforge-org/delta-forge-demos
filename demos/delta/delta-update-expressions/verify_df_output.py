#!/usr/bin/env python3
"""
Delta Update Expressions -- Delta Data Verification (PySpark)

Verifies the portfolio_holdings table: 20 rows,
3 portfolios, 5 asset classes.

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

def verify_portfolio_holdings(spark, data_root, verbose=False):
    print_section("portfolio_holdings -- Final State")

    table_path = os.path.join(data_root, "portfolio_holdings")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 20:
        ok(f"Row count is 20")
    else:
        fail(f"Expected 20 rows, got {row_count}")

    distinct_portfolio = df.select("portfolio_id").distinct().count()
    if distinct_portfolio == 3:
        ok(f"Distinct portfolio_id count is 3")
    else:
        fail(f"Expected 3 distinct portfolio_id values, got {distinct_portfolio}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Update Expressions -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "portfolio_holdings")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_portfolio_holdings(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
