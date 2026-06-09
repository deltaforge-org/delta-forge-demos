#!/usr/bin/env python3
"""
Delta Advanced SQL -- Delta Data Verification (PySpark)

Verifies the stock_prices table produced by the delta-advanced-sql demo.
Read-only analytics demo: 100 rows, 5 stocks (AAPL, MSFT, GOOGL, AMZN, TSLA),
20 trading days each.

Usage:
    python verify_df_output.py <data_root_path> [--verbose]
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (ok, fail, info,
    print_header, print_section, print_summary, exit_with_status)
from verify_lib.spark_session import get_spark, resolve_data_root


def verify_stock_prices(spark, data_root, verbose=False):
    print_section("stock_prices -- Final State")

    table_path = os.path.join(data_root, "stock_prices")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 100:
        ok("ROW_COUNT = 100")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 100")

    # Distinct symbol count
    distinct_symbols = df.select("symbol").distinct().count()
    if distinct_symbols == 5:
        ok("DISTINCT symbol = 5")
    else:
        fail(f"DISTINCT symbol = {distinct_symbols}, expected 5")

    # Per-symbol counts
    for symbol, expected in [("AAPL", 20), ("MSFT", 20), ("GOOGL", 20), ("AMZN", 20), ("TSLA", 20)]:
        cnt = df.filter(df.symbol == symbol).count()
        if cnt == expected:
            ok(f"COUNT WHERE symbol='{symbol}' = {expected}")
        else:
            fail(f"COUNT WHERE symbol='{symbol}' = {cnt}, expected {expected}")


def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Advanced SQL -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "stock_prices")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_stock_prices(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
