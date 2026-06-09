#!/usr/bin/env python3
"""
Iceberg Native Time Travel -- Stock Prices -- Data Verification
================================================================
Reads the stock_prices table through Iceberg metadata and verifies
the final state after 120 initial rows + 30 IPO inserts - 12 delisted
removals = 138 rows, across 4 sectors and 23 tickers.

Usage:
    python verify_df_output.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (
    read_iceberg_table, ok, fail, info,
    assert_row_count, assert_avg, assert_sum,
    assert_distinct_count, assert_count_where,
    assert_format_version, assert_value_where,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_stock_prices(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("stock_prices -- Time Travel")

    table_path = os.path.join(data_root, "stock_prices")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)

    # 120 initial + 30 IPO - 12 delisted = 138
    assert_row_count(table, 138)

    # Sector distribution
    assert_distinct_count(table, "sector", 4)
    assert_count_where(table, "sector", "Technology", 60)
    assert_count_where(table, "sector", "Healthcare", 30)
    assert_count_where(table, "sector", "Finance", 30)
    assert_count_where(table, "sector", "Energy", 18)

    # 23 distinct tickers
    assert_distinct_count(table, "ticker", 23)

    # Delisted tickers COP, SLB have 0 rows
    assert_count_where(table, "ticker", "COP", 0, label="COP delisted")
    assert_count_where(table, "ticker", "SLB", 0, label="SLB delisted")

    # IPO tickers: 30 rows total
    ipo_tickers = ["BIOT", "FINX", "GRNH", "NWAI", "QCMP"]
    ipo_total = 0
    for t in ipo_tickers:
        mask = pc.equal(table.column("ticker"), t)
        count = pc.sum(mask).as_py()
        ipo_total += count
    if ipo_total == 30:
        ok("IPO tickers (BIOT, FINX, GRNH, NWAI, QCMP) total rows = 30")
    else:
        fail(f"IPO tickers total rows = {ipo_total}, expected 30")

    # Grand averages
    assert_avg(table, "price", 239.37, label="grand_avg_price")
    assert_sum(table, "volume", 5255593877.0, label="grand_total_volume")

    # Spot checks: per-ticker avg price
    for ticker, expected_avg in [("AAPL", 196.40), ("MSFT", 438.53)]:
        mask = pc.equal(table.column("ticker"), ticker)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("price")).as_py(), 2)
        if actual == expected_avg:
            ok(f"Avg price for {ticker} = {expected_avg}")
        else:
            fail(f"Avg price for {ticker} = {actual}, expected {expected_avg}")

    # JPM unchanged: trade_date='2025-01-06' price=196.12
    jpm_mask = pc.and_(
        pc.equal(table.column("ticker"), "JPM"),
        pc.equal(pc.cast(table.column("trade_date"), "string"), "2025-01-06"),
    )
    jpm_rows = table.filter(jpm_mask)
    if jpm_rows.num_rows == 1:
        jpm_price = round(jpm_rows.column("price")[0].as_py(), 2)
        if jpm_price == 196.12:
            ok("JPM trade_date=2025-01-06 price = 196.12")
        else:
            fail(f"JPM trade_date=2025-01-06 price = {jpm_price}, expected 196.12")
    else:
        fail(f"JPM trade_date=2025-01-06 rows = {jpm_rows.num_rows}, expected 1")

    # NWAI IPO: trade_date='2025-01-06' price=45.37
    nwai_mask = pc.and_(
        pc.equal(table.column("ticker"), "NWAI"),
        pc.equal(pc.cast(table.column("trade_date"), "string"), "2025-01-06"),
    )
    nwai_rows = table.filter(nwai_mask)
    if nwai_rows.num_rows == 1:
        nwai_price = round(nwai_rows.column("price")[0].as_py(), 2)
        if nwai_price == 45.37:
            ok("NWAI trade_date=2025-01-06 price = 45.37")
        else:
            fail(f"NWAI trade_date=2025-01-06 price = {nwai_price}, expected 45.37")
    else:
        fail(f"NWAI trade_date=2025-01-06 rows = {nwai_rows.num_rows}, expected 1")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-native-time-travel demo"
    )
    parser.add_argument("data_root", help="Root path containing stock_prices/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg Native Time Travel -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "stock_prices")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_stock_prices(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
