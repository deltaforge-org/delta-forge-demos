#!/usr/bin/env python3
"""
Iceberg V3 UniForm Investment Portfolio Audit Trail -- Data Verification
=========================================================================
Reads the portfolio_holdings table through Iceberg metadata after four
mutation rounds: rebalance (price updates), DRIP (share increases),
position exits (deletes), and new acquisitions (inserts).
Final state: 25 holdings.

Usage:
    python verify.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_count_where, assert_distinct_count,
    assert_value_where)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def assert_count_where_in(table, filter_col, filter_vals, expected, label=""):
    import pyarrow.compute as pc
    mask = pc.is_in(table.column(filter_col), value_set=filter_vals)
    actual = pc.sum(mask).as_py()
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"COUNT WHERE {filter_col} IN ... = {expected}{ctx}")
    else:
        fail(f"COUNT WHERE {filter_col} IN ... = {actual}, expected {expected}{ctx}")


def verify_portfolio_holdings(data_root, verbose=False):
    import pyarrow as pa
    import pyarrow.compute as pc

    print_section("portfolio_holdings -- Post-Mutation Final State")

    table_path = os.path.join(data_root, "portfolio_holdings")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # 25 seed - 3 deleted + 3 inserted = 25
    assert_row_count(table, 25)

    # Grand totals
    shares = pc.cast(table.column("shares"), pa.float64())
    market_price = table.column("market_price")
    cost_basis = table.column("cost_basis")
    market_values = pc.multiply(shares, market_price)
    cost_values = pc.multiply(shares, cost_basis)

    total_market = round(pc.sum(market_values).as_py(), 2)
    total_cost = round(pc.sum(cost_values).as_py(), 2)

    if total_market == 470295.4:
        ok(f"Total market value = 470295.4")
    else:
        fail(f"Total market value = {total_market}, expected 470295.4")

    if total_cost == 354170.0:
        ok(f"Total cost basis = 354170.0")
    else:
        fail(f"Total cost basis = {total_cost}, expected 354170.0")

    # Account and sector counts
    assert_distinct_count(table, "account", 3)
    assert_distinct_count(table, "sector", 5)

    # Per-account holding counts and market values
    for acct, expected_count, expected_value in [
        ("IRA-401K", 10, 279038.9),
        ("ROTH-IRA", 8, 106921.5),
        ("TAXABLE", 7, 84335.0),
    ]:
        mask = pc.equal(table.column("account"), acct)
        filtered = table.filter(mask)
        actual_count = filtered.num_rows
        if actual_count == expected_count:
            ok(f"Holdings in {acct} = {expected_count}")
        else:
            fail(f"Holdings in {acct} = {actual_count}, expected {expected_count}")

        f_shares = pc.cast(filtered.column("shares"), pa.float64())
        f_prices = filtered.column("market_price")
        f_values = pc.multiply(f_shares, f_prices)
        actual_value = round(pc.sum(f_values).as_py(), 2)
        if actual_value == expected_value:
            ok(f"Market value for {acct} = {expected_value}")
        else:
            fail(f"Market value for {acct} = {actual_value}, expected {expected_value}")

    # Deleted holdings are gone
    deleted_ids = pa.array([12, 20, 25])
    assert_count_where_in(table, "holding_id", deleted_ids, 0, "exited positions gone")

    # AAPL: mutated across V2 (price) and V3 (shares)
    assert_value_where(table, "ticker", "AAPL", "holding_id", 1)
    assert_value_where(table, "shares", 165, "holding_id", 1)         # DRIP: 150 -> 165
    assert_value_where(table, "market_price", 192.3, "holding_id", 1)  # rebalance: 178.5 -> 192.3
    assert_value_where(table, "cost_basis", 145.0, "holding_id", 1)    # unchanged
    assert_value_where(table, "account", "IRA-401K", "holding_id", 1)

    # MSFT: price rebalanced
    assert_value_where(table, "market_price", 430.5, "holding_id", 2)  # 280 -> 430.5

    # JNJ: shares DRIP
    assert_value_where(table, "shares", 88, "holding_id", 3)  # 80 -> 88

    # KO: shares DRIP
    assert_value_where(table, "shares", 220, "holding_id", 15)  # 200 -> 220

    # NVDA: untouched high-value
    assert_value_where(table, "ticker", "NVDA", "holding_id", 7)
    assert_value_where(table, "shares", 60, "holding_id", 7)
    assert_value_where(table, "market_price", 875.3, "holding_id", 7)
    assert_value_where(table, "sector", "Technology", "holding_id", 7)

    # New acquisitions (V5 inserts)
    assert_value_where(table, "ticker", "META", "holding_id", 26)
    assert_value_where(table, "shares", 80, "holding_id", 26)
    assert_value_where(table, "market_price", 512.4, "holding_id", 26)
    assert_value_where(table, "cost_basis", 485.0, "holding_id", 26)

    assert_value_where(table, "ticker", "LLY", "holding_id", 27)
    assert_value_where(table, "shares", 30, "holding_id", 27)
    assert_value_where(table, "market_price", 790.2, "holding_id", 27)

    assert_value_where(table, "ticker", "COP", "holding_id", 28)
    assert_value_where(table, "shares", 140, "holding_id", 28)
    assert_value_where(table, "market_price", 118.5, "holding_id", 28)

    # Price-updated holdings from V2 rebalance
    assert_value_where(table, "market_price", 42.1, "holding_id", 16)
    assert_value_where(table, "market_price", 265.4, "holding_id", 18)  # TSLA


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-time-travel demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing portfolio_holdings/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Time Travel -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "portfolio_holdings")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_portfolio_holdings(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
