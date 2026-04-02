#!/usr/bin/env python3
"""
Iceberg UniForm Column Rename (Field-ID Stability) — Data Verification
=========================================================================
Reads the financial_transactions table purely through the Iceberg metadata
chain and verifies the final state after column renames:
  - 24 transactions seeded with legacy names: amt, ccy, acct_num (V1)
  - RENAME amt -> transaction_amount (V2)
  - RENAME ccy -> currency_code (V3)
  - RENAME acct_num -> account_number (V4)
  - INSERT 4 new transactions with renamed columns (V5)

Final state: 28 transactions, columns use IFRS-standardized names.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing financial_transactions/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_distinct_count,
    assert_count_where, assert_value_where, assert_format_version,
    print_header, print_section, print_summary, exit_with_status)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_financial_transactions(data_root, verbose=False):
    print_section("financial_transactions — Column Rename")

    table_path = os.path.join(data_root, "financial_transactions")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    assert_format_version(metadata, 1)

    # Final state: 28 transactions (24 original + 4 inserted)
    assert_row_count(table, 28)

    # Renamed columns should appear with new names via field-ID mapping
    if "transaction_amount" in table.column_names:
        ok(f"Renamed column 'transaction_amount' present (was 'amt')")
    else:
        fail(f"Renamed column 'transaction_amount' not found in {table.column_names}")

    if "currency_code" in table.column_names:
        ok(f"Renamed column 'currency_code' present (was 'ccy')")
    else:
        fail(f"Renamed column 'currency_code' not found in {table.column_names}")

    if "account_number" in table.column_names:
        ok(f"Renamed column 'account_number' present (was 'acct_num')")
    else:
        fail(f"Renamed column 'account_number' not found in {table.column_names}")

    # Old names should NOT appear
    for old_name in ["amt", "ccy", "acct_num"]:
        if old_name not in table.column_names:
            ok(f"Old column name '{old_name}' not in schema")
        else:
            fail(f"Old column name '{old_name}' still in schema")

    # Total amount = 84986.98
    assert_sum(table, "transaction_amount", 84986.98)

    # Distinct types = 4, currencies = 3
    assert_distinct_count(table, "txn_type", 4)
    assert_distinct_count(table, "currency_code", 3)

    # Distinct accounts = 12 (original) + 4 new = 16
    assert_distinct_count(table, "account_number", 16)

    import pyarrow.compute as pc

    # Per-type totals (after insert)
    for txn_type, expected_total in [
        ("checking", 5726.50),
        ("credit", 1860.48),
        ("investment", 56500.00),
        ("savings", 20900.00),
    ]:
        mask = pc.equal(table.column("txn_type"), txn_type)
        filtered = table.filter(mask)
        actual = round(pc.sum(filtered.column("transaction_amount")).as_py(), 2)
        if actual == expected_total:
            ok(f"SUM(transaction_amount) for {txn_type} = {expected_total}")
        else:
            fail(f"SUM(transaction_amount) for {txn_type} = {actual}, expected {expected_total}")

    # Per-currency totals
    for currency, expected_total in [
        ("EUR", 28540.00),
        ("GBP", 14805.99),
        ("USD", 41640.99),
    ]:
        mask = pc.equal(table.column("currency_code"), currency)
        filtered = table.filter(mask)
        actual = round(pc.sum(filtered.column("transaction_amount")).as_py(), 2)
        if actual == expected_total:
            ok(f"SUM(transaction_amount) for {currency} = {expected_total}")
        else:
            fail(f"SUM(transaction_amount) for {currency} = {actual}, expected {expected_total}")

    # Spot-check specific rows via renamed columns
    assert_value_where(table, "transaction_amount", 1500.00, "txn_id", 1)
    assert_value_where(table, "currency_code", "USD", "txn_id", 1)
    assert_value_where(table, "account_number", "CHK-10001", "txn_id", 1)
    assert_value_where(table, "transaction_amount", 7500.00, "txn_id", 8)
    assert_value_where(table, "currency_code", "EUR", "txn_id", 8)
    assert_value_where(table, "transaction_amount", 12000.00, "txn_id", 23)

    # New rows (V5)
    assert_value_where(table, "transaction_amount", 950.00, "txn_id", 25)
    assert_value_where(table, "account_number", "CHK-10004", "txn_id", 25)
    assert_value_where(table, "transaction_amount", 7000.00, "txn_id", 28)
    assert_value_where(table, "currency_code", "USD", "txn_id", 28)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-schema-rename demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing financial_transactions/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Column Rename — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "financial_transactions")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_financial_transactions(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
