#!/usr/bin/env python3
"""
Bank Transaction Validation — CHECK Constraints with UniForm — Iceberg Data Verification
==========================================================================================
Reads the transactions table purely through the Iceberg metadata chain and verifies
the final state (no DML mutations — seed data only with CHECK constraints):
  - 25 transactions across 5 accounts, 3 currencies (USD, EUR, GBP)
  - Constraint: amount > 0
  - Constraint: currency IN ('USD', 'EUR', 'GBP')

Final state: 25 rows, total amount 71900.00, avg 2876.00.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing transactions/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (
    read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_avg, assert_min, assert_max,
    assert_distinct_count, assert_count_where, assert_value_where,
    assert_format_version,
    print_header, print_section, print_summary, exit_with_status,
)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_transactions(data_root, verbose=False):
    print_section("transactions — CHECK Constraints with UniForm")

    table_path = os.path.join(data_root, "transactions")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    import pyarrow.compute as pc

    # Format version
    assert_format_version(metadata, 1)

    # Final state: 25 rows (seed data only, no DML mutations)
    assert_row_count(table, 25)

    # 5 distinct accounts
    assert_distinct_count(table, "account_id", 5)

    # 3 distinct currencies
    assert_distinct_count(table, "currency", 3)

    # 3 distinct txn_types
    assert_distinct_count(table, "txn_type", 3)

    # Total amount = 71900.00
    assert_sum(table, "amount", 71900.00)

    # Average amount = 2876.00
    assert_avg(table, "amount", 2876.00)

    # Max amount = 12000.00, Min amount = 300.00
    assert_max(table, "amount", 12000.00)
    assert_min(table, "amount", 300.00)

    # Constraint validation: all amounts > 0
    amount_col = pc.cast(table.column("amount"), "float64")
    non_positive = pc.sum(pc.less_equal(amount_col, 0)).as_py()
    if non_positive == 0:
        ok("Constraint: all amounts > 0")
    else:
        fail(f"Constraint violation: {non_positive} rows with amount <= 0")

    # Constraint validation: all currencies in (USD, EUR, GBP)
    valid_currencies = {"USD", "EUR", "GBP"}
    all_currencies = set(table.column("currency").to_pylist())
    if all_currencies.issubset(valid_currencies):
        ok(f"Constraint: all currencies in {valid_currencies}")
    else:
        fail(f"Constraint violation: found currencies {all_currencies - valid_currencies}")

    # Per-account summary
    for acct, exp_count, exp_amount in [("ACC-1001", 6, 14000.00),
                                         ("ACC-1002", 5, 19000.00),
                                         ("ACC-1003", 5, 10400.00),
                                         ("ACC-1004", 5, 24000.00),
                                         ("ACC-1005", 4, 4500.00)]:
        mask = pc.equal(table.column("account_id"), acct)
        acct_table = table.filter(mask)
        if acct_table.num_rows == exp_count:
            ok(f"COUNT for {acct} = {exp_count}")
        else:
            fail(f"COUNT for {acct} = {acct_table.num_rows}, expected {exp_count}")
        acct_sum = round(pc.sum(acct_table.column("amount")).as_py(), 2)
        if acct_sum == exp_amount:
            ok(f"SUM(amount) for {acct} = {exp_amount}")
        else:
            fail(f"SUM(amount) for {acct} = {acct_sum}, expected {exp_amount}")

    # Per-currency summary
    for curr, exp_count, exp_amount in [("USD", 11, 38000.00),
                                         ("EUR", 9, 23500.00),
                                         ("GBP", 5, 10400.00)]:
        mask = pc.equal(table.column("currency"), curr)
        curr_table = table.filter(mask)
        if curr_table.num_rows == exp_count:
            ok(f"COUNT for {curr} = {exp_count}")
        else:
            fail(f"COUNT for {curr} = {curr_table.num_rows}, expected {exp_count}")
        curr_sum = round(pc.sum(curr_table.column("amount")).as_py(), 2)
        if curr_sum == exp_amount:
            ok(f"SUM(amount) for {curr} = {exp_amount}")
        else:
            fail(f"SUM(amount) for {curr} = {curr_sum}, expected {exp_amount}")

    # Per-type summary
    for txn_type, exp_count, exp_amount in [("deposit", 11, 50900.00),
                                              ("transfer", 7, 13500.00),
                                              ("withdrawal", 7, 7500.00)]:
        mask = pc.equal(table.column("txn_type"), txn_type)
        type_table = table.filter(mask)
        if type_table.num_rows == exp_count:
            ok(f"COUNT for {txn_type} = {exp_count}")
        else:
            fail(f"COUNT for {txn_type} = {type_table.num_rows}, expected {exp_count}")
        type_sum = round(pc.sum(type_table.column("amount")).as_py(), 2)
        if type_sum == exp_amount:
            ok(f"SUM(amount) for {txn_type} = {exp_amount}")
        else:
            fail(f"SUM(amount) for {txn_type} = {type_sum}, expected {exp_amount}")

    # Spot-check: txn_id=8 -> ACC-1004, deposit, 12000.00
    assert_value_where(table, "account_id", "ACC-1004", "txn_id", 8)
    assert_value_where(table, "amount", 12000.00, "txn_id", 8)

    # Latest balance per account (by max txn_date within each account)
    for acct, exp_balance in [("ACC-1001", 8000.00), ("ACC-1002", 4000.00),
                               ("ACC-1003", 5000.00), ("ACC-1004", 10000.00),
                               ("ACC-1005", 2900.00)]:
        mask = pc.equal(table.column("account_id"), acct)
        acct_table = table.filter(mask)
        # Find the row with max txn_date
        dates = acct_table.column("txn_date").to_pylist()
        max_date = max(dates)
        date_mask = pc.equal(acct_table.column("txn_date"), max_date)
        latest = acct_table.filter(date_mask)
        actual_balance = round(float(latest.column("balance_after")[0].as_py()), 2)
        if actual_balance == exp_balance:
            ok(f"Latest balance for {acct} = {exp_balance}")
        else:
            fail(f"Latest balance for {acct} = {actual_balance}, expected {exp_balance}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-constraints demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing transactions/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("CHECK Constraints with UniForm — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "transactions")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_transactions(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
