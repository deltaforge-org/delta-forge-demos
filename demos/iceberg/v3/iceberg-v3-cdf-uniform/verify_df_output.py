#!/usr/bin/env python3
"""
Iceberg V3 UniForm CDF Payment Reconciliation -- Data Verification
====================================================================
Reads the payment_transactions table through Iceberg metadata after four
mutation rounds (approve, decline, fraud delete, new insert).
Final state: 33 rows (30 seed - 2 deleted + 5 inserted).

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
    assert_row_count, assert_sum, assert_count_where, assert_distinct_count,
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


def verify_payment_transactions(data_root, verbose=False):
    import pyarrow as pa
    import pyarrow.compute as pc

    print_section("payment_transactions -- CDF+UniForm Final State")

    table_path = os.path.join(data_root, "payment_transactions")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # 30 seed - 2 deleted(18,24) + 5 inserted(31-35) = 33
    assert_row_count(table, 33)

    # Status distribution after all mutations:
    # 8 original completed + 8 approved = 16 completed
    # 20 - 8 approved - 3 declined + 5 new = 14 pending
    # 2 original failed + 3 declined - 2 deleted(18,24 were failed) = 3 failed
    assert_count_where(table, "status", "completed", 16)
    assert_count_where(table, "status", "pending", 14)
    assert_count_where(table, "status", "failed", 3)

    # Deleted rows absent
    deleted_ids = pa.array([18, 24])
    assert_count_where_in(table, "payment_id", deleted_ids, 0, "fraud rows deleted")

    # Total amount = 19135.83
    assert_sum(table, "amount", 19135.83)

    # Merchant count = 5
    assert_distinct_count(table, "merchant", 5)

    # Per-merchant counts
    assert_count_where(table, "merchant", "TechGadgets Inc", 7)
    assert_count_where(table, "merchant", "CloudSoft SaaS", 6)

    # Spot-check new inserts
    assert_value_where(table, "merchant", "TechGadgets Inc", "payment_id", 31)
    assert_value_where(table, "amount", 4999.99, "payment_id", 31)
    assert_value_where(table, "status", "pending", "payment_id", 31)

    # Approved payments verify (status changed from pending to completed)
    for pid in [1, 3, 7, 9, 14, 19, 25, 27]:
        assert_value_where(table, "status", "completed", "payment_id", pid)

    # Declined payments (status changed from pending to failed)
    for pid in [6, 22, 28]:
        assert_value_where(table, "status", "failed", "payment_id", pid)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-cdf-uniform demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing payment_transactions/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 CDF UniForm -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "payment_transactions")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_payment_transactions(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
