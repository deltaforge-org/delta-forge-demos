#!/usr/bin/env python3
"""
Iceberg UniForm Equality Deletes -- Data Verification
======================================================
Reads the eq_del_products table through the Iceberg metadata chain
(written via UniForm) and verifies that 3 products were deleted,
leaving 7 of the original 10 rows.

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
    assert_row_count, assert_sum,
    assert_count_where, assert_value_where,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_eq_del_products(data_root, verbose=False):
    import pyarrow as pa
    import pyarrow.compute as pc

    print_section("eq_del_products -- UniForm Equality Deletes")

    table_path = os.path.join(data_root, "eq_del_products")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # 10 seeded, 3 deleted (id IN 2, 5, 8), leaving 7
    assert_row_count(table, 7)

    # Total price of remaining products
    assert_sum(table, "price", 2813.48, label="total_price")

    # Category distribution
    assert_count_where(table, "category", "Electronics", 2)
    assert_count_where(table, "category", "Energy", 2)
    assert_count_where(table, "category", "Industrial", 2)
    assert_count_where(table, "category", "Science", 1)

    # Spot checks
    assert_value_where(table, "id", 1, "name", "Quantum Widget")
    assert_value_where(table, "id", 1, "price", 299.99)
    assert_value_where(table, "id", 3, "name", "Bio Reactor Kit")
    assert_value_where(table, "id", 3, "price", 599.0)
    assert_value_where(table, "id", 10, "name", "Plasma Cutter Pro")
    assert_value_where(table, "id", 10, "price", 399.99)

    # Deleted ids must be absent
    for deleted_id in [2, 5, 8]:
        mask = pc.is_in(table.column("id"), value_set=pa.array([deleted_id]))
        filtered = table.filter(mask)
        if filtered.num_rows == 0:
            ok(f"Deleted id={deleted_id} is absent")
        else:
            fail(f"Deleted id={deleted_id} still present ({filtered.num_rows} rows)")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-equality-deletes demo"
    )
    parser.add_argument("data_root", help="Root path containing eq_del_products/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("UniForm Equality Deletes -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "eq_del_products")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_eq_del_products(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
