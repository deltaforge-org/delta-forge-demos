#!/usr/bin/env python3
"""
Delta Partition-Scoped DELETE — Iceberg Data Verification
==========================================================
Reads the warehouse_orders table through Iceberg metadata after three
DELETE operations (partition-scoped cancelled, cross-partition returned,
conditional pending) and verifies 33 rows remain with correct distributions.

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
    assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def assert_count_where_in(table, filter_col, filter_vals, expected, label=""):
    import pyarrow.compute as pc
    mask = pc.is_in(table.column(filter_col), value_set=filter_vals)
    actual = pc.sum(mask).as_py()
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"COUNT WHERE {filter_col} IN {filter_vals} = {expected}{ctx}")
    else:
        fail(f"COUNT WHERE {filter_col} IN {filter_vals} = {actual}, expected {expected}{ctx}")


def verify_warehouse_orders(data_root, verbose=False):
    import pyarrow as pa
    import pyarrow.compute as pc

    print_section("warehouse_orders -- Post-DELETE Final State")

    table_path = os.path.join(data_root, "warehouse_orders")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # Total rows: 45 - 3 cancelled(us-west) - 6 returned(all) - 3 pending(us-east) = 33
    assert_row_count(table, 33)

    # Per-region counts
    assert_count_where(table, "region", "us-west", 10)
    assert_count_where(table, "region", "us-central", 13)
    assert_count_where(table, "region", "us-east", 10)

    # Status distribution
    assert_count_where(table, "status", "fulfilled", 18)
    assert_count_where(table, "status", "pending", 9)
    assert_count_where(table, "status", "cancelled", 6)
    assert_count_where(table, "status", "returned", 0)

    # Deleted IDs should be absent
    deleted_ids = pa.array([4, 8, 13, 6, 10, 21, 24, 36, 39, 38, 43, 45])
    assert_count_where_in(table, "id", deleted_ids, 0, "deleted rows absent")

    # Grand total value: SUM(quantity * unit_price) = 19696.33
    qty = pc.cast(table.column("quantity"), pa.float64())
    price = pc.cast(table.column("unit_price"), pa.float64())
    line_totals = pc.multiply(qty, price)
    total_value = round(pc.sum(line_totals).as_py(), 2)
    if total_value == 19696.33:
        ok(f"Total line value = 19696.33")
    else:
        fail(f"Total line value = {total_value}, expected 19696.33")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for delta-partition-delete demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing warehouse_orders/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Delta Partition-Scoped DELETE -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "warehouse_orders")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_warehouse_orders(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
