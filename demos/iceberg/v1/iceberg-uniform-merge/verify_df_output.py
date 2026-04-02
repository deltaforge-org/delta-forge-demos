#!/usr/bin/env python3
"""
Iceberg UniForm MERGE INTO (CDC Upsert) — Data Verification
==============================================================
Reads the order_fulfillment table purely through the Iceberg metadata chain
and verifies the final state after two MERGE operations:
  - 30 orders seeded (V1)
  - MERGE 1: 10 status updates + 5 new orders (V2) -> 35 rows
  - MERGE 2: 3 deletes (ids 3,6,9) + 3 status updates + 4 new orders (V3) -> 36 rows

Final state: 36 orders, statuses: 11 pending, 16 shipped, 9 delivered.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing order_fulfillment/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_distinct_count,
    assert_count_where, assert_value_where, assert_format_version,
    print_header, print_section, print_summary, exit_with_status)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_order_fulfillment(data_root, verbose=False):
    print_section("order_fulfillment — MERGE INTO (CDC Upsert)")

    table_path = os.path.join(data_root, "order_fulfillment")
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

    import pyarrow.compute as pc

    # Final state: 36 orders (30 - 3 deleted + 5 from merge1 + 4 from merge2)
    assert_row_count(table, 36)

    # 3 regions
    assert_distinct_count(table, "region", 3)

    # Per-region counts
    assert_count_where(table, "region", "eu-west", 13)
    assert_count_where(table, "region", "us-east", 10)
    assert_count_where(table, "region", "us-west", 13)

    # Per-status counts
    assert_count_where(table, "status", "pending", 11)
    assert_count_where(table, "status", "shipped", 16)
    assert_count_where(table, "status", "delivered", 9)

    # Total revenue = SUM(quantity * unit_price) = 6857.16
    revenue = round(sum(
        float(table.column("quantity")[i].as_py()) * float(table.column("unit_price")[i].as_py())
        for i in range(table.num_rows)
    ), 2)
    if revenue == 6857.16:
        ok(f"Total revenue = 6857.16")
    else:
        fail(f"Total revenue = {revenue}, expected 6857.16")

    # Per-region revenue
    for region, expected_rev in [
        ("eu-west", 2905.90),
        ("us-east", 1553.91),
        ("us-west", 2397.35),
    ]:
        mask = pc.equal(table.column("region"), region)
        filtered = table.filter(mask)
        rev = round(sum(
            float(filtered.column("quantity")[i].as_py()) * float(filtered.column("unit_price")[i].as_py())
            for i in range(filtered.num_rows)
        ), 2)
        if rev == expected_rev:
            ok(f"Revenue for {region} = {expected_rev}")
        else:
            fail(f"Revenue for {region} = {rev}, expected {expected_rev}")

    # Deleted orders (3, 6, 9) should not exist
    for order_id in [3, 6, 9]:
        mask = pc.equal(table.column("order_id"), order_id)
        count = pc.sum(mask).as_py()
        if count == 0:
            ok(f"Deleted order_id={order_id} not present")
        else:
            fail(f"Deleted order_id={order_id} still present ({count} rows)")

    # Status updates from MERGE 1
    assert_value_where(table, "status", "shipped", "order_id", 1, "MERGE1 pending->shipped")
    assert_value_where(table, "status", "shipped", "order_id", 4, "MERGE1 pending->shipped")
    assert_value_where(table, "status", "shipped", "order_id", 7, "MERGE1 pending->shipped")
    assert_value_where(table, "status", "shipped", "order_id", 10, "MERGE1 pending->shipped")
    assert_value_where(table, "status", "delivered", "order_id", 2, "MERGE1 shipped->delivered")
    assert_value_where(table, "status", "delivered", "order_id", 5, "MERGE1 shipped->delivered")
    assert_value_where(table, "status", "delivered", "order_id", 8, "MERGE1 shipped->delivered")
    assert_value_where(table, "status", "shipped", "order_id", 11, "MERGE1 pending->shipped")
    assert_value_where(table, "status", "shipped", "order_id", 14, "MERGE1 pending->shipped")
    assert_value_where(table, "status", "shipped", "order_id", 17, "MERGE1 pending->shipped")

    # Status updates from MERGE 2
    assert_value_where(table, "status", "shipped", "order_id", 21, "MERGE2 pending->shipped")
    assert_value_where(table, "status", "shipped", "order_id", 24, "MERGE2 pending->shipped")
    assert_value_where(table, "status", "shipped", "order_id", 27, "MERGE2 pending->shipped")

    # New orders from MERGE 1
    assert_value_where(table, "status", "pending", "order_id", 31, "MERGE1 new order")
    assert_value_where(table, "status", "pending", "order_id", 32, "MERGE1 new order")
    assert_value_where(table, "status", "pending", "order_id", 33, "MERGE1 new order")
    assert_value_where(table, "status", "pending", "order_id", 34, "MERGE1 new order")
    assert_value_where(table, "status", "pending", "order_id", 35, "MERGE1 new order")

    # New orders from MERGE 2
    assert_value_where(table, "status", "pending", "order_id", 36, "MERGE2 new order")
    assert_value_where(table, "status", "pending", "order_id", 37, "MERGE2 new order")
    assert_value_where(table, "status", "pending", "order_id", 38, "MERGE2 new order")
    assert_value_where(table, "status", "pending", "order_id", 39, "MERGE2 new order")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-merge demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing order_fulfillment/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm MERGE INTO — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "order_fulfillment")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_order_fulfillment(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
