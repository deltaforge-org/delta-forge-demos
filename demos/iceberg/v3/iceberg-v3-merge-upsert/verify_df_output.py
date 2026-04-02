#!/usr/bin/env python3
"""
Iceberg V3 UniForm Supply Chain Inventory MERGE Sync -- Data Verification
==========================================================================
Reads the warehouse_inventory table through Iceberg metadata after two
MERGE rounds. Final state: 33 items (30 seed + 3 inserted), with updated
quantities and prices from MERGE operations.

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


def verify_warehouse_inventory(data_root, verbose=False):
    import pyarrow as pa
    import pyarrow.compute as pc

    print_section("warehouse_inventory -- Post-MERGE Final State")

    table_path = os.path.join(data_root, "warehouse_inventory")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # 30 seed + 2 inserted (MERGE 1) + 1 inserted (MERGE 2) = 33
    assert_row_count(table, 33)

    # Grand totals
    assert_sum(table, "quantity", 2868)

    # Total inventory value = SUM(quantity * unit_price) = 202356.32
    qty = pc.cast(table.column("quantity"), pa.float64())
    price = table.column("unit_price")
    line_totals = pc.multiply(qty, price)
    total_value = round(pc.sum(line_totals).as_py(), 2)
    if total_value == 202356.32:
        ok(f"Total inventory value = 202356.32")
    else:
        fail(f"Total inventory value = {total_value}, expected 202356.32")

    # Distinct warehouses and SKUs
    assert_distinct_count(table, "warehouse", 3)
    assert_distinct_count(table, "sku", 12)

    # Per-warehouse counts and quantities
    assert_count_where(table, "warehouse", "WH-CENTRAL", 10)
    assert_count_where(table, "warehouse", "WH-EAST", 12)
    assert_count_where(table, "warehouse", "WH-WEST", 11)

    # Per-warehouse quantity sums
    for wh, expected_qty in [("WH-CENTRAL", 608), ("WH-EAST", 1122), ("WH-WEST", 1138)]:
        mask = pc.equal(table.column("warehouse"), wh)
        filtered = table.filter(mask)
        actual = pc.sum(filtered.column("quantity")).as_py()
        if actual == expected_qty:
            ok(f"SUM(quantity) WHERE warehouse={wh!r} = {expected_qty}")
        else:
            fail(f"SUM(quantity) WHERE warehouse={wh!r} = {actual}, expected {expected_qty}")

    # MERGE Round 1: Updated quantities in WH-EAST
    assert_value_where(table, "quantity", 175, "item_id", 1)   # was 150
    assert_value_where(table, "quantity", 30, "item_id", 3)    # was 25
    assert_value_where(table, "quantity", 110, "item_id", 7)   # was 90
    assert_value_where(table, "unit_price", 24.99, "item_id", 1)  # price unchanged

    # MERGE Round 1: New inserts
    assert_value_where(table, "product_name", "Wireless Earbuds", "item_id", 31)
    assert_value_where(table, "quantity", 200, "item_id", 31)
    assert_value_where(table, "unit_price", 49.99, "item_id", 31)
    assert_value_where(table, "warehouse", "WH-EAST", "item_id", 31)

    assert_value_where(table, "product_name", "Desk Lamp LED", "item_id", 32)
    assert_value_where(table, "quantity", 85, "item_id", 32)

    # MERGE Round 2: Price updates in WH-WEST
    assert_value_where(table, "unit_price", 26.99, "item_id", 11)   # was 24.99
    assert_value_where(table, "unit_price", 84.99, "item_id", 15)   # was 79.99
    assert_value_where(table, "unit_price", 64.99, "item_id", 19)   # was 59.99
    assert_value_where(table, "quantity", 180, "item_id", 11)       # quantity unchanged

    # MERGE Round 2: New insert
    assert_value_where(table, "product_name", "Wireless Earbuds", "item_id", 33)
    assert_value_where(table, "warehouse", "WH-WEST", "item_id", 33)
    assert_value_where(table, "quantity", 160, "item_id", 33)

    # Untouched WH-CENTRAL row
    assert_value_where(table, "warehouse", "WH-CENTRAL", "item_id", 21)
    assert_value_where(table, "sku", "SKU-1001", "item_id", 21)
    assert_value_where(table, "quantity", 100, "item_id", 21)
    assert_value_where(table, "unit_price", 24.99, "item_id", 21)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-merge-upsert demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing warehouse_inventory/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 MERGE Upsert -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "warehouse_inventory")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_warehouse_inventory(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
