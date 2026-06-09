#!/usr/bin/env python3
"""
Iceberg V1 Warehouse Inventory -- Data Verification
=====================================================
Reads the warehouse_inventory table through the Iceberg V1 metadata chain
and verifies the final state: 489 SKUs across 3 warehouses and 5 categories.

Usage:
    python verify_df_output.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_avg, assert_distinct_count,
    assert_count_where, assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_warehouse_inventory(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("warehouse_inventory -- Iceberg V1")

    table_path = os.path.join(data_root, "warehouse_inventory")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 1)
    assert_row_count(table, 489)

    # Per-warehouse counts
    assert_count_where(table, "warehouse", "Charlotte-NC", 159)
    assert_count_where(table, "warehouse", "Dallas-TX", 166)
    assert_count_where(table, "warehouse", "Portland-OR", 164)

    # Per-category counts
    assert_count_where(table, "category", "Apparel", 100)
    assert_count_where(table, "category", "Electronics", 99)
    assert_count_where(table, "category", "Food-Bev", 97)
    assert_count_where(table, "category", "Furniture", 94)
    assert_count_where(table, "category", "Industrial", 99)

    assert_distinct_count(table, "warehouse", 3)
    assert_distinct_count(table, "category", 5)
    assert_distinct_count(table, "supplier", 5)

    # Supplier distribution
    assert_count_where(table, "supplier", "Acme Corp", 88)
    assert_count_where(table, "supplier", "EcoSupply", 101)
    assert_count_where(table, "supplier", "GlobalTrade", 82)
    assert_count_where(table, "supplier", "PrimeParts", 108)
    assert_count_where(table, "supplier", "QuickShip", 110)

    # Total inventory value: SUM(quantity_on_hand * unit_cost) = 17554271.58
    qty = table.column("quantity_on_hand")
    cost = table.column("unit_cost")
    total_value = round(sum(
        float(qty[i].as_py()) * float(cost[i].as_py())
        for i in range(table.num_rows)
    ), 2)
    if total_value == 17554271.58:
        ok(f"Total inventory value = 17554271.58")
    else:
        fail(f"Total inventory value = {total_value}, expected 17554271.58")

    # Items below reorder point
    below_reorder = sum(
        1 for i in range(table.num_rows)
        if table.column("quantity_on_hand")[i].as_py() < table.column("reorder_point")[i].as_py()
    )
    if below_reorder == 56:
        ok(f"Items below reorder point = 56")
    else:
        fail(f"Items below reorder point = {below_reorder}, expected 56")

    # Average unit cost by category
    for cat, expected in [("Apparel", 137.18), ("Electronics", 148.05),
                          ("Food-Bev", 144.74), ("Furniture", 137.43),
                          ("Industrial", 150.28)]:
        mask = pc.equal(table.column("category"), cat)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("unit_cost")).as_py(), 2)
        if actual == expected:
            ok(f"AVG(unit_cost) WHERE category={cat!r} = {expected}")
        else:
            fail(f"AVG(unit_cost) WHERE category={cat!r} = {actual}, expected {expected}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v1-warehouse-inventory demo"
    )
    parser.add_argument("data_root", help="Root path containing warehouse_inventory/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V1 Warehouse Inventory -- Data Verification")
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
