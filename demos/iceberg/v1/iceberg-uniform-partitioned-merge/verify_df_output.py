#!/usr/bin/env python3
"""
Iceberg UniForm Partitioned MERGE (Inventory Sync) — Data Verification
========================================================================
Reads the warehouse_inventory table through the Iceberg metadata chain and
verifies the final state after two MERGE operations (shipment + audit).

Final state: 36 SKUs (12 per warehouse), partitioned by warehouse.
  - MERGE 1 (shipment): +6 new SKUs, 18 quantity additions
  - MERGE 2 (audit): -6 discontinued SKUs, 6 quantity corrections
  - Total inventory value = 93761.00, total quantity = 13755

Usage:
    python verify.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (
    read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_distinct_count,
    assert_count_where, assert_value_where, assert_format_version,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status
from verify_lib.assertions import CYAN, RESET


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_warehouse_inventory(data_root, verbose=False):
    print_section("warehouse_inventory — Partitioned MERGE Final State")

    table_path = os.path.join(data_root, "warehouse_inventory")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    assert_format_version(metadata, 2)

    # Final: 36 seed + 6 new - 6 discontinued = 36
    assert_row_count(table, 36)

    # Grand totals
    print(f"\n  {CYAN}Grand totals:{RESET}")
    assert_distinct_count(table, "warehouse", 3)
    assert_sum(table, "quantity_on_hand", 13755)

    # Inventory value = SUM(quantity_on_hand * unit_cost) = 93761.00
    import pyarrow.compute as pc
    qty = table.column("quantity_on_hand")
    cost = table.column("unit_cost")
    product = pc.multiply(pc.cast(qty, "float64"), cost)
    total_value = round(pc.sum(product).as_py(), 2)
    if total_value == 93761.00:
        ok(f"Total inventory value = 93761.00")
    else:
        fail(f"Total inventory value = {total_value}, expected 93761.00")

    # Per-warehouse counts
    print(f"\n  {CYAN}Per-warehouse counts:{RESET}")
    assert_count_where(table, "warehouse", "charlotte", 12)
    assert_count_where(table, "warehouse", "dallas", 12)
    assert_count_where(table, "warehouse", "portland", 12)

    # Per-warehouse inventory values
    print(f"\n  {CYAN}Per-warehouse inventory values:{RESET}")
    for wh, expected_value in [("charlotte", 32321.25), ("dallas", 28811.90), ("portland", 32627.85)]:
        mask = pc.equal(table.column("warehouse"), wh)
        filtered = table.filter(mask)
        q = filtered.column("quantity_on_hand")
        c = filtered.column("unit_cost")
        p = pc.multiply(pc.cast(q, "float64"), c)
        actual = round(pc.sum(p).as_py(), 2)
        if actual == expected_value:
            ok(f"Inventory value WHERE warehouse={wh!r} = {expected_value}")
        else:
            fail(f"Inventory value WHERE warehouse={wh!r} = {actual}, expected {expected_value}")

    # Shipment quantity additions (MERGE 1)
    print(f"\n  {CYAN}Shipment quantity spot-checks (MERGE 1):{RESET}")
    assert_value_where(table, "quantity_on_hand", 750, "sku", "WH-P001")
    assert_value_where(table, "quantity_on_hand", 120, "sku", "WH-P003")
    assert_value_where(table, "quantity_on_hand", 650, "sku", "WH-D001")
    assert_value_where(table, "quantity_on_hand", 105, "sku", "WH-D003")
    assert_value_where(table, "quantity_on_hand", 700, "sku", "WH-C001")
    assert_value_where(table, "quantity_on_hand", 135, "sku", "WH-C003")

    # Audit corrections (MERGE 2)
    print(f"\n  {CYAN}Audit correction spot-checks (MERGE 2):{RESET}")
    assert_value_where(table, "quantity_on_hand", 1150, "sku", "WH-P002")
    assert_value_where(table, "quantity_on_hand", 960, "sku", "WH-D002")
    assert_value_where(table, "quantity_on_hand", 1060, "sku", "WH-C002")
    assert_value_where(table, "quantity_on_hand", 870, "sku", "WH-P006")
    assert_value_where(table, "quantity_on_hand", 780, "sku", "WH-D006")
    assert_value_where(table, "quantity_on_hand", 820, "sku", "WH-C006")

    # Discontinued SKUs removed (MERGE 2 DELETE)
    print(f"\n  {CYAN}Discontinued SKUs removed:{RESET}")
    for sku in ["WH-P012", "WH-D012", "WH-C012", "WH-P008", "WH-D008", "WH-C008"]:
        mask = pc.equal(table.column("sku"), sku)
        count = pc.sum(mask).as_py()
        if count == 0:
            ok(f"SKU {sku} correctly discontinued (removed)")
        else:
            fail(f"SKU {sku} still present ({count} rows), expected removed")

    # New SKUs present (MERGE 1 INSERT)
    print(f"\n  {CYAN}New SKUs from shipment (MERGE 1):{RESET}")
    assert_value_where(table, "product_name", "Epoxy Resin 1gal", "sku", "WH-P013")
    assert_value_where(table, "product_name", "Cable Tie 12\" (100pk)", "sku", "WH-P014")
    assert_value_where(table, "quantity_on_hand", 75, "sku", "WH-P013")
    assert_value_where(table, "quantity_on_hand", 60, "sku", "WH-D013")
    assert_value_where(table, "quantity_on_hand", 65, "sku", "WH-C013")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-partitioned-merge demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing warehouse_inventory/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Partitioned MERGE — Data Verification")
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
