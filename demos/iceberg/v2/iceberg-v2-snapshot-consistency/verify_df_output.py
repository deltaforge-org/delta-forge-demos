#!/usr/bin/env python3
"""
Iceberg v2 Snapshot Consistency -- Inventory -- Data Verification
==================================================================
Reads the inventory table through Iceberg metadata and verifies 90 rows
representing the final state after 4 snapshots: load 80, insert 20,
update electronics +8%, delete 10 discontinued.

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
    assert_row_count, assert_sum, assert_avg,
    assert_distinct_count, assert_count_where,
    assert_format_version,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_inventory(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("inventory -- Snapshot Consistency (4 snapshots)")

    table_path = os.path.join(data_root, "inventory")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 90)

    # Category distribution
    assert_count_where(table, "category", "Clothing", 22)
    assert_count_where(table, "category", "Electronics", 23)
    assert_count_where(table, "category", "Home & Garden", 22)
    assert_count_where(table, "category", "Sports", 23)

    # Category quantities
    for cat, expected_qty in [
        ("Clothing", 2685),
        ("Electronics", 2104),
        ("Home & Garden", 2514),
        ("Sports", 2729),
    ]:
        mask = pc.equal(table.column("category"), cat)
        filtered = table.filter(mask)
        actual = round(pc.sum(filtered.column("quantity_on_hand")).as_py(), 2)
        if actual == expected_qty:
            ok(f"Total quantity for {cat} = {expected_qty}")
        else:
            fail(f"Total quantity for {cat} = {actual}, expected {expected_qty}")

    # Electronics avg price after +8% update
    elec_mask = pc.equal(table.column("category"), "Electronics")
    elec = table.filter(elec_mask)
    elec_avg = round(pc.mean(elec.column("unit_price")).as_py(), 2)
    if elec_avg == 50.05:
        ok(f"Electronics avg_price = 50.05 (after +8% update)")
    else:
        fail(f"Electronics avg_price = {elec_avg}, expected 50.05")

    # New products (SKU like 'SKU-%-N%')
    sku_col = table.column("sku")
    new_count = 0
    for i in range(table.num_rows):
        sku = sku_col[i].as_py()
        if sku and "-N" in sku:
            new_count += 1
    if new_count == 20:
        ok(f"New product count (SKU contains '-N') = 20")
    else:
        fail(f"New product count = {new_count}, expected 20")

    # Discontinued SKUs should be deleted (0 remaining)
    discontinued_skus = [
        "SKU-C-D01", "SKU-C-D02", "SKU-C-D03",
        "SKU-E-D01", "SKU-E-D02", "SKU-E-D03",
        "SKU-H-D01", "SKU-H-D02",
        "SKU-S-D01", "SKU-S-D02",
    ]
    disc_count = 0
    for i in range(table.num_rows):
        sku = sku_col[i].as_py()
        if sku in discontinued_skus:
            disc_count += 1
    if disc_count == 0:
        ok(f"Discontinued SKUs remaining = 0 (all 10 deleted)")
    else:
        fail(f"Discontinued SKUs remaining = {disc_count}, expected 0")

    # Overall totals
    assert_sum(table, "unit_price", 2976.3, label="total_unit_price")

    # total_inventory_value = sum(unit_price * quantity)
    total_value = 0.0
    price_col = table.column("unit_price")
    qty_col = table.column("quantity_on_hand")
    for i in range(table.num_rows):
        total_value += price_col[i].as_py() * qty_col[i].as_py()
    total_value = round(total_value, 2)
    if total_value == 300102.64:
        ok(f"Total inventory value = 300102.64")
    else:
        fail(f"Total inventory value = {total_value}, expected 300102.64")

    assert_avg(table, "unit_price", 33.07, label="avg_price")
    assert_sum(table, "quantity_on_hand", 10032.0, label="total_quantity")

    # Spot checks
    # SKU-C-N01: Compression Shorts, 24.99
    cn01 = table.filter(pc.equal(table.column("sku"), "SKU-C-N01"))
    if cn01.num_rows == 1:
        name = cn01.column("product_name")[0].as_py()
        price = round(cn01.column("unit_price")[0].as_py(), 2)
        if name == "Compression Shorts":
            ok(f"SKU-C-N01 product_name = 'Compression Shorts'")
        else:
            fail(f"SKU-C-N01 product_name = {name!r}, expected 'Compression Shorts'")
        if price == 24.99:
            ok(f"SKU-C-N01 unit_price = 24.99")
        else:
            fail(f"SKU-C-N01 unit_price = {price}, expected 24.99")
    else:
        fail(f"SKU-C-N01 not found (expected 1 row, got {cn01.num_rows})")

    # SKU-E-N01: USB-C Cable 3ft, 9.71
    en01 = table.filter(pc.equal(table.column("sku"), "SKU-E-N01"))
    if en01.num_rows == 1:
        name = en01.column("product_name")[0].as_py()
        price = round(en01.column("unit_price")[0].as_py(), 2)
        if name == "USB-C Cable 3ft":
            ok(f"SKU-E-N01 product_name = 'USB-C Cable 3ft'")
        else:
            fail(f"SKU-E-N01 product_name = {name!r}, expected 'USB-C Cable 3ft'")
        if price == 9.71:
            ok(f"SKU-E-N01 unit_price = 9.71")
        else:
            fail(f"SKU-E-N01 unit_price = {price}, expected 9.71")
    else:
        fail(f"SKU-E-N01 not found (expected 1 row, got {en01.num_rows})")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-snapshot-consistency demo"
    )
    parser.add_argument("data_root", help="Root path containing inventory/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg v2 Snapshot Consistency -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "inventory")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_inventory(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
