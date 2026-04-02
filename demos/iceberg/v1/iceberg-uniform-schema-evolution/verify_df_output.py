#!/usr/bin/env python3
"""
Iceberg UniForm Schema Evolution — Data Verification
======================================================
Reads the customer_orders table purely through the Iceberg metadata chain
and verifies the final state after schema evolution:
  - 20 orders seeded with 6 columns (V1)
  - ADD COLUMN loyalty_tier (V2)
  - Backfill loyalty_tier (V3)
  - ADD COLUMN discount_pct, notes (V4-V5)
  - Populate discount_pct and notes (V6)
  - Insert 4 new orders with full 9-column schema (V7)

Final state: 24 orders, 9 columns, all populated.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing customer_orders/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_distinct_count,
    assert_count_where, assert_format_version,
    print_header, print_section, print_summary, exit_with_status)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_customer_orders(data_root, verbose=False):
    print_section("customer_orders — Schema Evolution")

    table_path = os.path.join(data_root, "customer_orders")
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

    # Final state: 24 orders (20 original + 4 post-evolution inserts)
    assert_row_count(table, 24)

    # All 24 rows should have loyalty_tier, discount_pct, notes populated
    import pyarrow.compute as pc

    tier_count = pc.sum(pc.is_valid(table.column("loyalty_tier"))).as_py()
    if tier_count == 24:
        ok(f"All 24 rows have loyalty_tier populated")
    else:
        fail(f"loyalty_tier populated count = {tier_count}, expected 24")

    discount_count = pc.sum(pc.is_valid(table.column("discount_pct"))).as_py()
    if discount_count == 24:
        ok(f"All 24 rows have discount_pct populated")
    else:
        fail(f"discount_pct populated count = {discount_count}, expected 24")

    notes_count = pc.sum(pc.is_valid(table.column("notes"))).as_py()
    if notes_count == 24:
        ok(f"All 24 rows have notes populated")
    else:
        fail(f"notes populated count = {notes_count}, expected 24")

    # Distinct tiers = 3 (Platinum, Gold, Silver)
    assert_distinct_count(table, "loyalty_tier", 3)

    # Platinum orders = 12 (DataFlow LLC 5+1=6, Global Foods 5+1=6)
    assert_count_where(table, "loyalty_tier", "Platinum", 12)
    assert_count_where(table, "loyalty_tier", "Gold", 6)  # TechStart Inc 5+1
    assert_count_where(table, "loyalty_tier", "Silver", 6)  # Acme Corp 5+1

    # Gross revenue = SUM(quantity * unit_price) = 13445.00
    gross = round(sum(
        float(table.column("quantity")[i].as_py()) * float(table.column("unit_price")[i].as_py())
        for i in range(table.num_rows)
    ), 2)
    if gross == 13445.00:
        ok(f"Gross revenue = 13445.00")
    else:
        fail(f"Gross revenue = {gross}, expected 13445.00")

    # Discounted revenue = SUM(quantity * unit_price * (1 - discount_pct/100)) = 12296.63
    discounted = round(sum(
        float(table.column("quantity")[i].as_py()) *
        float(table.column("unit_price")[i].as_py()) *
        (1 - float(table.column("discount_pct")[i].as_py()) / 100)
        for i in range(table.num_rows)
    ), 2)
    if discounted == 12296.63:
        ok(f"Discounted revenue = 12296.63")
    else:
        fail(f"Discounted revenue = {discounted}, expected 12296.63")

    # Per-customer discounted revenue
    for cust, expected_dr in [
        ("Acme Corp", 1363.25),
        ("TechStart Inc", 2104.38),
        ("Global Foods", 2407.50),
        ("DataFlow LLC", 2439.00),
    ]:
        mask = pc.equal(table.column("customer_name"), cust)
        filtered = table.filter(mask)
        dr = round(sum(
            float(filtered.column("quantity")[i].as_py()) *
            float(filtered.column("unit_price")[i].as_py()) *
            (1 - float(filtered.column("discount_pct")[i].as_py()) / 100)
            for i in range(filtered.num_rows)
        ), 2)
        if dr == expected_dr:
            ok(f"Discounted revenue for {cust} = {expected_dr}")
        else:
            fail(f"Discounted revenue for {cust} = {dr}, expected {expected_dr}")

    # Schema should have 9 columns
    if len(table.column_names) >= 9:
        ok(f"Schema has {len(table.column_names)} columns (expected >= 9)")
    else:
        fail(f"Schema has {len(table.column_names)} columns, expected >= 9")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-schema-evolution demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing customer_orders/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Schema Evolution — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "customer_orders")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_customer_orders(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
