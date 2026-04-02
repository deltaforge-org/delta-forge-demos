#!/usr/bin/env python3
"""
Iceberg UniForm Partitioned — Data Verification
==================================================
Reads the regional_sales table purely through the Iceberg metadata chain
and verifies the final state after partitioned DML operations:
  - 24 transactions seeded across 3 regions (V1)
  - Q4 amounts +5% bonus (V2)
  - DELETE eu-west where amount < 700 (V3) — removes id=20 (680.00)
  - INSERT 3 new Q1-2025 transactions (V4)

Final state: 26 transactions, 3 regions (us-east=9, us-west=9, eu-west=8).

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing regional_sales/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_distinct_count,
    assert_count_where, assert_value_where, assert_format_version,
    print_header, print_section, print_summary, exit_with_status)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_regional_sales(data_root, verbose=False):
    print_section("regional_sales — Partitioned Table")

    table_path = os.path.join(data_root, "regional_sales")
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

    # Final state: 26 transactions
    assert_row_count(table, 26)

    # 3 regions
    assert_distinct_count(table, "region", 3)

    # Per-region counts
    assert_count_where(table, "region", "us-east", 9)
    assert_count_where(table, "region", "us-west", 9)
    assert_count_where(table, "region", "eu-west", 8)

    # Total revenue = 31121.50
    assert_sum(table, "amount", 31121.50)

    # Q4 bonus total = 6751.50
    q4_mask = pc.equal(table.column("quarter"), "Q4-2024")
    q4 = table.filter(q4_mask)
    q4_total = round(pc.sum(q4.column("amount")).as_py(), 2)
    if q4_total == 6751.50:
        ok(f"Q4-2024 bonus total = 6751.50")
    else:
        fail(f"Q4-2024 bonus total = {q4_total}, expected 6751.50")

    # Q1-2025 total = 5500.00
    q1_25_mask = pc.equal(table.column("quarter"), "Q1-2025")
    q1_25 = table.filter(q1_25_mask)
    q1_25_total = round(pc.sum(q1_25.column("amount")).as_py(), 2)
    if q1_25_total == 5500.00:
        ok(f"Q1-2025 total = 5500.00")
    else:
        fail(f"Q1-2025 total = {q1_25_total}, expected 5500.00")

    # Spot-check Q4 bonus amounts
    assert_value_where(table, "amount", 1890.00, "id", 7, "Q4 bonus id=7")
    assert_value_where(table, "amount", 535.50, "id", 8, "Q4 bonus id=8")
    assert_value_where(table, "amount", 630.00, "id", 15, "Q4 bonus id=15")
    assert_value_where(table, "amount", 1606.50, "id", 16, "Q4 bonus id=16")
    assert_value_where(table, "amount", 840.00, "id", 23, "Q4 bonus id=23")
    assert_value_where(table, "amount", 1249.50, "id", 24, "Q4 bonus id=24")

    # Deleted row (id=20, eu-west, Gadget Max, Q2-2024, 680.00 < 700) should be gone
    mask_20 = pc.equal(table.column("id"), 20)
    count_20 = pc.sum(mask_20).as_py()
    if count_20 == 0:
        ok(f"Deleted row id=20 (eu-west, amount=680) not present")
    else:
        fail(f"Deleted row id=20 still present ({count_20} rows)")

    # New Q1-2025 rows exist
    assert_value_where(table, "amount", 2100.00, "id", 25, "new Q1-2025 us-east")
    assert_value_where(table, "amount", 1800.00, "id", 26, "new Q1-2025 us-west")
    assert_value_where(table, "amount", 1600.00, "id", 27, "new Q1-2025 eu-west")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-partitioned demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing regional_sales/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Partitioned — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "regional_sales")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_regional_sales(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
