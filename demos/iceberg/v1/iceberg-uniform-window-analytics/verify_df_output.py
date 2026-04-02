#!/usr/bin/env python3
"""
Regional Sales Performance — Window Analytics with UniForm — Iceberg Data Verification
========================================================================================
Reads the sales table purely through the Iceberg metadata chain and verifies
the final state (no DML mutations — seed data only):
  - 40 sales across 7 reps, 4 regions, 3 product categories
  - Window function results verified: ROW_NUMBER, RANK, running totals, LAG/LEAD

Final state: 40 rows, total revenue 185300.00, avg sale 4632.50.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing sales/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (
    read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_avg, assert_min, assert_max,
    assert_distinct_count, assert_count_where, assert_value_where,
    assert_format_version,
    print_header, print_section, print_summary, exit_with_status,
)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_sales(data_root, verbose=False):
    print_section("sales — Window Analytics with UniForm")

    table_path = os.path.join(data_root, "sales")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    import pyarrow.compute as pc

    # Format version
    assert_format_version(metadata, 1)

    # Final state: 40 rows (seed data only, no DML mutations)
    assert_row_count(table, 40)

    # 7 distinct reps
    assert_distinct_count(table, "rep_name", 7)

    # 4 distinct regions
    assert_distinct_count(table, "region", 4)

    # 3 distinct product categories
    assert_distinct_count(table, "product_category", 3)

    # Total revenue = 185300.00
    assert_sum(table, "sale_amount", 185300.00)

    # Average sale = 4632.50
    assert_avg(table, "sale_amount", 4632.50)

    # Max sale = 9400.00, Min sale = 1200.00
    assert_max(table, "sale_amount", 9400.00)
    assert_min(table, "sale_amount", 1200.00)

    # Region totals
    for region, exp_total in [("Northeast", 61700.00), ("West", 56800.00),
                               ("Southeast", 46700.00), ("Midwest", 20100.00)]:
        mask = pc.equal(table.column("region"), region)
        region_table = table.filter(mask)
        region_sum = round(pc.sum(region_table.column("sale_amount")).as_py(), 2)
        if region_sum == exp_total:
            ok(f"SUM(sale_amount) for {region} = {exp_total}")
        else:
            fail(f"SUM(sale_amount) for {region} = {region_sum}, expected {exp_total}")

    # Category totals
    for cat, exp_total in [("Electronics", 98200.00), ("Furniture", 62300.00),
                            ("Clothing", 24800.00)]:
        mask = pc.equal(table.column("product_category"), cat)
        cat_table = table.filter(mask)
        cat_sum = round(pc.sum(cat_table.column("sale_amount")).as_py(), 2)
        if cat_sum == exp_total:
            ok(f"SUM(sale_amount) for {cat} = {exp_total}")
        else:
            fail(f"SUM(sale_amount) for {cat} = {cat_sum}, expected {exp_total}")

    # Top sale per rep (ROW_NUMBER verification)
    # Find each rep's highest sale_amount
    top_sales = {
        "Emma Clark": 8900.00,
        "Olivia Kim": 9200.00,
        "Sophia Grant": 9400.00,
        "Liam Foster": 7800.00,
        "James Lee": 5500.00,
        "Ava Moore": 8200.00,
        "Noah Hayes": 5200.00,
    }
    for rep, exp_max in top_sales.items():
        mask = pc.equal(table.column("rep_name"), rep)
        rep_table = table.filter(mask)
        rep_max = round(float(pc.max(rep_table.column("sale_amount")).as_py()), 2)
        if rep_max == exp_max:
            ok(f"MAX(sale_amount) for {rep} = {exp_max}")
        else:
            fail(f"MAX(sale_amount) for {rep} = {rep_max}, expected {exp_max}")

    # Rep total revenues (RANK verification)
    rep_totals = {
        "Olivia Kim": 33100.00,
        "Sophia Grant": 32700.00,
        "Emma Clark": 28600.00,
        "Liam Foster": 24700.00,
        "Ava Moore": 24100.00,
        "James Lee": 22000.00,
        "Noah Hayes": 20100.00,
    }
    for rep, exp_total in rep_totals.items():
        mask = pc.equal(table.column("rep_name"), rep)
        rep_table = table.filter(mask)
        rep_sum = round(pc.sum(rep_table.column("sale_amount")).as_py(), 2)
        if rep_sum == exp_total:
            ok(f"Total revenue for {rep} = {exp_total}")
        else:
            fail(f"Total revenue for {rep} = {rep_sum}, expected {exp_total}")

    # Emma Clark's sales in order (LAG/LEAD verification)
    # sale_ids: 1, 6, 13, 20, 27, 34
    emma_mask = pc.equal(table.column("rep_name"), "Emma Clark")
    emma_table = table.filter(emma_mask)
    emma_count = emma_table.num_rows
    if emma_count == 6:
        ok(f"Emma Clark has 6 sales")
    else:
        fail(f"Emma Clark has {emma_count} sales, expected 6")

    # Verify specific Emma Clark sales
    assert_value_where(table, "sale_amount", 4500.00, "sale_id", 1)
    assert_value_where(table, "sale_amount", 2100.00, "sale_id", 6)
    assert_value_where(table, "sale_amount", 8900.00, "sale_id", 13)
    assert_value_where(table, "sale_amount", 3700.00, "sale_id", 20)
    assert_value_where(table, "sale_amount", 1900.00, "sale_id", 27)
    assert_value_where(table, "sale_amount", 7500.00, "sale_id", 34)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-window-analytics demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing sales/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Window Analytics with UniForm — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "sales")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_sales(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
