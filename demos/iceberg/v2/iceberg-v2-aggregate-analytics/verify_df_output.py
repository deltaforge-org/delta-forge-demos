#!/usr/bin/env python3
"""
Iceberg V2 Aggregate Analytics -- Data Verification
=====================================================
Reads the retail_sales table through the Iceberg metadata chain and
verifies 120 retail transactions with regional and category breakdowns.

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


def verify_retail_sales(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("retail_sales -- Aggregate Analytics")

    table_path = os.path.join(data_root, "retail_sales")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 120)

    # Dimension cardinalities
    assert_distinct_count(table, "region", 3)
    assert_distinct_count(table, "category", 5)
    assert_distinct_count(table, "store_name", 4)

    # Revenue and unit totals (computed from raw columns)
    # gross_revenue = SUM(quantity * unit_price)
    gross_values = [
        round(table.column("quantity")[i].as_py() * table.column("unit_price")[i].as_py(), 2)
        for i in range(table.num_rows)
    ]
    actual_gross = round(sum(gross_values), 2)
    if actual_gross == 25506.46:
        ok("gross_revenue = 25506.46")
    else:
        fail(f"gross_revenue = {actual_gross}, expected 25506.46")

    # net_revenue = SUM(quantity * unit_price * (1 - discount_pct/100))
    net_values = [
        round(
            table.column("quantity")[i].as_py()
            * table.column("unit_price")[i].as_py()
            * (1 - table.column("discount_pct")[i].as_py() / 100),
            2,
        )
        for i in range(table.num_rows)
    ]
    actual_net = round(sum(net_values), 2)
    if abs(actual_net - 23220.27) < 0.05:
        ok(f"net_revenue = {actual_net}")
    else:
        fail(f"net_revenue = {actual_net}, expected ~23220.27")

    assert_sum(table, "quantity", 529.0, label="total_units")

    # Return count
    assert_count_where(table, "is_return", 1, 9)

    # Per-region counts and gross revenue
    for region, expected_cnt, expected_gross in [
        ("Central", 39, 8617.99),
        ("East", 50, 9098.57),
        ("West", 31, 7789.9),
    ]:
        assert_count_where(table, "region", region, expected_cnt)
        mask = pc.equal(table.column("region"), region)
        filtered = table.filter(mask)
        region_gross = sum(
            round(filtered.column("quantity")[i].as_py() * filtered.column("unit_price")[i].as_py(), 2)
            for i in range(filtered.num_rows)
        )
        region_gross = round(region_gross, 2)
        if region_gross == expected_gross:
            ok(f"Gross revenue for {region} = {expected_gross}")
        else:
            fail(f"Gross revenue for {region} = {region_gross}, expected {expected_gross}")

    # Per-category counts
    assert_count_where(table, "category", "Clothing", 28)
    assert_count_where(table, "category", "Electronics", 20)

    # Electronics gross revenue
    mask = pc.equal(table.column("category"), "Electronics")
    filtered = table.filter(mask)
    elec_gross = sum(
        round(filtered.column("quantity")[i].as_py() * filtered.column("unit_price")[i].as_py(), 2)
        for i in range(filtered.num_rows)
    )
    elec_gross = round(elec_gross, 2)
    if elec_gross == 11302.93:
        ok("Electronics gross_revenue = 11302.93")
    else:
        fail(f"Electronics gross_revenue = {elec_gross}, expected 11302.93")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-aggregate-analytics demo"
    )
    parser.add_argument("data_root", help="Root path containing retail_sales/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V2 Aggregate Analytics -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "retail_sales")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_retail_sales(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
