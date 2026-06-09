#!/usr/bin/env python3
"""
Iceberg v2 Window Analytics -- Loyalty Members -- Data Verification
====================================================================
Reads the loyalty_members table through Iceberg metadata and verifies
60 members across 4 tiers and 5 airports.

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


def verify_loyalty_members(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("loyalty_members -- Window Analytics")

    table_path = os.path.join(data_root, "loyalty_members")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 60)

    # Tier distribution
    assert_count_where(table, "tier", "Bronze", 20)
    assert_count_where(table, "tier", "Silver", 18)
    assert_count_where(table, "tier", "Gold", 14)
    assert_count_where(table, "tier", "Platinum", 8)

    # Airport distribution: 5 airports, 12 each
    assert_distinct_count(table, "home_airport", 5)
    for airport in ["ATL", "DFW", "JFK", "LAX", "ORD"]:
        assert_count_where(table, "home_airport", airport, 12)

    # Overall totals
    assert_sum(table, "miles_ytd", 2685394.0, label="total_miles")
    assert_sum(table, "flights_ytd", 1682.0, label="total_flights")
    assert_sum(table, "spend_ytd", 615209.16, label="spend_ytd")
    assert_avg(table, "spend_ytd", 10253.49, label="avg_spend")

    # Platinum total spend
    plat_mask = pc.equal(table.column("tier"), "Platinum")
    plat = table.filter(plat_mask)
    plat_spend = round(pc.sum(plat.column("spend_ytd")).as_py(), 2)
    if plat_spend == 294131.32:
        ok(f"Platinum spend_ytd = 294131.32")
    else:
        fail(f"Platinum spend_ytd = {plat_spend}, expected 294131.32")

    # Top member by miles_ytd
    miles_col = table.column("miles_ytd")
    max_miles_idx = pc.index(miles_col, pc.max(miles_col)).as_py()
    top_id = table.column("member_id")[max_miles_idx].as_py()
    top_name = table.column("member_name")[max_miles_idx].as_py()
    top_miles = miles_col[max_miles_idx].as_py()

    if top_id == 55:
        ok(f"Top member member_id = 55")
    else:
        fail(f"Top member member_id = {top_id}, expected 55")
    if top_name == "Leo Park":
        ok(f"Top member name = 'Leo Park'")
    else:
        fail(f"Top member name = {top_name!r}, expected 'Leo Park'")
    if top_miles == 198664:
        ok(f"Top member miles_ytd = 198664")
    else:
        fail(f"Top member miles_ytd = {top_miles}, expected 198664")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-window-analytics demo"
    )
    parser.add_argument("data_root", help="Root path containing loyalty_members/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg v2 Window Analytics -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "loyalty_members")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_loyalty_members(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
