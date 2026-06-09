#!/usr/bin/env python3
"""
Iceberg UniForm Column-Level Statistics — Data Verification
=============================================================
Reads the ad_clicks table through the Iceberg metadata chain and verifies
the final state after INSERTs (extreme values) and UPDATEs (late conversions).

Final state: 35 clicks, 3 campaigns, 4 device types.
  - min CPC = 0.10, max CPC = 6.00
  - min CV = 0.50, max CV = 150.00
  - 13 NULL conversion_values, 22 non-NULL
  - 22 converted clicks

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
    assert_row_count, assert_min, assert_max, assert_distinct_count,
    assert_null_count, assert_count_where, assert_value_where,
    assert_format_version,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status
from verify_lib.assertions import CYAN, RESET


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_ad_clicks(data_root, verbose=False):
    print_section("ad_clicks — Column Stats Final State")

    table_path = os.path.join(data_root, "ad_clicks")
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

    # Final row count: 30 seed + 5 inserted = 35
    assert_row_count(table, 35)

    # Distinct counts
    assert_distinct_count(table, "campaign_id", 3)
    assert_distinct_count(table, "device_type", 4)

    # Column statistics: min/max after INSERT of extreme values
    print(f"\n  {CYAN}Column statistics (min/max):{RESET}")
    assert_min(table, "cost_per_click", 0.10)
    assert_max(table, "cost_per_click", 6.00)
    assert_min(table, "conversion_value", 0.50)
    assert_max(table, "conversion_value", 150.00)

    # NULL counts after UPDATE (3 NULLs filled)
    # 14 original NULLs + 2 new NULLs (clicks 31,33) - 3 filled (clicks 2,6,12) = 13
    assert_null_count(table, "conversion_value", 13)

    import pyarrow.compute as pc
    nonnull = pc.sum(pc.is_valid(table.column("conversion_value"))).as_py()
    if nonnull == 22:
        ok(f"Non-null conversion_value count = 22")
    else:
        fail(f"Non-null conversion_value count = {nonnull}, expected 22")

    # Converted count
    print(f"\n  {CYAN}Conversion status:{RESET}")
    assert_count_where(table, "is_converted", True, 22)

    # Campaign distribution
    print(f"\n  {CYAN}Campaign distribution:{RESET}")
    assert_count_where(table, "campaign_id", "summer-sale", 12)
    assert_count_where(table, "campaign_id", "back-to-school", 11)
    assert_count_where(table, "campaign_id", "holiday-promo", 12)

    # Verify UPDATE mutations persisted
    print(f"\n  {CYAN}UPDATE spot-checks (late conversions):{RESET}")
    assert_value_where(table, "conversion_value", 5.0, "click_id", 2)
    assert_value_where(table, "is_converted", True, "click_id", 2)
    assert_value_where(table, "conversion_value", 7.5, "click_id", 6)
    assert_value_where(table, "is_converted", True, "click_id", 6)
    assert_value_where(table, "conversion_value", 11.0, "click_id", 12)
    assert_value_where(table, "is_converted", True, "click_id", 12)

    # Verify INSERT extreme values
    print(f"\n  {CYAN}INSERT extreme value spot-checks:{RESET}")
    assert_value_where(table, "cost_per_click", 0.15, "click_id", 31)
    assert_value_where(table, "cost_per_click", 5.5, "click_id", 32)
    assert_value_where(table, "conversion_value", 120.0, "click_id", 32)
    assert_value_where(table, "cost_per_click", 0.1, "click_id", 34)
    assert_value_where(table, "conversion_value", 0.5, "click_id", 34)
    assert_value_where(table, "cost_per_click", 6.0, "click_id", 35)
    assert_value_where(table, "conversion_value", 150.0, "click_id", 35)

    # Seed data spot-check
    print(f"\n  {CYAN}Seed data spot-checks:{RESET}")
    assert_value_where(table, "campaign_id", "summer-sale", "click_id", 1)
    assert_value_where(table, "cost_per_click", 1.25, "click_id", 1)
    assert_value_where(table, "conversion_value", 12.5, "click_id", 1)
    assert_value_where(table, "device_type", "mobile", "click_id", 1)
    assert_value_where(table, "campaign_id", "holiday-promo", "click_id", 27)
    assert_value_where(table, "cost_per_click", 4.0, "click_id", 27)
    assert_value_where(table, "conversion_value", 55.0, "click_id", 27)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-column-stats demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing ad_clicks/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Column Stats — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "ad_clicks")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_ad_clicks(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
