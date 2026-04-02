#!/usr/bin/env python3
"""
Customer Loyalty Program — Bloom Filters with UniForm — Iceberg Data Verification
===================================================================================
Reads the members table purely through the Iceberg metadata chain and verifies
the final state (no DML mutations — seed data only):
  - 40 loyalty members across 4 tiers (Bronze, Silver, Gold, Platinum)
  - Bloom filter columns on member_id and full_name

Final state: 40 rows, 4 tiers, total points 461500, total spend 136910.00.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing members/

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
def verify_members(data_root, verbose=False):
    print_section("members — Bloom Filters with UniForm")

    table_path = os.path.join(data_root, "members")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    # Format version
    assert_format_version(metadata, 1)

    # Final state: 40 rows (seed data only, no DML mutations)
    assert_row_count(table, 40)

    # 4 distinct tiers
    assert_distinct_count(table, "tier", 4)

    # Per-tier member counts: Bronze=11, Silver=10, Gold=10, Platinum=9
    assert_count_where(table, "tier", "Bronze", 11)
    assert_count_where(table, "tier", "Silver", 10)
    assert_count_where(table, "tier", "Gold", 10)
    assert_count_where(table, "tier", "Platinum", 9)

    # Total points = 461500
    assert_sum(table, "points", 461500)

    # Total lifetime_spend = 136910.00
    assert_sum(table, "lifetime_spend", 136910.00)

    # Average points = 11537.50
    assert_avg(table, "points", 11537.50)

    # Max spend = 10500.00, Min spend = 130.00
    assert_max(table, "lifetime_spend", 10500.00)
    assert_min(table, "lifetime_spend", 130.00)

    # Per-tier total points
    import pyarrow.compute as pc

    for tier, expected_pts in [("Bronze", 12500), ("Silver", 48500),
                               ("Gold", 136500), ("Platinum", 264000)]:
        mask = pc.equal(table.column("tier"), tier)
        tier_table = table.filter(mask)
        tier_pts = pc.sum(tier_table.column("points")).as_py()
        if tier_pts == expected_pts:
            ok(f"SUM(points) for {tier} = {expected_pts}")
        else:
            fail(f"SUM(points) for {tier} = {tier_pts}, expected {expected_pts}")

    # Spot-check: member_id=4 -> David Garcia, Platinum, 28000 pts, 8500.00 spend
    assert_value_where(table, "full_name", "David Garcia", "member_id", 4)
    assert_value_where(table, "tier", "Platinum", "member_id", 4)
    assert_value_where(table, "points", 28000, "member_id", 4)
    assert_value_where(table, "lifetime_spend", 8500.00, "member_id", 4)

    # Spot-check: Brian Wright (member_id=24), Platinum, 35000 pts, 10500.00 spend
    assert_value_where(table, "member_id", 24, "full_name", "Brian Wright")
    assert_value_where(table, "tier", "Platinum", "full_name", "Brian Wright")
    assert_value_where(table, "points", 35000, "full_name", "Brian Wright")
    assert_value_where(table, "lifetime_spend", 10500.00, "full_name", "Brian Wright")

    # Spot-check: member_id=8 -> James Wilson, Platinum, 32000 pts, 9800.00 spend
    assert_value_where(table, "full_name", "James Wilson", "member_id", 8)
    assert_value_where(table, "points", 32000, "member_id", 8)
    assert_value_where(table, "lifetime_spend", 9800.00, "member_id", 8)

    # Min/Max points per tier
    for tier, exp_min, exp_max in [("Bronze", 450, 2000), ("Silver", 3600, 6100),
                                    ("Gold", 11000, 17000), ("Platinum", 25500, 35000)]:
        mask = pc.equal(table.column("tier"), tier)
        tier_table = table.filter(mask)
        actual_min = pc.min(tier_table.column("points")).as_py()
        actual_max = pc.max(tier_table.column("points")).as_py()
        if actual_min == exp_min:
            ok(f"MIN(points) for {tier} = {exp_min}")
        else:
            fail(f"MIN(points) for {tier} = {actual_min}, expected {exp_min}")
        if actual_max == exp_max:
            ok(f"MAX(points) for {tier} = {exp_max}")
        else:
            fail(f"MAX(points) for {tier} = {actual_max}, expected {exp_max}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-bloom-filters demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing members/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Bloom Filters with UniForm — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "members")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_members(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
