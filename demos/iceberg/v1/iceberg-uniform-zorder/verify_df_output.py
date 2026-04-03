#!/usr/bin/env python3
"""
Iceberg UniForm Z-ORDER Spatial Optimization — Data Verification
==================================================================
Reads the delivery_tracking table through the Iceberg metadata chain and
verifies the final state after INSERT x3 and OPTIMIZE ZORDER BY (lat, lon).

Final state: 72 deliveries across 6 cities (12 each).
  - total_fees = 726.19, total_weight = 233.2
  - 48 delivered, 12 in_transit, 12 pending
  - NYC bounding box (40.65-40.80, -74.05 to -73.95): 10 deliveries

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
def verify_delivery_tracking(data_root, verbose=False):
    print_section("delivery_tracking — Z-ORDER Final State")

    table_path = os.path.join(data_root, "delivery_tracking")
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

    # Final: 36 + 18 + 18 = 72
    assert_row_count(table, 72)

    # Grand totals
    print(f"\n  {CYAN}Grand totals:{RESET}")
    assert_distinct_count(table, "city", 6)
    assert_sum(table, "delivery_fee", 726.19)
    # Note: package_weight needs 1 decimal precision — round manually
    import pyarrow.compute as pc
    actual_weight = round(pc.sum(table.column("package_weight")).as_py(), 1)
    if actual_weight == 233.2:
        ok(f"SUM(package_weight) = 233.2")
    else:
        fail(f"SUM(package_weight) = {actual_weight}, expected 233.2")

    # Status distribution
    print(f"\n  {CYAN}Status distribution:{RESET}")
    assert_count_where(table, "delivery_status", "delivered", 48)
    assert_count_where(table, "delivery_status", "in_transit", 12)
    assert_count_where(table, "delivery_status", "pending", 12)

    # Per-city counts (12 each)
    print(f"\n  {CYAN}Per-city counts:{RESET}")
    for city in ["Chicago", "Houston", "Los Angeles", "New York", "Philadelphia", "Phoenix"]:
        assert_count_where(table, "city", city, 12)

    # Per-city fee totals
    print(f"\n  {CYAN}Per-city fee totals:{RESET}")
    for city, expected_fee in [("Chicago", 115.97), ("Houston", 126.95),
                                ("Los Angeles", 136.43), ("New York", 109.94),
                                ("Philadelphia", 112.96), ("Phoenix", 123.94)]:
        mask = pc.equal(table.column("city"), city)
        filtered = table.filter(mask)
        actual = round(pc.sum(filtered.column("delivery_fee")).as_py(), 2)
        if actual == expected_fee:
            ok(f"SUM(delivery_fee) WHERE city={city!r} = {expected_fee}")
        else:
            fail(f"SUM(delivery_fee) WHERE city={city!r} = {actual}, expected {expected_fee}")

    # Per-city weight totals
    print(f"\n  {CYAN}Per-city weight totals:{RESET}")
    for city, expected_wt in [("Chicago", 36.8), ("Houston", 42.2),
                               ("Los Angeles", 47.0), ("New York", 32.4),
                               ("Philadelphia", 34.3), ("Phoenix", 40.5)]:
        mask = pc.equal(table.column("city"), city)
        filtered = table.filter(mask)
        actual = round(pc.sum(filtered.column("package_weight")).as_py(), 1)
        if actual == expected_wt:
            ok(f"SUM(package_weight) WHERE city={city!r} = {expected_wt}")
        else:
            fail(f"SUM(package_weight) WHERE city={city!r} = {actual}, expected {expected_wt}")

    # Spot-checks across all 3 batches
    print(f"\n  {CYAN}Cross-batch spot-checks:{RESET}")
    # Seed (batch 1)
    assert_value_where(table, "driver_id", "DRV-101", "delivery_id", 1)
    assert_value_where(table, "latitude", 40.71, "delivery_id", 1)
    assert_value_where(table, "delivery_status", "delivered", "delivery_id", 1)
    assert_value_where(table, "delivery_fee", 8.99, "delivery_id", 1)
    assert_value_where(table, "city", "New York", "delivery_id", 1)

    # Batch 2
    assert_value_where(table, "driver_id", "DRV-107", "delivery_id", 37)
    assert_value_where(table, "delivery_fee", 7.50, "delivery_id", 37)
    assert_value_where(table, "city", "New York", "delivery_id", 37)

    # Batch 3
    assert_value_where(table, "driver_id", "DRV-612", "delivery_id", 72)
    assert_value_where(table, "delivery_fee", 5.50, "delivery_id", 72)
    assert_value_where(table, "city", "Philadelphia", "delivery_id", 72)

    # NYC bounding box (lat 40.65-40.80, lon -74.05 to -73.95)
    print(f"\n  {CYAN}NYC bounding box query:{RESET}")
    lat = table.column("latitude")
    lon = table.column("longitude")
    lat_mask = pc.and_(pc.greater_equal(lat, 40.65), pc.less_equal(lat, 40.80))
    lon_mask = pc.and_(pc.greater_equal(lon, -74.05), pc.less_equal(lon, -73.95))
    bbox_mask = pc.and_(lat_mask, lon_mask)
    bbox_count = pc.sum(bbox_mask).as_py()
    if bbox_count == 10:
        ok(f"NYC bounding box count = 10")
    else:
        fail(f"NYC bounding box count = {bbox_count}, expected 10")

    bbox_filtered = table.filter(bbox_mask)
    bbox_fee = round(pc.sum(bbox_filtered.column("delivery_fee")).as_py(), 2)
    if bbox_fee == 88.94:
        ok(f"NYC bounding box total_fee = 88.94")
    else:
        fail(f"NYC bounding box total_fee = {bbox_fee}, expected 88.94")

    # LA bounding box
    print(f"\n  {CYAN}LA bounding box query:{RESET}")
    lat_mask2 = pc.and_(pc.greater_equal(lat, 33.0), pc.less_equal(lat, 35.0))
    lon_mask2 = pc.and_(pc.greater_equal(lon, -119.0), pc.less_equal(lon, -118.0))
    la_mask = pc.and_(lat_mask2, lon_mask2)
    la_count = pc.sum(la_mask).as_py()
    if la_count == 12:
        ok(f"LA bounding box count = 12")
    else:
        fail(f"LA bounding box count = {la_count}, expected 12")

    la_filtered = table.filter(la_mask)
    la_fee = round(pc.sum(la_filtered.column("delivery_fee")).as_py(), 2)
    if la_fee == 136.43:
        ok(f"LA bounding box total_fee = 136.43")
    else:
        fail(f"LA bounding box total_fee = {la_fee}, expected 136.43")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-zorder demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing delivery_tracking/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Z-ORDER — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "delivery_tracking")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_delivery_tracking(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
