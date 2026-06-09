#!/usr/bin/env python3
"""
Iceberg V2 Fleet Telemetry -- Data Verification
=================================================
Reads the fleet_telemetry table through the Iceberg metadata chain and
verifies 450 telemetry records across 3 fleets with vehicle, driver,
and route analytics.

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
    assert_row_count, assert_avg, assert_sum,
    assert_distinct_count, assert_count_where,
    assert_format_version,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_fleet_telemetry(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("fleet_telemetry -- Fleet Telemetry")

    table_path = os.path.join(data_root, "fleet_telemetry")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 450)

    # Fleet distribution: 3 fleets, 150 each
    assert_distinct_count(table, "fleet", 3)
    for fleet in ["East-Coast", "Midwest", "West-Coast"]:
        assert_count_where(table, "fleet", fleet, 150)

    # Vehicle type distribution
    assert_count_where(table, "vehicle_type", "Box-Truck", 132)
    assert_count_where(table, "vehicle_type", "Delivery-Van", 156)
    assert_count_where(table, "vehicle_type", "Semi-Truck", 162)

    # Average speed per fleet
    for fleet, expected_avg in [("East-Coast", 37.27), ("Midwest", 37.89), ("West-Coast", 37.13)]:
        mask = pc.equal(table.column("fleet"), fleet)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("speed_mph")).as_py(), 2)
        if actual == expected_avg:
            ok(f"Avg speed for {fleet} = {expected_avg}")
        else:
            fail(f"Avg speed for {fleet} = {actual}, expected {expected_avg}")

    # Total idle time per fleet
    for fleet, expected_idle in [("East-Coast", 3355), ("Midwest", 3439), ("West-Coast", 3751)]:
        mask = pc.equal(table.column("fleet"), fleet)
        filtered = table.filter(mask)
        actual = int(pc.sum(filtered.column("idle_minutes")).as_py())
        if actual == expected_idle:
            ok(f"Total idle for {fleet} = {expected_idle}")
        else:
            fail(f"Total idle for {fleet} = {actual}, expected {expected_idle}")

    # Harsh braking per fleet and total
    total_harsh = 0
    for fleet, expected_harsh in [("East-Coast", 27), ("Midwest", 20), ("West-Coast", 31)]:
        mask = pc.and_(
            pc.equal(table.column("fleet"), fleet),
            pc.equal(table.column("harsh_braking"), True),
        )
        cnt = pc.sum(mask.cast("int64")).as_py()
        total_harsh += cnt
        if cnt == expected_harsh:
            ok(f"Harsh braking for {fleet} = {expected_harsh}")
        else:
            fail(f"Harsh braking for {fleet} = {cnt}, expected {expected_harsh}")

    if total_harsh == 78:
        ok("Total harsh braking = 78")
    else:
        fail(f"Total harsh braking = {total_harsh}, expected 78")

    # Speeding events (speed > 65 mph)
    speeding = pc.sum(pc.greater(table.column("speed_mph"), 65).cast("int64")).as_py()
    if speeding == 53:
        ok("Speeding events (>65 mph) = 53")
    else:
        fail(f"Speeding events (>65 mph) = {speeding}, expected 53")

    # Low fuel events (fuel_pct < 20)
    low_fuel = pc.sum(pc.less(table.column("fuel_level_pct"), 20).cast("int64")).as_py()
    if low_fuel == 74:
        ok("Low fuel events (<20%) = 74")
    else:
        fail(f"Low fuel events (<20%) = {low_fuel}, expected 74")

    # Distinct counts
    assert_distinct_count(table, "vehicle_id", 450)
    assert_distinct_count(table, "driver_id", 98)
    assert_distinct_count(table, "route_id", 15)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-fleet-telemetry demo"
    )
    parser.add_argument("data_root", help="Root path containing fleet_telemetry/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V2 Fleet Telemetry -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "fleet_telemetry")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_fleet_telemetry(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
