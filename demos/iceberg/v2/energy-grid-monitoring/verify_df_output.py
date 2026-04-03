#!/usr/bin/env python3
"""
Energy Grid Monitoring -- Data Verification
=============================================
Reads the grid_readings table through Iceberg metadata and verifies
600 sensor readings across 3 regions, 3 meter types, and 3 voltage levels.

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


def verify_grid_readings(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("grid_readings -- Energy Grid Monitoring")

    # Support both data_root/grid_readings and data_root directly as table path
    table_path = os.path.join(data_root, "grid_readings")
    if not os.path.isdir(table_path) and os.path.isdir(os.path.join(data_root, "metadata")):
        table_path = data_root

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 600)

    # Region distribution: 3 regions, 200 each
    assert_distinct_count(table, "region", 3)
    assert_count_where(table, "region", "North", 200)
    assert_count_where(table, "region", "South", 200)
    assert_count_where(table, "region", "East", 200)

    # Meter type distribution
    assert_count_where(table, "meter_type", "Commercial", 208)
    assert_count_where(table, "meter_type", "Industrial", 198)
    assert_count_where(table, "meter_type", "Residential", 194)

    # Voltage distribution
    assert_count_where(table, "voltage", 220, 187)
    assert_count_where(table, "voltage", 230, 205)
    assert_count_where(table, "voltage", 240, 208)

    # Total energy
    assert_sum(table, "energy_kwh", 993.24, label="total_energy_kwh")

    # Per-region energy
    for region, expected in [("North", 341.6525), ("South", 324.7675), ("East", 326.8175)]:
        mask = pc.equal(table.column("region"), region)
        filtered = table.filter(mask)
        actual = round(pc.sum(filtered.column("energy_kwh")).as_py(), 4)
        if actual == expected:
            ok(f"Energy for {region} = {expected}")
        else:
            fail(f"Energy for {region} = {actual}, expected {expected}")

    # Distinct meters and substations
    assert_distinct_count(table, "meter_id", 600)
    assert_distinct_count(table, "substation", 15)

    # Avg power by meter type
    for meter_type, expected_avg in [("Commercial", 6.44), ("Industrial", 6.75), ("Residential", 6.68)]:
        mask = pc.equal(table.column("meter_type"), meter_type)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("power_kw")).as_py(), 2)
        if actual == expected_avg:
            ok(f"Avg power for {meter_type} = {expected_avg}")
        else:
            fail(f"Avg power for {meter_type} = {actual}, expected {expected_avg}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for energy-grid-monitoring demo"
    )
    parser.add_argument("data_root", help="Root path containing grid_readings/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Energy Grid Monitoring -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "grid_readings")
    # Fallback: if grid_readings/ doesn't exist but data_root has metadata/, use data_root
    if not os.path.isdir(tbl_dir):
        if os.path.isdir(os.path.join(data_root, "metadata")):
            info("grid_readings/ not found, but data_root has metadata/ -- using data_root directly")
        else:
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    verify_grid_readings(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
