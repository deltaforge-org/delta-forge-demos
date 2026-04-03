#!/usr/bin/env python3
"""
Iceberg v2 Position Deletes -- Cold Chain Readings -- Data Verification
========================================================================
Reads the cold_chain_readings table through Iceberg metadata and verifies
570 rows (600 - 30 faulty SENSOR-F01 rows) across 4 routes and 4 vaccine
types with position delete support.

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
    assert_format_version, assert_min, assert_max,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_cold_chain_readings(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("cold_chain_readings -- Position Deletes")

    table_path = os.path.join(data_root, "cold_chain_readings")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 570)

    # Faulty sensor SENSOR-F01 must have 0 rows (deleted via position deletes)
    assert_count_where(table, "sensor_id", "SENSOR-F01", 0, label="faulty sensor removed")

    # Per-route distribution
    assert_count_where(table, "route", "ROUTE-A", 120)
    assert_count_where(table, "route", "ROUTE-B", 150)
    assert_count_where(table, "route", "ROUTE-C", 150)
    assert_count_where(table, "route", "ROUTE-D", 150)

    # Distinct sensors
    assert_distinct_count(table, "sensor_id", 20)

    # Vaccine type distribution
    assert_count_where(table, "vaccine_type", "HPV-9v", 150)
    assert_count_where(table, "vaccine_type", "Influenza-Quad", 150)
    assert_count_where(table, "vaccine_type", "Tdap", 150)
    assert_count_where(table, "vaccine_type", "mRNA-COVID", 120)

    # Temperature excursions per route (temp outside safe range)
    for route, expected_exc in [
        ("ROUTE-A", 45),
        ("ROUTE-B", 45),
        ("ROUTE-C", 57),
        ("ROUTE-D", 63),
    ]:
        mask = pc.equal(table.column("route"), route)
        filtered = table.filter(mask)
        exc_col = filtered.column("temp_excursion")
        exc_count = pc.sum(exc_col).as_py()
        if exc_count == expected_exc:
            ok(f"Excursions for {route} = {expected_exc}")
        else:
            fail(f"Excursions for {route} = {exc_count}, expected {expected_exc}")

    # Total excursions
    total_exc = pc.sum(table.column("temp_excursion")).as_py()
    if total_exc == 210:
        ok(f"Total excursions = 210")
    else:
        fail(f"Total excursions = {total_exc}, expected 210")

    # Per-route average temperature
    for route, expected_avg in [
        ("ROUTE-A", -0.24),
        ("ROUTE-B", 0.81),
        ("ROUTE-C", 0.21),
        ("ROUTE-D", -0.29),
    ]:
        mask = pc.equal(table.column("route"), route)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("temperature_c")).as_py(), 2)
        if actual == expected_avg:
            ok(f"Avg temp for {route} = {expected_avg}")
        else:
            fail(f"Avg temp for {route} = {actual}, expected {expected_avg}")

    # Overall temperature stats
    assert_min(table, "temperature_c", -7.95, label="min_temp")
    assert_max(table, "temperature_c", 7.98, label="max_temp")
    assert_avg(table, "temperature_c", 0.14, label="avg_temp")

    # Low battery readings (battery_pct <= 25)
    low_batt = pc.sum(pc.less_equal(table.column("battery_pct"), 25)).as_py()
    if low_batt == 70:
        ok(f"Low battery readings (<=25%) = 70")
    else:
        fail(f"Low battery readings (<=25%) = {low_batt}, expected 70")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-position-deletes demo"
    )
    parser.add_argument("data_root", help="Root path containing cold_chain_readings/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg v2 Position Deletes -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "cold_chain_readings")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_cold_chain_readings(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
