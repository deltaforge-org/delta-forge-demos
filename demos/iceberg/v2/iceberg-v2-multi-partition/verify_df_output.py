#!/usr/bin/env python3
"""
Iceberg v2 Multi-Partition -- Weather Readings -- Data Verification
====================================================================
Reads the weather_readings table through Iceberg metadata and verifies
450 rows across 5 regions, 15 stations, and 5 conditions with
multi-partition on region (identity) + years(observation_date).

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
    assert_null_count,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_weather_readings(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("weather_readings -- Multi-Partition (region + years(observation_date))")

    table_path = os.path.join(data_root, "weather_readings")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 450)

    # Region distribution: 5 regions, 90 each
    assert_distinct_count(table, "region", 5)

    # Station and condition counts
    assert_distinct_count(table, "station_id", 15)
    assert_distinct_count(table, "condition", 5)

    # Condition distribution
    assert_count_where(table, "condition", "Clear", 83)
    assert_count_where(table, "condition", "Cloudy", 102)
    assert_count_where(table, "condition", "Rain", 88)
    assert_count_where(table, "condition", "Snow", 94)
    assert_count_where(table, "condition", "Storm", 83)

    # Extreme readings (temp > 35 or temp < -5)
    temp_col = table.column("temperature_c")
    extreme_mask = pc.or_(pc.greater(temp_col, 35), pc.less(temp_col, -5))
    extreme_count = pc.sum(extreme_mask).as_py()
    if extreme_count == 41:
        ok(f"Extreme temperature count (>35 or <-5) = 41")
    else:
        fail(f"Extreme temperature count = {extreme_count}, expected 41")

    # NULL precipitation
    assert_null_count(table, "precipitation_mm", 58, label="null_precip_count")

    # Temperature range
    assert_min(table, "temperature_c", -9.9, label="min_temp")
    assert_max(table, "temperature_c", 39.5, label="max_temp")

    # Humidity range
    assert_min(table, "humidity_pct", 20.9, label="min_humidity")
    assert_max(table, "humidity_pct", 99.9, label="max_humidity")

    # Wind and precipitation totals
    assert_sum(table, "wind_speed_kmh", 25320.8, label="total_wind")

    # Average precipitation (excluding NULLs)
    not_null_mask = pc.is_valid(table.column("precipitation_mm"))
    precip_filtered = table.filter(not_null_mask)
    actual_avg_precip = round(pc.mean(precip_filtered.column("precipitation_mm")).as_py(), 2)
    if actual_avg_precip == 25.01:
        ok(f"AVG(precipitation_mm) excluding NULLs = 25.01")
    else:
        fail(f"AVG(precipitation_mm) excluding NULLs = {actual_avg_precip}, expected 25.01")

    # Per-station average temperature (one station per region prefix).
    # WX-SA001's raw avg is 24.605000000000004, a float boundary that rounds
    # to either 24.60 or 24.61 depending on summation order — accept both.
    for station, expected_avg in [
        ("WX-AF001", 23.84),
        ("WX-AS001", 12.98),
        ("WX-EU001", 12.99),
        ("WX-NA001", 12.46),
        ("WX-SA001", (24.60, 24.61)),
    ]:
        mask = pc.equal(table.column("station_id"), station)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("temperature_c")).as_py(), 2)
        if isinstance(expected_avg, tuple):
            if actual in expected_avg:
                ok(f"Avg temp for {station} = {actual} (in {expected_avg})")
            else:
                fail(f"Avg temp for {station} = {actual}, expected one of {expected_avg}")
        else:
            if actual == expected_avg:
                ok(f"Avg temp for {station} = {expected_avg}")
            else:
                fail(f"Avg temp for {station} = {actual}, expected {expected_avg}")

    # Per-year average temperature
    for year, expected_avg in [(2023, 16.65), (2024, 17.73), (2025, 17.18)]:
        obs_date = table.column("observation_date")
        year_col = pc.year(obs_date)
        year_mask = pc.equal(year_col, year)
        filtered = table.filter(year_mask)
        actual = round(pc.mean(filtered.column("temperature_c")).as_py(), 2)
        if actual == expected_avg:
            ok(f"Avg temp for year {year} = {expected_avg}")
        else:
            fail(f"Avg temp for year {year} = {actual}, expected {expected_avg}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-multi-partition demo"
    )
    parser.add_argument("data_root", help="Root path containing weather_readings/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg v2 Multi-Partition -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "weather_readings")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_weather_readings(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
