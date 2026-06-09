#!/usr/bin/env python3
"""
Iceberg v2 Hidden Partitions -- Rideshare Trips -- Data Verification
=====================================================================
Reads the trips table through Iceberg metadata and verifies 300 rows
across 6 months, 5 cities, and 3 payment types with hidden partition
on months(pickup_date).

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
    assert_format_version, assert_null_count,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_trips(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("trips -- Hidden Partitions (months(pickup_date))")

    table_path = os.path.join(data_root, "trips")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 300)

    # City distribution: 5 cities, 60 each
    assert_distinct_count(table, "city", 5)
    for city in ["Austin", "Chicago", "New York", "San Francisco", "Seattle"]:
        assert_count_where(table, "city", city, 60)

    # Payment type distribution
    assert_distinct_count(table, "payment_type", 3)
    assert_count_where(table, "payment_type", "Cash", 104)
    assert_count_where(table, "payment_type", "Credit Card", 108)
    assert_count_where(table, "payment_type", "Digital Wallet", 88)

    # Driver count
    assert_distinct_count(table, "driver_id", 50)

    # Fare statistics
    assert_sum(table, "fare_amount", 10890.01, label="total_fare")
    assert_avg(table, "fare_amount", 36.3, label="avg_fare")
    assert_sum(table, "distance_miles", 3747.1, label="total_distance")

    # NULL tips (Cash rides with NULL tips)
    assert_null_count(table, "tip_amount", 14, label="Cash rides with NULL tips")

    # Per-city average distance
    for city, expected_avg in [
        ("San Francisco", 13.99),
        ("New York", 13.26),
        ("Seattle", 12.22),
        ("Austin", 12.11),
        ("Chicago", 10.87),
    ]:
        mask = pc.equal(table.column("city"), city)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("distance_miles")).as_py(), 2)
        if actual == expected_avg:
            ok(f"Avg distance for {city} = {expected_avg}")
        else:
            fail(f"Avg distance for {city} = {actual}, expected {expected_avg}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-hidden-partitions demo"
    )
    parser.add_argument("data_root", help="Root path containing trips/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg v2 Hidden Partitions -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "trips")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_trips(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
