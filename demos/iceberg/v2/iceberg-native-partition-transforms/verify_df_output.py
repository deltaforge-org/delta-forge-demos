#!/usr/bin/env python3
"""
Iceberg Native Partition Transforms -- Network Traffic -- Data Verification
============================================================================
Reads the network_traffic table through Iceberg metadata and verifies
480 rows across 3 regions, 4 protocols, and 4 threat levels.

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


def verify_network_traffic(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("network_traffic -- Partition Transforms")

    table_path = os.path.join(data_root, "network_traffic")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 480)

    # Region distribution: 3 regions, 160 each
    assert_distinct_count(table, "region", 3)
    assert_count_where(table, "region", "asia-pacific", 160)
    assert_count_where(table, "region", "europe", 160)
    assert_count_where(table, "region", "north-america", 160)

    # Protocol distribution: 4 protocols, 120 each
    assert_distinct_count(table, "protocol", 4)
    for proto in ["DNS", "ICMP", "TCP", "UDP"]:
        assert_count_where(table, "protocol", proto, 120)

    # Threat level distribution
    assert_count_where(table, "threat_level", "critical", 22)
    assert_count_where(table, "threat_level", "high", 51)
    assert_count_where(table, "threat_level", "low", 248)
    assert_count_where(table, "threat_level", "medium", 159)

    # Byte statistics
    assert_sum(table, "bytes_transferred", 251040311.0, label="total_bytes")
    assert_avg(table, "bytes_transferred", 523000.65, label="avg_bytes")
    assert_min(table, "bytes_transferred", 1837, label="min_bytes")
    assert_max(table, "bytes_transferred", 1047983, label="max_bytes")

    # Per-region bytes
    for region, expected in [
        ("asia-pacific", 85974312),
        ("europe", 80023887),
        ("north-america", 85042112),
    ]:
        mask = pc.equal(table.column("region"), region)
        filtered = table.filter(mask)
        actual = pc.sum(filtered.column("bytes_transferred")).as_py()
        if actual == expected:
            ok(f"Bytes for {region} = {expected}")
        else:
            fail(f"Bytes for {region} = {actual}, expected {expected}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-native-partition-transforms demo"
    )
    parser.add_argument("data_root", help="Root path containing network_traffic/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg Native Partition Transforms -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "network_traffic")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_network_traffic(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
