#!/usr/bin/env python3
"""
Iceberg Native Large Manifests -- Web Analytics -- Data Verification
=====================================================================
Reads the web_analytics table through Iceberg metadata and verifies
600 rows across 10 batches, 10 countries, 3 device types, and 10 referrers.

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
    assert_format_version, assert_value_where,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_web_analytics(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("web_analytics -- Large Manifests")

    table_path = os.path.join(data_root, "web_analytics")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 600)

    # Distinct dimensions
    assert_distinct_count(table, "country", 10)
    assert_distinct_count(table, "device_type", 3)
    assert_distinct_count(table, "referrer", 10)

    # Per-device counts
    assert_count_where(table, "device_type", "desktop", 194)
    assert_count_where(table, "device_type", "mobile", 204)
    assert_count_where(table, "device_type", "tablet", 202)

    # Bounce count
    bounce_mask = pc.equal(table.column("is_bounce"), True)
    bounce_count = pc.sum(bounce_mask).as_py()
    if bounce_count == 165:
        ok("Bounce count = 165")
    else:
        fail(f"Bounce count = {bounce_count}, expected 165")

    # Total and avg events
    assert_sum(table, "event_count", 6023.0, label="total_events")
    assert_avg(table, "event_count", 10.04, label="avg_events")

    # Total and avg time on page
    assert_sum(table, "time_on_page", 173924.0, label="total_time_on_page")
    assert_avg(table, "time_on_page", 289.87, label="avg_time_on_page")

    # Per-country counts
    for country, expected in [
        ("AU", 48), ("BR", 66), ("CA", 76), ("DE", 48), ("FR", 71),
        ("IN", 65), ("JP", 58), ("MX", 57), ("UK", 64), ("US", 47),
    ]:
        assert_count_where(table, "country", country, expected)

    # Spot check: session_id='sess-00-005-30893faf'
    assert_value_where(table, "country", "CA", "session_id", "sess-00-005-30893faf")
    assert_value_where(table, "device_type", "desktop", "session_id", "sess-00-005-30893faf")
    assert_value_where(table, "event_count", 16, "session_id", "sess-00-005-30893faf")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-native-large-manifests demo"
    )
    parser.add_argument("data_root", help="Root path containing web_analytics/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg Native Large Manifests -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "web_analytics")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_web_analytics(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
