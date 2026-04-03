#!/usr/bin/env python3
"""
Iceberg V3 Puffin Deletion Vectors -- Shipment Manifests Verification
======================================================================
Reads the shipment_manifests table through Iceberg metadata after seeding
540 rows and deleting 36 faulty SCAN-ERR scanner rows via deletion vectors.
Final state: 504 shipment manifest rows.

Usage:
    python verify_df_output.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_count_where, assert_distinct_count,
    assert_min, assert_max, assert_avg, assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_shipment_manifests(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("shipment_manifests -- Post-Deletion Vectors")

    table_path = os.path.join(data_root, "shipment_manifests")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # Format version
    assert_format_version(metadata, 3)

    # 540 - 36 SCAN-ERR = 504
    assert_row_count(table, 504)

    # SCAN-ERR scanner must be gone
    assert_count_where(table, "scanner_id", "SCAN-ERR", 0)

    # Per-region counts
    assert_count_where(table, "region", "Americas", 144)
    assert_count_where(table, "region", "EMEA", 180)
    assert_count_where(table, "region", "APAC", 180)

    # Scanner count
    assert_distinct_count(table, "scanner_id", 15)

    # Category counts
    assert_count_where(table, "product_category", "Automotive-Parts", 86)
    assert_count_where(table, "product_category", "Electronics", 107)
    assert_count_where(table, "product_category", "Heavy-Machinery", 73)
    assert_count_where(table, "product_category", "Perishable-Foods", 82)
    assert_count_where(table, "product_category", "Pharmaceuticals", 77)
    assert_count_where(table, "product_category", "Textiles", 79)

    # Total hazardous
    total_hazardous = pc.sum(pc.equal(table.column("is_hazardous"), True)).as_py()
    if total_hazardous == 59:
        ok(f"Total hazardous = 59")
    else:
        fail(f"Total hazardous = {total_hazardous}, expected 59")

    # Average weight per region
    for region, expected_avg in [
        ("Americas", 1276.76),
        ("EMEA", 1344.52),
        ("APAC", 1216.02),
    ]:
        mask = pc.equal(table.column("region"), region)
        filtered = table.filter(mask)
        actual_avg = round(pc.mean(filtered.column("weight_kg")).as_py(), 2)
        if actual_avg == expected_avg:
            ok(f"Avg weight {region} = {expected_avg}")
        else:
            fail(f"Avg weight {region} = {actual_avg}, expected {expected_avg}")

    # Overall weight stats
    assert_min(table, "weight_kg", 1.29)
    assert_max(table, "weight_kg", 2499.03)
    assert_avg(table, "weight_kg", 1279.27)

    # Country and carrier counts
    assert_distinct_count(table, "destination_country", 18)
    assert_distinct_count(table, "carrier", 12)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-deletion-vectors demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing shipment_manifests/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Deletion Vectors -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "shipment_manifests")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_shipment_manifests(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
