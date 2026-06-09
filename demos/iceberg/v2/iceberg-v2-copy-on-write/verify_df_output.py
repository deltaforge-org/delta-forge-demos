#!/usr/bin/env python3
"""
Iceberg V2 Copy-on-Write -- Data Verification
===============================================
Reads the shipments table through the Iceberg metadata chain and verifies
110 rows remain after 10 deletions, with no delete files (copy-on-write).

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


def verify_shipments(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("shipments -- Copy-on-Write")

    table_path = os.path.join(data_root, "shipments")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)

    # 120 - 10 deleted = 110
    assert_row_count(table, 110)

    # Status distribution
    assert_count_where(table, "status", "Delivered", 70)
    assert_count_where(table, "status", "In Transit", 15)
    assert_count_where(table, "status", "Processing", 14)
    assert_count_where(table, "status", "Returned", 11)

    # Carrier distribution and avg cost
    for carrier, expected_cnt, expected_avg in [
        ("DHL", 30, 230.84),
        ("FedEx", 32, 265.77),
        ("UPS", 24, 295.16),
        ("USPS", 24, 259.04),
    ]:
        assert_count_where(table, "carrier", carrier, expected_cnt)
        mask = pc.equal(table.column("carrier"), carrier)
        filtered = table.filter(mask)
        actual_avg = round(pc.mean(filtered.column("shipping_cost")).as_py(), 2)
        if actual_avg == expected_avg:
            ok(f"Avg shipping cost for {carrier} = {expected_avg}")
        else:
            fail(f"Avg shipping cost for {carrier} = {actual_avg}, expected {expected_avg}")

    # Priority distribution
    assert_count_where(table, "priority", "Express", 43)
    assert_count_where(table, "priority", "Overnight", 33)
    assert_count_where(table, "priority", "Standard", 34)

    # Delivered with actual_delivery date (NOT NULL)
    non_null = pc.is_valid(table.column("actual_delivery"))
    delivered_with_date = pc.sum(non_null.cast("int64")).as_py()
    if delivered_with_date == 70:
        ok("Delivered with actual_delivery date = 70")
    else:
        fail(f"Delivered with actual_delivery date = {delivered_with_date}, expected 70")

    # Totals and averages
    assert_sum(table, "shipping_cost", 28730.35, label="total_shipping_cost")
    assert_avg(table, "weight_kg", 67.44, label="avg_weight")

    # Deleted shipment IDs must be absent
    deleted_ids = [
        "SHP-0009", "SHP-0012", "SHP-0015", "SHP-0017", "SHP-0020",
        "SHP-0022", "SHP-0029", "SHP-0031", "SHP-0032", "SHP-0036",
    ]
    for shp_id in deleted_ids:
        mask = pc.equal(table.column("shipment_id"), shp_id)
        filtered = table.filter(mask)
        if filtered.num_rows == 0:
            ok(f"Deleted {shp_id} is absent")
        else:
            fail(f"Deleted {shp_id} still present ({filtered.num_rows} rows)")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-copy-on-write demo"
    )
    parser.add_argument("data_root", help="Root path containing shipments/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V2 Copy-on-Write -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "shipments")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_shipments(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
