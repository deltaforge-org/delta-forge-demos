#!/usr/bin/env python3
"""
Iceberg V3 UniForm Puffin Deletion Vectors -- Data Verification
=================================================================
Reads the puffin_dv_products table through Iceberg metadata after seeding
10 products and deleting 3 (ids 2, 5, 8) via Puffin deletion vectors.
Final state: 7 products.

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
    assert_row_count, assert_sum, assert_count_where,
    assert_value_where, assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_puffin_dv_products(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("puffin_dv_products -- Post-Deletion Final State")

    table_path = os.path.join(data_root, "puffin_dv_products")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # Format version
    assert_format_version(metadata, 3)

    # 10 seeded - 3 deleted (ids 2, 5, 8) = 7
    assert_row_count(table, 7)

    # Total price
    assert_sum(table, "price", 2813.48)

    # Category counts
    assert_count_where(table, "category", "Electronics", 2)
    assert_count_where(table, "category", "Energy", 2)
    assert_count_where(table, "category", "Industrial", 2)
    assert_count_where(table, "category", "Science", 1)

    # Spot checks per row
    assert_value_where(table, "name", "Quantum Widget", "id", 1)
    assert_value_where(table, "price", 299.99, "id", 1)

    assert_value_where(table, "name", "Bio Reactor Kit", "id", 3)
    assert_value_where(table, "price", 599.0, "id", 3)

    assert_value_where(table, "name", "Solar Panel Mini", "id", 4)
    assert_value_where(table, "price", 425.0, "id", 4)

    assert_value_where(table, "name", "LED Matrix Board", "id", 6)
    assert_value_where(table, "price", 175.0, "id", 6)

    assert_value_where(table, "name", "Thermal Coupler", "id", 7)
    assert_value_where(table, "price", 64.5, "id", 7)

    assert_value_where(table, "name", "Wind Turbine Blade", "id", 9)
    assert_value_where(table, "price", 850.0, "id", 9)

    assert_value_where(table, "name", "Plasma Cutter Pro", "id", 10)
    assert_value_where(table, "price", 399.99, "id", 10)

    # Deleted ids must be absent
    assert_count_where(table, "id", 2, 0)
    assert_count_where(table, "id", 5, 0)
    assert_count_where(table, "id", 8, 0)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-puffin-dv demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing puffin_dv_products/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 UniForm Puffin Deletion Vectors -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "puffin_dv_products")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_puffin_dv_products(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
