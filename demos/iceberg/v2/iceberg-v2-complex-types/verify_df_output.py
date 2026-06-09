#!/usr/bin/env python3
"""
Iceberg V2 Complex Types -- Data Verification
===============================================
Reads the nested_orders table through the Iceberg metadata chain and verifies
100 nested_orders with nested STRUCT and ARRAY<STRUCT> columns.

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
    assert_row_count, assert_sum,
    assert_distinct_count, assert_count_where,
    assert_format_version,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_nested_orders(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("nested_orders -- Complex Types")

    table_path = os.path.join(data_root, "nested_orders")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 100)

    # Sum of order_total
    assert_sum(table, "order_total", 111547.7, label="sum_order_total")

    # Status distribution
    assert_distinct_count(table, "status", 4)
    assert_count_where(table, "status", "Cancelled", 6)
    assert_count_where(table, "status", "Delivered", 43)
    assert_count_where(table, "status", "Processing", 15)
    assert_count_where(table, "status", "Shipped", 36)

    # Nested struct: shipping_address.city distinct count
    # In Arrow, struct sub-fields are accessed via .field("sub_field")
    try:
        addr_col = table.column("shipping_address")
        city_array = pc.struct_field(addr_col, "city")
        distinct_cities = pc.count_distinct(city_array).as_py()
        if distinct_cities == 15:
            ok(f"Distinct shipping cities = 15")
        else:
            fail(f"Distinct shipping cities = {distinct_cities}, expected 15")
    except Exception as e:
        # Fallback: if struct is flattened into dotted column names
        if "shipping_address.city" in table.column_names:
            assert_distinct_count(table, "shipping_address.city", 15)
        else:
            fail(f"Could not access shipping_address.city: {e}")

    # Total items across all nested_orders (sum of list lengths in items column)
    try:
        items_col = table.column("items")
        total_items = sum(len(items_col[i].as_py()) for i in range(table.num_rows))
        if total_items == 311:
            ok(f"Total items (sum of array lengths) = 311")
        else:
            fail(f"Total items = {total_items}, expected 311")
    except Exception as e:
        fail(f"Could not compute total items from items array: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-complex-types demo"
    )
    parser.add_argument("data_root", help="Root path containing nested_orders/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V2 Complex Types -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "nested_orders")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_nested_orders(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
