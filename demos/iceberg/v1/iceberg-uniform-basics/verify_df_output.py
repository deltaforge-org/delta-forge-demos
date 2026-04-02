#!/usr/bin/env python3
"""
Iceberg UniForm Basics — Data Verification
=============================================
Reads the product_catalog table purely through the Iceberg metadata chain
and verifies the final state after all DML operations:
  - 15 products seeded
  - 3 products inserted (ids 16-18)
  - Electronics prices updated (+10%)

Final state: 18 products, 6 per category, Electronics prices 10% higher.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing product_catalog/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_avg, assert_distinct_count,
    assert_count_where, assert_value_where, assert_format_version,
    print_header, print_section, print_summary, exit_with_status)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_product_catalog(data_root, verbose=False):
    print_section("product_catalog — UniForm Basics")

    table_path = os.path.join(data_root, "product_catalog")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    # Format version
    assert_format_version(metadata, 1)

    # Final state: 18 rows (15 original + 3 inserted)
    assert_row_count(table, 18)

    # 6 per category
    assert_count_where(table, "category", "Electronics", 6)
    assert_count_where(table, "category", "Furniture", 6)
    assert_count_where(table, "category", "Audio", 6)
    assert_distinct_count(table, "category", 3)

    # Total stock = 1945
    assert_sum(table, "stock", 1945)

    # Average rating = 4.39
    assert_avg(table, "rating", 4.39)

    # Electronics prices should be 10% higher than original
    # Laptop Pro: 1299.99 * 1.10 = 1429.989 -> 1429.99
    import pyarrow.compute as pc
    assert_value_where(table, "price", 1429.99, "name", "Laptop Pro")
    assert_value_where(table, "price", 32.99, "name", "Wireless Mouse")
    assert_value_where(table, "price", 76.99, "name", "Webcam HD")

    # Furniture and Audio prices should be unchanged
    assert_value_where(table, "price", 549.99, "name", "Standing Desk")
    assert_value_where(table, "price", 249.99, "name", "Headphones Pro")

    # Electronics revenue (price * stock) after 10% increase
    # Calculated from final prices: electronics_revenue = 124018.40
    electronics_mask = pc.equal(table.column("category"), "Electronics")
    electronics = table.filter(electronics_mask)
    elec_revenue = round(sum(
        float(electronics.column("price")[i].as_py()) * float(electronics.column("stock")[i].as_py())
        for i in range(electronics.num_rows)
    ), 2)
    if elec_revenue == 124018.40:
        ok(f"Electronics revenue = 124018.40")
    else:
        fail(f"Electronics revenue = {elec_revenue}, expected 124018.40")

    # New products exist
    assert_value_where(table, "category", "Electronics", "name", "Webcam HD")
    assert_value_where(table, "category", "Furniture", "name", "Cable Management")
    assert_value_where(table, "category", "Audio", "name", "DAC Amplifier")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-basics demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing product_catalog/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Basics — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "product_catalog")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_product_catalog(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
