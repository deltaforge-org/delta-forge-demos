#!/usr/bin/env python3
"""
E-Commerce Order Lifecycle — Change Data Feed with UniForm — Iceberg Data Verification
========================================================================================
Reads the orders table purely through the Iceberg metadata chain and verifies
the final state after all DML operations:
  - 30 orders seeded across 5 customers, 5 products, 5 statuses
  - UPDATE: orders 1-5 status changed from 'pending' to 'processing'
  - DELETE: 2 cancelled orders removed (ids 25, 26)

Final state: 28 rows, 4 statuses (no cancelled), total revenue 14269.51.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing orders/

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
    assert_distinct_count, assert_count_where, assert_value_where,
    assert_format_version,
    print_header, print_section, print_summary, exit_with_status,
)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_orders(data_root, verbose=False):
    print_section("orders — Change Data Feed with UniForm")

    table_path = os.path.join(data_root, "orders")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    import pyarrow.compute as pc

    # Format version
    assert_format_version(metadata, 1)

    # Final state: 28 rows (30 seeded - 2 cancelled deleted)
    assert_row_count(table, 28)

    # 5 distinct customers remain
    assert_distinct_count(table, "customer_name", 5)

    # 5 distinct products
    assert_distinct_count(table, "product", 5)

    # 4 statuses (no cancelled)
    assert_distinct_count(table, "status", 4)

    # No cancelled orders
    assert_count_where(table, "status", "cancelled", 0)

    # Post-mutation status distribution:
    # pending=5, processing=12, shipped=6, delivered=5
    assert_count_where(table, "status", "pending", 5)
    assert_count_where(table, "status", "processing", 12)
    assert_count_where(table, "status", "shipped", 6)
    assert_count_where(table, "status", "delivered", 5)

    # Spot-check: order_id=1 was updated from pending to processing
    assert_value_where(table, "status", "processing", "order_id", 1)
    assert_value_where(table, "customer_name", "Alice Johnson", "order_id", 1)
    assert_value_where(table, "unit_price", 1299.99, "order_id", 1)
    assert_value_where(table, "product", "Laptop Pro", "order_id", 1)

    # Revenue by product (quantity * unit_price)
    # Compute total revenue
    qty = pc.cast(table.column("quantity"), "float64")
    price = pc.cast(table.column("unit_price"), "float64")
    revenue_arr = pc.multiply(qty, price)

    total_revenue = round(pc.sum(revenue_arr).as_py(), 2)
    if total_revenue == 14269.51:
        ok(f"Total revenue = 14269.51")
    else:
        fail(f"Total revenue = {total_revenue}, expected 14269.51")

    # Average order value = 509.63
    avg_revenue = round(pc.mean(revenue_arr).as_py(), 2)
    if avg_revenue == 509.63:
        ok(f"AVG order value = 509.63")
    else:
        fail(f"AVG order value = {avg_revenue}, expected 509.63")

    # Revenue by product
    for product, expected_rev in [("Laptop Pro", 9099.93), ("Monitor 27in", 2799.93),
                                   ("Keyboard Mech", 1349.91), ("USB-C Hub", 599.88),
                                   ("Wireless Mouse", 419.86)]:
        mask = pc.equal(table.column("product"), product)
        filtered = table.filter(mask)
        f_qty = pc.cast(filtered.column("quantity"), "float64")
        f_price = pc.cast(filtered.column("unit_price"), "float64")
        f_rev = round(pc.sum(pc.multiply(f_qty, f_price)).as_py(), 2)
        if f_rev == expected_rev:
            ok(f"Revenue for {product} = {expected_rev}")
        else:
            fail(f"Revenue for {product} = {f_rev}, expected {expected_rev}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-change-data-feed demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing orders/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Change Data Feed with UniForm — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "orders")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_orders(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
