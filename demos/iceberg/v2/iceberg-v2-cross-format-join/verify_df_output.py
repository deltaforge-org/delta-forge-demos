#!/usr/bin/env python3
"""
Iceberg Cross-Format Join — Retail Store Analytics — Data Verification
=======================================================================
Reads the sales table through the Iceberg metadata chain (written via
UniForm) and verifies 40 retail transactions match expected values.

NOTE: This demo creates the Delta+Iceberg table at runtime via setup.sql.
The verify.py reads the Iceberg metadata produced by the UniForm writer
at the data_path/sales location. The CSV store data is not verified here
since it is a static external file.

Usage:
    python verify.py <data_root_path> [--verbose]

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
    reset_counters,
)
from verify_lib import print_header, print_summary, exit_with_status


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v2-cross-format-join demo"
    )
    parser.add_argument("data_root", help="Parent folder containing sales/ (the UniForm Delta+Iceberg table)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show sample data")
    args = parser.parse_args()

    import pyarrow.compute as pc

    data_root = os.path.abspath(args.data_root)
    table_path = os.path.join(data_root, "sales")

    print_header("Cross-Format Join (Sales) — Data Verification")
    print(f"  Data root: {data_root}")

    if not os.path.isdir(os.path.join(table_path, "metadata")):
        print(f"\nError: {table_path}/metadata not found")
        print(f"  This demo creates the sales table at runtime via setup.sql.")
        print(f"  Run the demo first, then run verify.py against the data_path.")
        sys.exit(1)

    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg metadata chain")

    if args.verbose:
        info(f"Columns: {table.column_names}")

    assert_row_count(table, 40)

    # Total revenue = quantity * unit_price
    revenue_values = [
        round(table.column("quantity")[i].as_py() * table.column("unit_price")[i].as_py(), 2)
        for i in range(table.num_rows)
    ]
    total_revenue = round(sum(revenue_values), 2)
    if total_revenue == 14828.73:
        ok(f"Total revenue = 14828.73")
    else:
        fail(f"Total revenue = {total_revenue}, expected 14828.73")

    # Total quantity
    assert_sum(table, "quantity", 127.0, label="total quantity")

    # Distinct stores
    assert_distinct_count(table, "store_id", 10)

    # Per-store counts (4 transactions each)
    for store in [f"S{i:03d}" for i in range(1, 11)]:
        assert_count_where(table, "store_id", store, 4)

    # Category distribution
    assert_distinct_count(table, "category", 3)
    assert_count_where(table, "category", "Shoes", 16)
    assert_count_where(table, "category", "Apparel", 12)
    assert_count_where(table, "category", "Accessories", 12)

    # Spot check txn_id=1
    txn1 = table.filter(pc.equal(table.column("txn_id"), 1))
    if txn1.num_rows > 0:
        store = txn1.column("store_id")[0].as_py()
        prod = txn1.column("product_name")[0].as_py()
        qty = txn1.column("quantity")[0].as_py()
        price = txn1.column("unit_price")[0].as_py()
        if store == "S001":
            ok("txn_id=1 store_id = 'S001'")
        else:
            fail(f"txn_id=1 store_id = {store!r}, expected 'S001'")
        if prod == "Running Pro X":
            ok("txn_id=1 product_name = 'Running Pro X'")
        else:
            fail(f"txn_id=1 product_name = {prod!r}, expected 'Running Pro X'")
        if qty == 3:
            ok("txn_id=1 quantity = 3")
        else:
            fail(f"txn_id=1 quantity = {qty}, expected 3")
        if round(price, 2) == 129.99:
            ok("txn_id=1 unit_price = 129.99")
        else:
            fail(f"txn_id=1 unit_price = {price}, expected 129.99")

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
