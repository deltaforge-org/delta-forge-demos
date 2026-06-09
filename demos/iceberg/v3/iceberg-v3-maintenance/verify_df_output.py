#!/usr/bin/env python3
"""
Iceberg V3 UniForm SaaS Billing OPTIMIZE & VACUUM -- Data Verification
========================================================================
Reads the subscriptions table through Iceberg metadata after seed (30),
two INSERT batches (+15, +15), OPTIMIZE, DELETE trial/suspended, and VACUUM.
Final state: 50 active subscriptions.

Usage:
    python verify.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_avg, assert_count_where,
    assert_distinct_count, assert_value_where)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def assert_count_where_in(table, filter_col, filter_vals, expected, label=""):
    import pyarrow.compute as pc
    mask = pc.is_in(table.column(filter_col), value_set=filter_vals)
    actual = pc.sum(mask).as_py()
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"COUNT WHERE {filter_col} IN ... = {expected}{ctx}")
    else:
        fail(f"COUNT WHERE {filter_col} IN ... = {actual}, expected {expected}{ctx}")


def verify_subscriptions(data_root, verbose=False):
    import pyarrow as pa
    import pyarrow.compute as pc

    print_section("subscriptions -- Post-OPTIMIZE/DELETE/VACUUM")

    table_path = os.path.join(data_root, "subscriptions")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # 60 total - 10 (trial/suspended) = 50
    assert_row_count(table, 50)

    # Only active status remains
    assert_count_where(table, "status", "active", 50)
    assert_count_where(table, "status", "trial", 0)
    assert_count_where(table, "status", "suspended", 0)

    # Per-plan counts
    assert_count_where(table, "plan_tier", "enterprise", 17)
    assert_count_where(table, "plan_tier", "pro", 17)
    assert_count_where(table, "plan_tier", "startup", 16)

    # MRR totals
    assert_sum(table, "mrr", 22349.5)
    assert_avg(table, "mrr", 446.99)

    # Distinct plan count
    assert_distinct_count(table, "plan_tier", 3)

    # Spot checks from different batches
    assert_value_where(table, "company", "Acme Corp", "sub_id", 1)
    assert_value_where(table, "plan_tier", "enterprise", "sub_id", 1)
    assert_value_where(table, "mrr", 499.99, "sub_id", 1)
    assert_value_where(table, "status", "active", "sub_id", 1)
    assert_value_where(table, "billing_cycle", "annual", "sub_id", 1)

    # Batch 2 spot check
    assert_value_where(table, "company", "SynapseDB", "sub_id", 45)
    assert_value_where(table, "plan_tier", "pro", "sub_id", 45)
    assert_value_where(table, "mrr", 1199.99, "sub_id", 45)

    # Batch 3 spot check
    assert_value_where(table, "company", "HorizonOps", "sub_id", 60)
    assert_value_where(table, "plan_tier", "pro", "sub_id", 60)
    assert_value_where(table, "mrr", 1199.99, "sub_id", 60)

    # Deleted rows are absent (trial/suspended sub_ids)
    # trial: 8, 16, 26, 33, 44, 49; suspended: 12, 20, 38, 54
    deleted_ids = pa.array([8, 16, 26, 33, 44, 49, 12, 20, 38, 54])
    assert_count_where_in(table, "sub_id", deleted_ids, 0, "trial/suspended deleted")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-maintenance demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing subscriptions/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Maintenance -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "subscriptions")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_subscriptions(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
