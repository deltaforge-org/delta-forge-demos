#!/usr/bin/env python3
"""
Iceberg V3 NULL Edge Cases -- Lab Results Verification
========================================================
Reads the null_lab_results table through Iceberg metadata with 50 rows
containing intentional NULLs across multiple columns.

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
    assert_row_count, assert_sum, assert_avg, assert_min, assert_max,
    assert_distinct_count, assert_null_count, assert_count_where,
    assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_lab_results(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("null_lab_results -- NULL Edge Cases")

    table_path = os.path.join(data_root, "null_lab_results")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # Format version
    assert_format_version(metadata, 3)

    # Total rows
    assert_row_count(table, 50)

    # NULL counts per column
    assert_null_count(table, "result_value", 5)
    assert_null_count(table, "unit", 2)
    assert_null_count(table, "reference_low", 3)
    assert_null_count(table, "is_critical", 12)
    assert_null_count(table, "lab_technician", 7)
    assert_null_count(table, "notes", 37)

    # Non-null counts
    non_null_results = pc.sum(pc.is_valid(table.column("result_value"))).as_py()
    if non_null_results == 45:
        ok(f"Non-null result_value = 45")
    else:
        fail(f"Non-null result_value = {non_null_results}, expected 45")

    non_null_notes = pc.sum(pc.is_valid(table.column("notes"))).as_py()
    if non_null_notes == 13:
        ok(f"Non-null notes = 13")
    else:
        fail(f"Non-null notes = {non_null_notes}, expected 13")

    # Result value aggregations (over non-null values)
    assert_sum(table, "result_value", 4532.72)
    assert_avg(table, "result_value", 100.73)
    assert_min(table, "result_value", 0.1)
    assert_max(table, "result_value", 567.17)

    # Distinct counts
    assert_distinct_count(table, "test_name", 10)
    assert_distinct_count(table, "patient_name", 15)

    # Critical counts: is_critical=1 (true), is_critical=0 (false), IS NULL
    critical_count = pc.sum(pc.equal(table.column("is_critical"), 1)).as_py()
    if critical_count == 24:
        ok(f"Critical count (is_critical=1) = 24")
    else:
        fail(f"Critical count (is_critical=1) = {critical_count}, expected 24")

    normal_count = pc.sum(pc.equal(table.column("is_critical"), 0)).as_py()
    if normal_count == 14:
        ok(f"Normal count (is_critical=0) = 14")
    else:
        fail(f"Normal count (is_critical=0) = {normal_count}, expected 14")

    unknown_count = pc.sum(pc.is_null(table.column("is_critical"))).as_py()
    if unknown_count == 12:
        ok(f"Unknown count (is_critical IS NULL) = 12")
    else:
        fail(f"Unknown count (is_critical IS NULL) = {unknown_count}, expected 12")

    # Automated runs (lab_technician IS NULL)
    automated_runs = pc.sum(pc.is_null(table.column("lab_technician"))).as_py()
    if automated_runs == 7:
        ok(f"Automated runs (lab_technician IS NULL) = 7")
    else:
        fail(f"Automated runs (lab_technician IS NULL) = {automated_runs}, expected 7")

    # Technician counts (including NULL)
    assert_null_count(table, "lab_technician", 7)
    assert_count_where(table, "lab_technician", "Dr. Patel", 9)
    assert_count_where(table, "lab_technician", "Dr. Smith", 9)

    # Per-test has_result (non-null result_value count)
    for test_name, expected_count in [
        ("Hemoglobin", 5),
        ("Glucose", 4),
        ("Platelet Count", 3),
    ]:
        mask = pc.and_(
            pc.equal(table.column("test_name"), test_name),
            pc.is_valid(table.column("result_value")),
        )
        actual_count = pc.sum(mask).as_py()
        if actual_count == expected_count:
            ok(f"Has result for {test_name} = {expected_count}")
        else:
            fail(f"Has result for {test_name} = {actual_count}, expected {expected_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-null-edge-cases demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing null_lab_results/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 NULL Edge Cases -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "null_lab_results")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_lab_results(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
