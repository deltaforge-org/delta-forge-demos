#!/usr/bin/env python3
"""
Iceberg Native Schema Evolution -- Employee Directory -- Data Verification
===========================================================================
Reads the employee_directory table through Iceberg metadata and verifies
360 rows across 5 departments after schema evolution (title added,
location added, dept renamed to department).

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
    assert_format_version, assert_min, assert_max,
    assert_null_count, assert_value_where,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_employee_directory(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("employee_directory -- Schema Evolution")

    table_path = os.path.join(data_root, "employee_directory")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 360)

    # Department distribution: 5 departments, 72 each
    assert_distinct_count(table, "department", 5)
    assert_count_where(table, "department", "Engineering", 72)
    assert_count_where(table, "department", "Finance", 72)
    assert_count_where(table, "department", "HR", 72)
    assert_count_where(table, "department", "Marketing", 72)
    assert_count_where(table, "department", "Sales", 72)

    # Null counts for evolved columns
    assert_null_count(table, "title", 300, label="null_title_count")

    # has_title = 60
    non_null_title = pc.sum(pc.invert(pc.is_null(table.column("title")))).as_py()
    if non_null_title == 60:
        ok("Non-null title count = 60")
    else:
        fail(f"Non-null title count = {non_null_title}, expected 60")

    assert_null_count(table, "location", 300, label="null_location")

    non_null_location = pc.sum(pc.invert(pc.is_null(table.column("location")))).as_py()
    if non_null_location == 60:
        ok("Non-null location count = 60")
    else:
        fail(f"Non-null location count = {non_null_location}, expected 60")

    # Salary statistics
    assert_sum(table, "salary", 35044993.03, label="total_salary")
    assert_avg(table, "salary", 97347.2, label="avg_salary")
    assert_min(table, "salary", 50115.82, label="min_salary")
    assert_max(table, "salary", 149715.19, label="max_salary")

    # Per-department avg salary
    for dept, expected_avg in [
        ("Engineering", 96315.9),
        ("Finance", 94828.12),
        ("HR", 97538.32),
        ("Marketing", 97049.79),
        ("Sales", 101003.89),
    ]:
        mask = pc.equal(table.column("department"), dept)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("salary")).as_py(), 2)
        if actual == expected_avg:
            ok(f"Avg salary for {dept} = {expected_avg}")
        else:
            fail(f"Avg salary for {dept} = {actual}, expected {expected_avg}")

    # Spot checks
    assert_value_where(table, "full_name", "Alice Smith", "emp_id", 1)
    assert_value_where(table, "department", "Engineering", "emp_id", 1)

    assert_value_where(table, "full_name", "Eve Miller", "emp_id", 360)
    assert_value_where(table, "title", "Data Scientist", "emp_id", 360)
    assert_value_where(table, "location", "New York", "emp_id", 360)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-native-schema-evolution demo"
    )
    parser.add_argument("data_root", help="Root path containing employee_directory/")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg Native Schema Evolution -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "employee_directory")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_employee_directory(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
