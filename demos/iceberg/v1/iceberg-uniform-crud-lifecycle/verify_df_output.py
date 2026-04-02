#!/usr/bin/env python3
"""
Iceberg UniForm CRUD Lifecycle — Data Verification
=====================================================
Reads the employees table purely through the Iceberg metadata chain
and verifies the final state after the full CRUD lifecycle:
  - 20 employees seeded (V1)
  - Engineering salaries +15% (V2)
  - Deactivate ids 8, 13 (V3), then DELETE inactive (V4)
  - Insert 3 new hires ids 21-23 (V5)

Final state: 21 employees, all active, across 4 departments.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing employees/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_distinct_count,
    assert_count_where, assert_value_where, assert_format_version,
    print_header, print_section, print_summary, exit_with_status)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_employees(data_root, verbose=False):
    print_section("employees — CRUD Lifecycle")

    table_path = os.path.join(data_root, "employees")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    assert_format_version(metadata, 1)

    # Final state: 21 employees (20 - 2 deleted + 3 new hires)
    assert_row_count(table, 21)

    # All active
    assert_count_where(table, "is_active", True, 21, "all employees active")

    # 4 departments
    assert_distinct_count(table, "department", 4)

    # Department counts: Engineering=6, Sales=5, Marketing=5, Finance=5
    assert_count_where(table, "department", "Engineering", 6)
    assert_count_where(table, "department", "Sales", 5)
    assert_count_where(table, "department", "Marketing", 5)
    assert_count_where(table, "department", "Finance", 5)

    # Engineering salaries after 15% raise
    # Alice Chen: 135000 * 1.15 = 155250.00
    # Bob Martinez: 155000 * 1.15 = 178250.00
    # Carol Wang: 170000 * 1.15 = 195500.00
    # David Kim: 95000 * 1.15 = 109250.00
    # Eve Johnson: 190000 * 1.15 = 218500.00
    # Wendy Chang (new hire): 145000.00
    assert_value_where(table, "salary", 155250.00, "name", "Alice Chen")
    assert_value_where(table, "salary", 178250.00, "name", "Bob Martinez")
    assert_value_where(table, "salary", 195500.00, "name", "Carol Wang")
    assert_value_where(table, "salary", 109250.00, "name", "David Kim")
    assert_value_where(table, "salary", 218500.00, "name", "Eve Johnson")
    assert_value_where(table, "salary", 145000.00, "name", "Wendy Chang")

    # New hires exist
    assert_value_where(table, "department", "Sales", "name", "Uma Foster")
    assert_value_where(table, "department", "Marketing", "name", "Victor Reyes")
    assert_value_where(table, "department", "Engineering", "name", "Wendy Chang")

    # Deleted employees should NOT exist (ids 8=Henry Brown, 13=Mia Patel)
    import pyarrow.compute as pc
    for emp_id in [8, 13]:
        mask = pc.equal(table.column("id"), emp_id)
        count = pc.sum(mask).as_py()
        if count == 0:
            ok(f"Deleted employee id={emp_id} not present")
        else:
            fail(f"Deleted employee id={emp_id} still present ({count} rows)")

    # Total payroll = 2609750.00
    assert_sum(table, "salary", 2609750.00)

    # Engineering avg salary = 166958.33
    eng_mask = pc.equal(table.column("department"), "Engineering")
    eng = table.filter(eng_mask)
    eng_avg = round(pc.mean(eng.column("salary")).as_py(), 2)
    if eng_avg == 166958.33:
        ok(f"AVG(salary) for Engineering = 166958.33")
    else:
        fail(f"AVG(salary) for Engineering = {eng_avg}, expected 166958.33")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-crud-lifecycle demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing employees/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm CRUD Lifecycle — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "employees")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_employees(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
