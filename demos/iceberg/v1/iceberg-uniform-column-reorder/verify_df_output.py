#!/usr/bin/env python3
"""
Iceberg UniForm Column Reorder — Data Verification
====================================================
Reads the patient_records table through the Iceberg metadata chain and verifies
the final state after column reordering and additional inserts.

Final state: 24 patient records, 12 distinct diagnosis codes, 8 physicians.
Columns have been reordered: mrn, first_name, last_name, record_id, dob,
diagnosis_code, admission_date, discharge_date, attending_physician.

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
    assert_row_count, assert_distinct_count, assert_count_where,
    assert_value_where, assert_format_version,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status
from verify_lib.assertions import CYAN, RESET


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_patient_records(data_root, verbose=False):
    print_section("patient_records — Column Reorder Final State")

    table_path = os.path.join(data_root, "patient_records")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    # Format version (UniForm generates v2)
    assert_format_version(metadata, 2)

    # Final state: 24 rows (20 seed + 4 inserted after reorder)
    assert_row_count(table, 24)

    # Distinct counts
    assert_distinct_count(table, "mrn", 24)
    assert_distinct_count(table, "diagnosis_code", 12)
    assert_distinct_count(table, "attending_physician", 8)

    # Pre-reorder patient spot-checks
    print(f"\n  {CYAN}Pre-reorder patient spot-checks:{RESET}")
    assert_value_where(table, "mrn", "MRN-1001", "record_id", 1)
    assert_value_where(table, "first_name", "John", "record_id", 1)
    assert_value_where(table, "last_name", "Smith", "record_id", 1)
    assert_value_where(table, "dob", "1955-03-12", "record_id", 1)
    assert_value_where(table, "diagnosis_code", "I25.10", "record_id", 1)
    assert_value_where(table, "attending_physician", "Dr. Chen", "record_id", 1)

    assert_value_where(table, "mrn", "MRN-1010", "record_id", 10)
    assert_value_where(table, "first_name", "Susan", "record_id", 10)
    assert_value_where(table, "last_name", "Taylor", "record_id", 10)
    assert_value_where(table, "diagnosis_code", "S72.001", "record_id", 10)
    assert_value_where(table, "attending_physician", "Dr. Kim", "record_id", 10)

    assert_value_where(table, "mrn", "MRN-1020", "record_id", 20)
    assert_value_where(table, "first_name", "Elizabeth", "record_id", 20)
    assert_value_where(table, "last_name", "Clark", "record_id", 20)
    assert_value_where(table, "diagnosis_code", "C50.911", "record_id", 20)
    assert_value_where(table, "attending_physician", "Dr. Okafor", "record_id", 20)

    # Post-reorder insert spot-checks
    print(f"\n  {CYAN}Post-reorder insert spot-checks:{RESET}")
    assert_value_where(table, "mrn", "MRN-1021", "record_id", 21)
    assert_value_where(table, "first_name", "Andrew", "record_id", 21)
    assert_value_where(table, "last_name", "Lee", "record_id", 21)
    assert_value_where(table, "diagnosis_code", "I25.10", "record_id", 21)
    assert_value_where(table, "attending_physician", "Dr. Chen", "record_id", 21)

    assert_value_where(table, "mrn", "MRN-1024", "record_id", 24)
    assert_value_where(table, "first_name", "Margaret", "record_id", 24)
    assert_value_where(table, "last_name", "Allen", "record_id", 24)
    assert_value_where(table, "diagnosis_code", "C18.9", "record_id", 24)
    assert_value_where(table, "attending_physician", "Dr. Reeves", "record_id", 24)

    # Diagnosis code distribution
    print(f"\n  {CYAN}Diagnosis code distribution:{RESET}")
    assert_count_where(table, "diagnosis_code", "I25.10", 3)
    assert_count_where(table, "diagnosis_code", "I48.0", 2)
    assert_count_where(table, "diagnosis_code", "M54.5", 2)
    assert_count_where(table, "diagnosis_code", "G30.9", 2)
    assert_count_where(table, "diagnosis_code", "C18.9", 2)
    assert_count_where(table, "diagnosis_code", "C50.911", 2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-column-reorder demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing patient_records/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Column Reorder — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "patient_records")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_patient_records(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
