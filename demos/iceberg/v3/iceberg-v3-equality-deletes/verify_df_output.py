#!/usr/bin/env python3
"""
Iceberg V3 Equality Deletes -- Patient Visits Verification
============================================================
Reads the patient_visits table through Iceberg metadata after seeding 500
rows and applying equality deletes on 4 GDPR patients (55 rows removed).
Final state: 445 patient visit rows.

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
    assert_row_count, assert_sum, assert_avg, assert_count_where,
    assert_distinct_count, assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_patient_visits(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("patient_visits -- Post-Equality Deletes")

    table_path = os.path.join(data_root, "patient_visits")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # Format version
    assert_format_version(metadata, 3)

    # 500 - 55 GDPR deletions = 445
    assert_row_count(table, 445)

    # GDPR patients must be absent
    for pid in ["P-0012", "P-0025", "P-0041", "P-0067"]:
        assert_count_where(table, "patient_id", pid, 0)

    # Per-hospital counts
    assert_count_where(table, "hospital", "Cleveland-Clinic-OH", 96)
    assert_count_where(table, "hospital", "Johns-Hopkins-Baltimore", 80)
    assert_count_where(table, "hospital", "Mass-General-Boston", 93)
    assert_count_where(table, "hospital", "Mayo-Clinic-Rochester", 84)
    assert_count_where(table, "hospital", "Mount-Sinai-NYC", 92)

    # Distinct counts
    assert_distinct_count(table, "patient_id", 75)
    assert_distinct_count(table, "department", 8)

    # Per-department counts
    assert_count_where(table, "department", "Cardiology", 52)
    assert_count_where(table, "department", "Dermatology", 58)
    assert_count_where(table, "department", "Emergency", 50)
    assert_count_where(table, "department", "Neurology", 63)
    assert_count_where(table, "department", "Oncology", 59)
    assert_count_where(table, "department", "Orthopedics", 63)
    assert_count_where(table, "department", "Pediatrics", 55)
    assert_count_where(table, "department", "Radiology", 45)

    # Total emergency visits
    assert_count_where(table, "department", "Emergency", 50)

    # Cost aggregations
    assert_sum(table, "treatment_cost", 5859194.13)
    assert_avg(table, "treatment_cost", 13166.73)

    # Per-hospital average cost
    for hospital, expected_avg in [
        ("Cleveland-Clinic-OH", 11490.21),
        ("Johns-Hopkins-Baltimore", 14292.60),
        ("Mass-General-Boston", 13725.55),
        ("Mayo-Clinic-Rochester", 12337.53),
        ("Mount-Sinai-NYC", 14129.32),
    ]:
        mask = pc.equal(table.column("hospital"), hospital)
        filtered = table.filter(mask)
        actual_avg = round(pc.mean(filtered.column("treatment_cost")).as_py(), 2)
        if actual_avg == expected_avg:
            ok(f"Avg cost {hospital} = {expected_avg}")
        else:
            fail(f"Avg cost {hospital} = {actual_avg}, expected {expected_avg}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-equality-deletes demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing patient_visits/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Equality Deletes -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "patient_visits")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_patient_visits(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
