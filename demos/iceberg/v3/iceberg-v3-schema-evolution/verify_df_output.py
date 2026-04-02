#!/usr/bin/env python3
"""
Iceberg V3 UniForm Pharmaceutical Drug Registry Schema Evolution -- Data Verification
======================================================================================
Reads the drug_registry table through Iceberg metadata after two ADD COLUMN
operations (trial_phase, priority_score), backfill UPDATEs, and 5 new drug
inserts. Final state: 35 drugs with 9 columns.

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
    assert_row_count, assert_avg, assert_count_where, assert_distinct_count,
    assert_value_where)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_drug_registry(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("drug_registry -- Post-Schema-Evolution Final State")

    table_path = os.path.join(data_root, "drug_registry")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # 30 seed + 5 inserted = 35
    assert_row_count(table, 35)

    # Per-category drug counts
    assert_count_where(table, "category", "Cardiology", 9)
    assert_count_where(table, "category", "Immunology", 8)
    assert_count_where(table, "category", "Neurology", 8)
    assert_count_where(table, "category", "Oncology", 10)

    # Distinct categories
    assert_distinct_count(table, "category", 4)

    # Trial phase distribution (5 phases after evolution)
    assert_count_where(table, "trial_phase", "Discontinued", 2)
    assert_count_where(table, "trial_phase", "Phase I", 2)
    assert_count_where(table, "trial_phase", "Phase II", 2)
    assert_count_where(table, "trial_phase", "Phase III", 12)
    assert_count_where(table, "trial_phase", "Phase IV", 17)
    assert_distinct_count(table, "trial_phase", 5)

    # All rows have trial_phase and priority_score populated (no NULLs)
    null_phase = pc.sum(pc.is_null(table.column("trial_phase"))).as_py()
    null_priority = pc.sum(pc.is_null(table.column("priority_score"))).as_py()
    if null_phase == 0:
        ok(f"All 35 rows have trial_phase populated")
    else:
        fail(f"{null_phase} rows have NULL trial_phase, expected 0")
    if null_priority == 0:
        ok(f"All 35 rows have priority_score populated")
    else:
        fail(f"{null_priority} rows have NULL priority_score, expected 0")

    # Average dosage and priority
    assert_avg(table, "dosage_mg", 243.43)
    assert_avg(table, "priority_score", 8.09)

    # Spot-check seed row with evolved columns
    assert_value_where(table, "drug_name", "Oncarex", "drug_id", 1)
    assert_value_where(table, "category", "Oncology", "drug_id", 1)
    assert_value_where(table, "manufacturer", "PharmaCorp", "drug_id", 1)
    assert_value_where(table, "dosage_mg", 250, "drug_id", 1)
    assert_value_where(table, "approval_status", "approved", "drug_id", 1)
    assert_value_where(table, "trial_phase", "Phase IV", "drug_id", 1)
    assert_value_where(table, "priority_score", 9.5, "drug_id", 1)

    # Spot-check post-evolution insert
    assert_value_where(table, "drug_name", "Immutarget", "drug_id", 31)
    assert_value_where(table, "category", "Oncology", "drug_id", 31)
    assert_value_where(table, "dosage_mg", 1200, "drug_id", 31)
    assert_value_where(table, "trial_phase", "Phase II", "drug_id", 31)
    assert_value_where(table, "priority_score", 9.5, "drug_id", 31)

    # Spot-check rejected drug
    assert_value_where(table, "drug_name", "Neurozen", "drug_id", 7)
    assert_value_where(table, "approval_status", "rejected", "drug_id", 7)
    assert_value_where(table, "trial_phase", "Discontinued", "drug_id", 7)
    assert_value_where(table, "priority_score", 7.5, "drug_id", 7)

    # Per-category average dosage
    for cat, expected_avg in [("Cardiology", 103.33), ("Oncology", 532.5)]:
        mask = pc.equal(table.column("category"), cat)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("dosage_mg")).as_py(), 2)
        if actual == expected_avg:
            ok(f"AVG(dosage_mg) WHERE category={cat!r} = {expected_avg}")
        else:
            fail(f"AVG(dosage_mg) WHERE category={cat!r} = {actual}, expected {expected_avg}")

    # Per-category average priority
    for cat, expected_avg in [("Cardiology", 8.0), ("Immunology", 7.0),
                               ("Neurology", 7.5), ("Oncology", 9.5)]:
        mask = pc.equal(table.column("category"), cat)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("priority_score")).as_py(), 2)
        if actual == expected_avg:
            ok(f"AVG(priority_score) WHERE category={cat!r} = {expected_avg}")
        else:
            fail(f"AVG(priority_score) WHERE category={cat!r} = {actual}, expected {expected_avg}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-schema-evolution demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing drug_registry/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Schema Evolution -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "drug_registry")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_drug_registry(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
