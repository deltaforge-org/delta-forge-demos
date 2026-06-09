#!/usr/bin/env python3
"""
Iceberg V3 Clinical Trials Lab Results -- Data Verification
=============================================================
Reads the lab_results table through Iceberg metadata after seeding 480
lab result rows across 3 sites, 4 tests, and 5 analysts.

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
    assert_row_count, assert_count_where, assert_distinct_count,
    assert_format_version)
from verify_lib import print_header, print_section, print_summary, exit_with_status


def verify_lab_results(data_root, verbose=False):
    import pyarrow.compute as pc

    print_section("lab_results -- Clinical Trials V3")

    table_path = os.path.join(data_root, "iceberg_warehouse", "trials", "lab_results")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    # Format version
    assert_format_version(metadata, 3)

    # Total rows
    assert_row_count(table, 480)

    # Per-site counts (160 each)
    assert_count_where(table, "site", "Boston-MGH", 160)
    assert_count_where(table, "site", "Houston-MD", 160)
    assert_count_where(table, "site", "Seattle-UW", 160)

    # Per-test counts (120 each)
    assert_count_where(table, "test_name", "ALT", 120)
    assert_count_where(table, "test_name", "Creatinine", 120)
    assert_count_where(table, "test_name", "Hemoglobin", 120)
    assert_count_where(table, "test_name", "Platelet-Count", 120)

    # Abnormal counts
    total_abnormal = pc.sum(pc.equal(table.column("is_abnormal"), True)).as_py()
    if total_abnormal == 84:
        ok(f"Total abnormal = 84")
    else:
        fail(f"Total abnormal = {total_abnormal}, expected 84")

    above_range = pc.sum(pc.greater(table.column("result_value"),
                                      table.column("reference_high"))).as_py()
    if above_range == 31:
        ok(f"Above range = 31")
    else:
        fail(f"Above range = {above_range}, expected 31")

    below_range = pc.sum(pc.less(table.column("result_value"),
                                  table.column("reference_low"))).as_py()
    if below_range == 53:
        ok(f"Below range = 53")
    else:
        fail(f"Below range = {below_range}, expected 53")

    # Distinct counts
    assert_distinct_count(table, "patient_id", 173)
    assert_distinct_count(table, "analyst", 5)

    # Per-test average result_value
    for test_name, expected_avg in [
        ("ALT", 29.23),
        ("Creatinine", 0.89),
        ("Hemoglobin", 14.71),
        ("Platelet-Count", 243.14),
    ]:
        mask = pc.equal(table.column("test_name"), test_name)
        filtered = table.filter(mask)
        actual_avg = round(pc.mean(filtered.column("result_value")).as_py(), 2)
        if actual_avg == expected_avg:
            ok(f"Avg {test_name} = {expected_avg}")
        else:
            fail(f"Avg {test_name} = {actual_avg}, expected {expected_avg}")

    # Per-test abnormal counts
    for test_name, expected_abn in [
        ("ALT", 15),
        ("Creatinine", 38),
        ("Hemoglobin", 12),
        ("Platelet-Count", 19),
    ]:
        mask = pc.and_(
            pc.equal(table.column("test_name"), test_name),
            pc.equal(table.column("is_abnormal"), True),
        )
        actual_abn = pc.sum(mask).as_py()
        if actual_abn == expected_abn:
            ok(f"Abnormal {test_name} = {expected_abn}")
        else:
            fail(f"Abnormal {test_name} = {actual_abn}, expected {expected_abn}")

    # Per-site abnormal counts
    for site, expected_abn in [
        ("Boston-MGH", 28),
        ("Houston-MD", 29),
        ("Seattle-UW", 27),
    ]:
        mask = pc.and_(
            pc.equal(table.column("site"), site),
            pc.equal(table.column("is_abnormal"), True),
        )
        actual_abn = pc.sum(mask).as_py()
        if actual_abn == expected_abn:
            ok(f"Abnormal at {site} = {expected_abn}")
        else:
            fail(f"Abnormal at {site} = {actual_abn}, expected {expected_abn}")

    # Analyst counts
    for analyst, expected_count in [
        ("Dr. Chen", 90),
        ("Dr. Kovacs", 100),
        ("Dr. Okafor", 99),
        ("Dr. Patel", 100),
        ("Dr. Reyes", 91),
    ]:
        assert_count_where(table, "analyst", analyst, expected_count)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-v3-clinical-trials demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing iceberg_warehouse/trials/lab_results/"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg V3 Clinical Trials -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "iceberg_warehouse", "trials", "lab_results")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_lab_results(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
