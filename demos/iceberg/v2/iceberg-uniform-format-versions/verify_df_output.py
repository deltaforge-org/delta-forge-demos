#!/usr/bin/env python3
"""
Iceberg UniForm Format Versions — Data Verification
=====================================================
Reads each table (sensors_v1, sensors_v2, sensors_v3) purely through the
Iceberg metadata chain and verifies the data matches expected values.

This script does NOT read the Delta transaction log. It reads ONLY through
Iceberg metadata → manifest list → manifest → Parquet (with field-ID column
mapping), proving that an external Iceberg engine would see the correct data.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing sensors_v1/, sensors_v2/, sensors_v3/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (
    read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_avg, assert_distinct_count,
    assert_count_where, assert_format_version,
    reset_counters,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status


# ---------------------------------------------------------------------------
# Per-table verification
# ---------------------------------------------------------------------------
def verify_sensors_v1(data_root, verbose=False):
    print_section("sensors_v1 — Iceberg V1 Format")

    table_path = os.path.join(data_root, "sensors_v1")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    # Format version
    assert_format_version(metadata, 1)

    # Row count
    assert_row_count(table, 12)

    # Aggregates
    assert_sum(table, "temperature", 266.5)
    assert_sum(table, "humidity", 571.5)
    assert_avg(table, "temperature", 22.21)

    # Distinct values
    assert_distinct_count(table, "location", 3)
    assert_distinct_count(table, "sensor_id", 4)
    assert_distinct_count(table, "status", 3)

    # Per-location averages (matching queries.sql assertions)
    import pyarrow.compute as pc
    for loc, expected_avg in [("Lab-A", 23.08), ("Lab-B", 21.12), ("Lab-C", 22.43)]:
        mask = pc.equal(table.column("location"), loc)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("temperature")).as_py(), 2)
        if actual == expected_avg:
            ok(f"AVG(temperature) WHERE location={loc!r} = {expected_avg}")
        else:
            fail(f"AVG(temperature) WHERE location={loc!r} = {actual}, expected {expected_avg}")

    # Status distribution
    assert_count_where(table, "status", "normal", 9)
    assert_count_where(table, "status", "warning", 2)
    assert_count_where(table, "status", "critical", 1)


def verify_sensors_v2(data_root, verbose=False):
    print_section("sensors_v2 — Iceberg V2 Format (post schema evolution)")

    table_path = os.path.join(data_root, "sensors_v2")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 2)
    assert_row_count(table, 12)
    assert_sum(table, "temperature", 266.5)
    assert_sum(table, "humidity", 571.5)
    assert_avg(table, "temperature", 22.21)

    assert_distinct_count(table, "location", 3)
    assert_distinct_count(table, "status", 3)

    # Per-location averages
    import pyarrow.compute as pc
    for loc, expected_avg in [("Lab-A", 23.08), ("Lab-B", 21.12), ("Lab-C", 22.43)]:
        mask = pc.equal(table.column("location"), loc)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("temperature")).as_py(), 2)
        if actual == expected_avg:
            ok(f"AVG(temperature) WHERE location={loc!r} = {expected_avg}")
        else:
            fail(f"AVG(temperature) WHERE location={loc!r} = {actual}, expected {expected_avg}")

    assert_count_where(table, "status", "normal", 9)
    assert_count_where(table, "status", "warning", 2)
    assert_count_where(table, "status", "critical", 1)

    # Schema evolution: calibration_offset column should exist after ALTER TABLE
    if "calibration_offset" in table.column_names:
        ok("calibration_offset column present (schema evolution)")
        # Verify calibrated averages per location (matching queries.sql Query 8)
        non_null = pc.is_valid(table.column("calibration_offset"))
        calibrated = table.filter(non_null)
        # Note: Python round() uses banker's rounding (20.825→20.82),
        # SQL ROUND uses half-up (20.825→20.83). Use Python's values here.
        for loc, expected_cal in [("Lab-A", 23.58), ("Lab-B", 20.82), ("Lab-C", 22.62)]:
            mask = pc.equal(calibrated.column("location"), loc)
            filtered = calibrated.filter(mask)
            temps = pc.add(filtered.column("temperature"), filtered.column("calibration_offset"))
            actual = round(pc.mean(temps).as_py(), 2)
            if actual == expected_cal:
                ok(f"AVG(temp+offset) WHERE location={loc!r} = {expected_cal}")
            else:
                fail(f"AVG(temp+offset) WHERE location={loc!r} = {actual}, expected {expected_cal}")
    else:
        info("calibration_offset column not present (initial state, no schema evolution yet)")


def verify_sensors_v3(data_root, verbose=False):
    print_section("sensors_v3 — Iceberg V3 Format (post UPDATE)")

    table_path = os.path.join(data_root, "sensors_v3")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")

    assert_format_version(metadata, 3)
    assert_row_count(table, 12)

    # Post-UPDATE: id=8 temperature changed from 26.3 to 22.0
    # New sum = 266.5 - 26.3 + 22.0 = 262.2, avg = 262.2/12 = 21.85
    assert_sum(table, "temperature", 262.2)
    assert_sum(table, "humidity", 571.5)
    assert_avg(table, "temperature", 21.85)

    assert_distinct_count(table, "location", 3)
    # Post-UPDATE: statuses are now normal(9), warning(2), corrected(1) — no critical
    assert_distinct_count(table, "status", 3)

    import pyarrow.compute as pc
    # Lab-B avg changed: (19.4 + 20.1 + 18.7 + 22.0) / 4 = 20.05
    for loc, expected_avg in [("Lab-A", 23.08), ("Lab-B", 20.05), ("Lab-C", 22.43)]:
        mask = pc.equal(table.column("location"), loc)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("temperature")).as_py(), 2)
        if actual == expected_avg:
            ok(f"AVG(temperature) WHERE location={loc!r} = {expected_avg}")
        else:
            fail(f"AVG(temperature) WHERE location={loc!r} = {actual}, expected {expected_avg}")

    assert_count_where(table, "status", "normal", 9)
    assert_count_where(table, "status", "warning", 2)
    assert_count_where(table, "status", "corrected", 1)


# ---------------------------------------------------------------------------
# Cross-table verification
# ---------------------------------------------------------------------------
def verify_cross_table(data_root, verbose=False):
    print_section("Cross-Table Parity — V1 vs V2 vs V3")

    import pyarrow.compute as pc

    tables = {}
    for version in ["v1", "v2", "v3"]:
        table_path = os.path.join(data_root, f"sensors_{version}")
        t, _ = read_iceberg_table(table_path)
        tables[version] = t

    # All tables should have the same row count
    counts = {v: t.num_rows for v, t in tables.items()}
    if len(set(counts.values())) == 1:
        ok(f"All tables have {counts['v1']} rows")
    else:
        fail(f"Row count mismatch: {counts}")

    # V1 and V2 unchanged (266.5), V3 post-UPDATE (262.2)
    sums = {v: round(pc.sum(t.column("temperature")).as_py(), 2) for v, t in tables.items()}
    if sums["v1"] == sums["v2"] == 266.5:
        ok(f"V1 and V2 have SUM(temperature) = {sums['v1']}")
    else:
        fail(f"V1/V2 SUM(temperature) mismatch: v1={sums['v1']}, v2={sums['v2']}, expected 266.5")
    if sums["v3"] == 262.2:
        ok(f"V3 has SUM(temperature) = {sums['v3']} (post-UPDATE)")
    else:
        fail(f"V3 SUM(temperature) = {sums['v3']}, expected 262.2")

    # All tables should have the same humidity sum (UPDATE didn't change humidity)
    h_sums = {v: round(pc.sum(t.column("humidity")).as_py(), 2) for v, t in tables.items()}
    if len(set(h_sums.values())) == 1:
        ok(f"All tables have SUM(humidity) = {h_sums['v1']}")
    else:
        fail(f"SUM(humidity) mismatch: {h_sums}")

    # V1 and V3 have 7 columns, V2 has 8 (calibration_offset added)
    v1_cols = set(tables["v1"].column_names)
    v2_cols = set(tables["v2"].column_names)
    v3_cols = set(tables["v3"].column_names)
    if v1_cols == v3_cols:
        ok(f"V1 and V3 have identical columns: {sorted(v1_cols)}")
    else:
        fail(f"V1 vs V3 column mismatch: V1={sorted(v1_cols)}, V3={sorted(v3_cols)}")
    if v2_cols - v1_cols == {"calibration_offset"}:
        ok("V2 has extra column 'calibration_offset' from schema evolution")
    else:
        fail(f"V2 column diff unexpected: {v2_cols - v1_cols}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-format-versions demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing sensors_v1/, sensors_v2/, sensors_v3/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Format Versions — Data Verification")
    print(f"  Data root: {data_root}")

    # Check all three table dirs exist
    for version in ["sensors_v1", "sensors_v2", "sensors_v3"]:
        tbl_dir = os.path.join(data_root, version)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    verify_sensors_v1(data_root, verbose=args.verbose)
    verify_sensors_v2(data_root, verbose=args.verbose)
    verify_sensors_v3(data_root, verbose=args.verbose)
    verify_cross_table(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
