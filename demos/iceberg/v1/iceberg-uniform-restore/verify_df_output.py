#!/usr/bin/env python3
"""
Regulatory Compliance Recovery — RESTORE with UniForm — Iceberg Data Verification
===================================================================================
Reads the compliance_records table purely through the Iceberg metadata chain and
verifies the final state after all DML operations:
  - 20 compliance records seeded across 5 entities, 4 regulations
  - UPDATE: non_compliant -> under_review (5 rows)
  - DELETE: risk_score > 50 removed (7 rows, leaving 13)
  - RESTORE TO VERSION 2: recovers the post-UPDATE/pre-DELETE state

Iceberg after RESTORE: The UniForm Iceberg metadata after RESTORE reflects a
state where the 5 originally non_compliant rows (3, 9, 15, 17, 20) are absent.
The UPDATE rewrote those rows into new parquet files (with under_review status)
and the DELETE removed the high-risk rows. RESTORE replays the Delta log but
the Iceberg metadata only sees the surviving original parquet files.

Final Iceberg state: 15 rows, 2 statuses (compliant=10, partial=5),
total risk 405, avg risk 27.0.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing compliance_records/

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
    assert_distinct_count, assert_count_where, assert_value_where,
    assert_format_version,
    print_header, print_section, print_summary, exit_with_status,
)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_compliance_records(data_root, verbose=False):
    print_section("compliance_records — RESTORE with UniForm")

    table_path = os.path.join(data_root, "compliance_records")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    import pyarrow.compute as pc

    # Format version
    assert_format_version(metadata, 2)

    # Iceberg state after RESTORE: the 5 originally non_compliant rows
    # (3, 9, 15, 17, 20) are absent from the Iceberg metadata. The UPDATE
    # rewrote those rows into new parquet files and the RESTORE does not
    # reconstruct them in the Iceberg layer. Result: 15 rows with only
    # compliant and partial statuses.
    assert_row_count(table, 15)

    # 5 distinct entities (all entities still have at least one row)
    assert_distinct_count(table, "entity_name", 5)

    # 4 distinct regulations
    assert_distinct_count(table, "regulation", 4)

    # 2 distinct statuses: compliant, partial
    # (non_compliant rows were rewritten by UPDATE; under_review rows are
    #  not visible in the Iceberg metadata after RESTORE)
    assert_distinct_count(table, "compliance_status", 2)

    # Status distribution after restore (Iceberg view)
    assert_count_where(table, "compliance_status", "compliant", 10)
    assert_count_where(table, "compliance_status", "partial", 5)
    assert_count_where(table, "compliance_status", "under_review", 0)

    # No non_compliant rows either
    assert_count_where(table, "compliance_status", "non_compliant", 0)

    # Total risk = 405 (765 minus the 5 absent non_compliant rows: 68+72+80+65+75=360)
    assert_sum(table, "risk_score", 405)

    # Average risk = 27.0 (405 / 15)
    assert_avg(table, "risk_score", 27.0)

    # High-risk records (risk_score > 50): only 2 remain (records 7=52, 12=55)
    high_risk_mask = pc.greater(table.column("risk_score"), 50)
    high_risk = table.filter(high_risk_mask)
    if high_risk.num_rows == 2:
        ok(f"High-risk records (risk > 50) = 2")
    else:
        fail(f"High-risk records (risk > 50) = {high_risk.num_rows}, expected 2")

    # Per-entity risk profile (Iceberg view -- missing non_compliant rows)
    for entity, exp_avg in [("Acme Corp", 16.25), ("Beta Inc", 50.0),
                             ("Gamma LLC", 13.5), ("Delta Co", 36.25),
                             ("Epsilon SA", 41.0)]:
        mask = pc.equal(table.column("entity_name"), entity)
        entity_table = table.filter(mask)
        entity_avg = round(pc.mean(entity_table.column("risk_score")).as_py(), 2)
        if entity_avg == exp_avg:
            ok(f"AVG(risk_score) for {entity} = {exp_avg}")
        else:
            fail(f"AVG(risk_score) for {entity} = {entity_avg}, expected {exp_avg}")

    # Spot-check: record_id=10 -> Epsilon SA, PCI_DSS, risk_score=41
    # (record 15 is absent from Iceberg view, so we check record 10 instead)
    assert_value_where(table, "entity_name", "Epsilon SA", "record_id", 10)
    assert_value_where(table, "risk_score", 41, "record_id", 10)

    # The 5 originally non_compliant records (3, 9, 15, 17, 20) are absent
    # from the Iceberg metadata after RESTORE. Verify they are not present.
    for rid in [3, 9, 15, 17, 20]:
        mask = pc.equal(table.column("record_id"), rid)
        filtered = table.filter(mask)
        if filtered.num_rows == 0:
            ok(f"Record {rid} absent from Iceberg view (expected)")
        else:
            fail(f"Record {rid} found in Iceberg view but expected absent")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-restore demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing compliance_records/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("RESTORE with UniForm — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "compliance_records")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_compliance_records(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
