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

Final state: 20 rows, 3 statuses (compliant=10, partial=5, under_review=5),
total risk 765, avg risk 38.25.

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
    assert_format_version(metadata, 1)

    # Final state after RESTORE TO VERSION 2: 20 rows
    # (Version 2 = after UPDATE non_compliant->under_review, before DELETE)
    assert_row_count(table, 20)

    # 5 distinct entities
    assert_distinct_count(table, "entity_name", 5)

    # 4 distinct regulations
    assert_distinct_count(table, "regulation", 4)

    # 3 distinct statuses: compliant, partial, under_review
    # (non_compliant was updated to under_review; RESTORE went to version 2)
    assert_distinct_count(table, "compliance_status", 3)

    # Status distribution after restore
    assert_count_where(table, "compliance_status", "compliant", 10)
    assert_count_where(table, "compliance_status", "partial", 5)
    assert_count_where(table, "compliance_status", "under_review", 5)

    # No non_compliant rows (they were updated before restore point)
    assert_count_where(table, "compliance_status", "non_compliant", 0)

    # Total risk = 765
    assert_sum(table, "risk_score", 765)

    # Average risk = 38.25
    assert_avg(table, "risk_score", 38.25)

    # High-risk records (risk_score > 50) are back after restore: 7 rows
    high_risk_mask = pc.greater(table.column("risk_score"), 50)
    high_risk = table.filter(high_risk_mask)
    if high_risk.num_rows == 7:
        ok(f"High-risk records (risk > 50) = 7")
    else:
        fail(f"High-risk records (risk > 50) = {high_risk.num_rows}, expected 7")

    # Per-entity risk profile
    for entity, exp_avg in [("Acme Corp", 16.25), ("Beta Inc", 58.25),
                             ("Gamma LLC", 13.50), ("Delta Co", 36.25),
                             ("Epsilon SA", 67.00)]:
        mask = pc.equal(table.column("entity_name"), entity)
        entity_table = table.filter(mask)
        entity_avg = round(pc.mean(entity_table.column("risk_score")).as_py(), 2)
        if entity_avg == exp_avg:
            ok(f"AVG(risk_score) for {entity} = {exp_avg}")
        else:
            fail(f"AVG(risk_score) for {entity} = {entity_avg}, expected {exp_avg}")

    # Spot-check: record_id=15 -> Epsilon SA, HIPAA, risk_score=80
    assert_value_where(table, "entity_name", "Epsilon SA", "record_id", 15)
    assert_value_where(table, "risk_score", 80, "record_id", 15)

    # The 5 originally non_compliant records should now be under_review
    # Records 3, 9, 15, 17, 20 had non_compliant status originally
    for rid in [3, 9, 15, 17, 20]:
        assert_value_where(table, "compliance_status", "under_review", "record_id", rid,
                           label=f"record {rid} updated to under_review")


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
