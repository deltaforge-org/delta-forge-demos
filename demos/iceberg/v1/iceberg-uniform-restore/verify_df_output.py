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

Iceberg after RESTORE: UniForm regenerates Iceberg metadata on every Delta
commit, including RESTORE commits. After RESTORE TO VERSION 2 the Iceberg
metadata reflects the V2 state — the same 20 rows the Delta reader sees:
10 compliant, 5 under_review (the originally non_compliant rows, now
re-classified), 5 partial. The DELETE's row removals are undone by RESTORE
and the Iceberg manifest chain is rewritten to point back at the V2 active
file set, so the Iceberg view stays in sync with the Delta view.

Final Iceberg state: 20 rows, 3 statuses (compliant=10, under_review=5,
partial=5), total risk 765, avg risk 38.25.

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

    # Iceberg state after RESTORE TO VERSION 2: the table reflects the V2
    # state (20 rows, UPDATE applied, DELETE undone). The UPDATE rewrote
    # the 5 originally non_compliant rows into files tagged under_review;
    # RESTORE brings those files back into the active manifest set.
    assert_row_count(table, 20)

    # 5 distinct entities (all original entities still present)
    assert_distinct_count(table, "entity_name", 5)

    # 4 distinct regulations
    assert_distinct_count(table, "regulation", 4)

    # 3 distinct statuses: compliant, under_review, partial
    # (non_compliant was renamed to under_review by the V2 UPDATE)
    assert_distinct_count(table, "compliance_status", 3)

    # Status distribution after RESTORE to V2 (Iceberg view mirrors Delta view)
    assert_count_where(table, "compliance_status", "compliant", 10)
    assert_count_where(table, "compliance_status", "under_review", 5)
    assert_count_where(table, "compliance_status", "partial", 5)

    # No non_compliant rows — the UPDATE is still in effect after RESTORE
    assert_count_where(table, "compliance_status", "non_compliant", 0)

    # Total risk = 765 (sum of all V1 risk scores; UPDATE doesn't touch risk)
    assert_sum(table, "risk_score", 765)

    # Average risk = 38.25 (765 / 20)
    assert_avg(table, "risk_score", 38.25)

    # High-risk records (risk_score > 50): 7 rows — the 5 originally
    # non_compliant (now under_review) plus records 7 (52) and 12 (55).
    high_risk_mask = pc.greater(table.column("risk_score"), 50)
    high_risk = table.filter(high_risk_mask)
    if high_risk.num_rows == 7:
        ok(f"High-risk records (risk > 50) = 7")
    else:
        fail(f"High-risk records (risk > 50) = {high_risk.num_rows}, expected 7")

    # Per-entity risk profile — all rows present, so V1/V2 averages apply
    for entity, exp_avg in [("Acme Corp", 16.25), ("Beta Inc", 58.25),
                             ("Gamma LLC", 13.5), ("Delta Co", 36.25),
                             ("Epsilon SA", 67.0)]:
        mask = pc.equal(table.column("entity_name"), entity)
        entity_table = table.filter(mask)
        entity_avg = round(pc.mean(entity_table.column("risk_score")).as_py(), 2)
        if entity_avg == exp_avg:
            ok(f"AVG(risk_score) for {entity} = {exp_avg}")
        else:
            fail(f"AVG(risk_score) for {entity} = {entity_avg}, expected {exp_avg}")

    # Spot-check: record_id=15 -> Epsilon SA, HIPAA, risk_score=80.
    # This row was originally non_compliant in V1 and was UPDATEd to
    # under_review in V2, then DELETEd in V3; RESTORE TO VERSION 2 brings
    # it back, so the Iceberg view sees it.
    assert_value_where(table, "entity_name", "Epsilon SA", "record_id", 15)
    assert_value_where(table, "risk_score", 80, "record_id", 15)

    # The 5 originally non_compliant records (3, 9, 15, 17, 20) are present
    # after RESTORE — they survived as under_review rows in V2.
    for rid in [3, 9, 15, 17, 20]:
        mask = pc.equal(table.column("record_id"), rid)
        filtered = table.filter(mask)
        if filtered.num_rows == 1:
            ok(f"Record {rid} present in Iceberg view (expected)")
        else:
            fail(f"Record {rid} count = {filtered.num_rows}, expected 1")


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
