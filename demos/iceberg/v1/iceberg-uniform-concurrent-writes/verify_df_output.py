#!/usr/bin/env python3
"""
Iceberg UniForm Concurrent Multi-Pipeline Writes — Data Verification
=======================================================================
Reads the ingestion_log table through the Iceberg metadata chain and verifies
the final state after INSERT x3, UPDATE (reprocessed), MERGE (corrections),
and DELETE (failed gamma records).

Final state: 50 records across 3 pipelines, 4 batches, 3 source systems.
  - alpha: 20 records (5 reprocessed)
  - beta: 20 records (3 corrected via MERGE + 5 new from MERGE)
  - gamma: 10 records (5 deleted)

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
def verify_ingestion_log(data_root, verbose=False):
    print_section("ingestion_log — Multi-Pipeline Final State")

    table_path = os.path.join(data_root, "ingestion_log")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    assert_format_version(metadata, 2)

    # Final row count: 20 + 15 + 15 + 5 (MERGE insert) - 5 (DELETE) = 50
    assert_row_count(table, 50)

    # Distinct counts
    print(f"\n  {CYAN}Distinct counts:{RESET}")
    assert_distinct_count(table, "pipeline_name", 3)
    assert_distinct_count(table, "batch_id", 4)
    assert_distinct_count(table, "source_system", 3)

    # Per-pipeline counts
    print(f"\n  {CYAN}Per-pipeline counts:{RESET}")
    assert_count_where(table, "pipeline_name", "etl-team-alpha", 20)
    assert_count_where(table, "pipeline_name", "etl-team-beta", 20)
    assert_count_where(table, "pipeline_name", "etl-team-gamma", 10)

    # Per-batch counts
    print(f"\n  {CYAN}Per-batch counts:{RESET}")
    assert_count_where(table, "batch_id", "batch-001", 20)
    assert_count_where(table, "batch_id", "batch-002", 12)
    assert_count_where(table, "batch_id", "batch-003", 10)
    assert_count_where(table, "batch_id", "batch-004", 8)

    # Reprocessed and corrected records
    print(f"\n  {CYAN}Record type counts:{RESET}")
    assert_count_where(table, "record_type", "reprocessed", 5)
    assert_count_where(table, "record_type", "corrected", 3)

    # Verify corrected records (MERGE updated)
    print(f"\n  {CYAN}MERGE correction spot-checks:{RESET}")
    assert_value_where(table, "record_type", "corrected", "record_id", 21)
    assert_value_where(table, "record_type", "corrected", "record_id", 25)
    assert_value_where(table, "record_type", "corrected", "record_id", 30)

    # Verify deleted records are gone (gamma 36-40)
    print(f"\n  {CYAN}DELETE verification (gamma records 36-40 removed):{RESET}")
    import pyarrow.compute as pc
    for rid in [36, 37, 38, 39, 40]:
        mask = pc.equal(table.column("record_id"), rid)
        count = pc.sum(mask).as_py()
        if count == 0:
            ok(f"record_id={rid} correctly deleted")
        else:
            fail(f"record_id={rid} still present ({count} rows), expected deleted")

    # Verify remaining gamma records exist (41-50)
    print(f"\n  {CYAN}Remaining gamma records (41-50):{RESET}")
    for rid in [41, 45, 50]:
        mask = pc.equal(table.column("record_id"), rid)
        count = pc.sum(mask).as_py()
        if count == 1:
            ok(f"record_id={rid} present")
        else:
            fail(f"record_id={rid} count = {count}, expected 1")

    # Verify MERGE-inserted records (51-55)
    print(f"\n  {CYAN}MERGE-inserted records (51-55):{RESET}")
    for rid in [51, 52, 53, 54, 55]:
        assert_value_where(table, "pipeline_name", "etl-team-beta", "record_id", rid)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-concurrent-writes demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing ingestion_log/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Concurrent Writes — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "ingestion_log")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_ingestion_log(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
