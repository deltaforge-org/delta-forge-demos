#!/usr/bin/env python3
"""
Delta Append-Only Enforcement -- Delta Data Verification (PySpark)

Verifies the compliance_ledger and mutable_ledger tables.
compliance_ledger: 25 rows (20 original + 5 inserted, append-only).
mutable_ledger: 18 rows (20 original - 2 deleted, 3 updated in place).

Usage:
    python verify_df_output.py <data_root_path> [--verbose]

Requirements:
    pip install pyspark delta-spark
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (ok, fail, info,
    print_header, print_section, print_summary, exit_with_status)
from verify_lib.spark_session import get_spark, resolve_data_root

def verify_compliance_ledger(spark, data_root, verbose=False):
    print_section("compliance_ledger -- Final State")

    table_path = os.path.join(data_root, "compliance_ledger")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    if row_count == 25:
        ok("ROW_COUNT = 25")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 25")

def verify_mutable_ledger(spark, data_root, verbose=False):
    print_section("mutable_ledger -- Final State")

    table_path = os.path.join(data_root, "mutable_ledger")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    if row_count == 18:
        ok("ROW_COUNT = 18")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 18")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Append-Only Enforcement -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("compliance_ledger", "mutable_ledger"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_compliance_ledger(spark, data_root, verbose=verbose)
        verify_mutable_ledger(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
