#!/usr/bin/env python3
"""
Delta CASE Conditional Logic -- Delta Data Verification (PySpark)

Verifies the insurance_claims table: 35 rows (no mutations),
4 claim types: auto 11, home 9, health 9, life 6.

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

def verify_insurance_claims(spark, data_root, verbose=False):
    print_section("insurance_claims -- Final State")

    table_path = os.path.join(data_root, "insurance_claims")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 35:
        ok("ROW_COUNT = 35")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 35")

    # Claim type counts
    for claim_type, expected in [("auto", 11), ("home", 9), ("health", 9), ("life", 6)]:
        cnt = df.filter(df.claim_type == claim_type).count()
        if cnt == expected:
            ok(f"COUNT WHERE claim_type='{claim_type}' = {expected}")
        else:
            fail(f"COUNT WHERE claim_type='{claim_type}' = {cnt}, expected {expected}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta CASE Conditional Logic -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "insurance_claims")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_insurance_claims(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
