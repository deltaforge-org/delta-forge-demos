#!/usr/bin/env python3
"""
Delta Change Data Feed -- Delta Data Verification (PySpark)

Verifies the customer_accounts table: 45 rows,
tiers: 10 gold, 15 silver, 20 bronze.

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

def verify_customer_accounts(spark, data_root, verbose=False):
    print_section("customer_accounts -- Final State")

    table_path = os.path.join(data_root, "customer_accounts")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 45:
        ok("ROW_COUNT = 45")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 45")

    # Tier counts
    for tier, expected in [("gold", 10), ("silver", 15), ("bronze", 20)]:
        cnt = df.filter(df.tier == tier).count()
        if cnt == expected:
            ok(f"COUNT WHERE tier='{tier}' = {expected}")
        else:
            fail(f"COUNT WHERE tier='{tier}' = {cnt}, expected {expected}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Change Data Feed -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "customer_accounts")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_customer_accounts(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
