#!/usr/bin/env python3
"""
Delta Bloom Filters -- Delta Data Verification (PySpark)

Verifies the transaction_log table: 60 rows,
40 completed, 5 disputed, 15 refunded.

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

def verify_transaction_log(spark, data_root, verbose=False):
    print_section("transaction_log -- Final State")

    table_path = os.path.join(data_root, "transaction_log")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 60:
        ok("ROW_COUNT = 60")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 60")

    # Status counts
    for status, expected in [("completed", 40), ("disputed", 5), ("refunded", 15)]:
        cnt = df.filter(df.status == status).count()
        if cnt == expected:
            ok(f"COUNT WHERE status='{status}' = {expected}")
        else:
            fail(f"COUNT WHERE status='{status}' = {cnt}, expected {expected}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Bloom Filters -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "transaction_log")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_transaction_log(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
