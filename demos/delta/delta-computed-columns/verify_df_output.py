#!/usr/bin/env python3
"""
Delta Computed Columns -- Delta Data Verification (PySpark)

Verifies the sales_invoices table: 50 rows,
4 distinct sales reps (Alice, Bob, Carol, Sarah).

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

def verify_sales_invoices(spark, data_root, verbose=False):
    print_section("sales_invoices -- Final State")

    table_path = os.path.join(data_root, "sales_invoices")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 50:
        ok("ROW_COUNT = 50")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 50")

    # Distinct sales_rep count
    distinct_reps = df.select("sales_rep").distinct().count()
    if distinct_reps == 4:
        ok("DISTINCT sales_rep = 4")
    else:
        fail(f"DISTINCT sales_rep = {distinct_reps}, expected 4")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Computed Columns -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "sales_invoices")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_sales_invoices(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
