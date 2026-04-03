#!/usr/bin/env python3
"""
Delta Decimal Precision -- Delta Data Verification (PySpark)

Verifies the financial_ledger table:
  - 40 rows total
  - 5 currencies: USD 15, EUR 5, GBP 5, JPY 5, CHF 5

Usage:
    python verify_df_output.py <data_root_path> [--verbose]

Requirements:
    pip install pyspark delta-spark
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
from verify_lib import ok, fail, info, print_header, print_section, print_summary, exit_with_status
from verify_lib.spark_session import get_spark, resolve_data_root

def verify_financial_ledger(spark, data_root, verbose=False):
    print_section("financial_ledger -- Final State")

    table_path = os.path.join(data_root, "financial_ledger")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 40 rows total
    if row_count == 40:
        ok("Row count = 40")
    else:
        fail(f"Row count = {row_count}, expected 40")

    # 5 distinct currencies
    distinct_currencies = df.select("currency").distinct().count()
    if distinct_currencies == 5:
        ok("Distinct currency count = 5")
    else:
        fail(f"Distinct currency count = {distinct_currencies}, expected 5")

    # Currency breakdown
    for currency, expected in [("USD", 15), ("EUR", 5), ("GBP", 5), ("JPY", 5), ("CHF", 5)]:
        actual = df.filter(df.currency == currency).count()
        if actual == expected:
            ok(f"currency='{currency}' count = {expected}")
        else:
            fail(f"currency='{currency}' count = {actual}, expected {expected}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Decimal Precision -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "financial_ledger")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_financial_ledger(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
