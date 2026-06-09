#!/usr/bin/env python3
"""
Delta Convert to Delta -- Delta Data Verification (PySpark)

Verifies the legacy_data table after migration:
  - 45 rows (50 original - 5 duplicates)
  - 35 migrated + 10 post-migration
  - No legacy payment codes ('cc' or 'pp')
  - 4 payment methods: credit_card, paypal, bank_transfer, cash

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

def verify_legacy_data(spark, data_root, verbose=False):
    print_section("legacy_data -- Final State")

    table_path = os.path.join(data_root, "legacy_data")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 45 rows total
    if row_count == 45:
        ok("Row count = 45")
    else:
        fail(f"Row count = {row_count}, expected 45")

    # 4 distinct payment methods
    distinct_pm = df.select("payment_method").distinct().count()
    if distinct_pm == 4:
        ok("Distinct payment_method count = 4")
    else:
        fail(f"Distinct payment_method count = {distinct_pm}, expected 4")

    # No legacy payment codes remain
    cc_count = df.filter(df.payment_method == "cc").count()
    pp_count = df.filter(df.payment_method == "pp").count()
    if cc_count == 0 and pp_count == 0:
        ok("No legacy payment codes ('cc' or 'pp') remain")
    else:
        fail(f"Legacy payment codes found: cc={cc_count}, pp={pp_count}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Convert to Delta -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "legacy_data")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_legacy_data(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
