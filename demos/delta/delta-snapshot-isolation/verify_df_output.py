#!/usr/bin/env python3
"""
Delta Snapshot Isolation -- Delta Data Verification (PySpark)
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
from pyspark.sql.functions import col

def verify_fund_holdings(spark, data_root, verbose=False):
    print_section("fund_holdings -- Final State")

    table_path = os.path.join(data_root, "fund_holdings")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 60
    if row_count == 60:
        ok(f"Row count is 60")
    else:
        fail(f"Expected 60 rows, got {row_count}")

    # Assert 4 distinct fund_ids
    distinct_funds = df.select("fund_id").distinct().count()
    if distinct_funds == 4:
        ok(f"Distinct 'fund_id' count is 4")
    else:
        fail(f"Expected 4 distinct 'fund_id', got {distinct_funds}")

    # Assert fund_id counts
    for fund_id, expected in [("GF01", 10), ("VF02", 10), ("IF03", 20), ("SR04", 20)]:
        actual = df.filter(col("fund_id") == fund_id).count()
        if actual == expected:
            ok(f"fund_id='{fund_id}' count is {expected}")
        else:
            fail(f"Expected {expected} rows where fund_id='{fund_id}', got {actual}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Snapshot Isolation -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "fund_holdings")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_fund_holdings(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
