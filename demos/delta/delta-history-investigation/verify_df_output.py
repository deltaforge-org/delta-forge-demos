#!/usr/bin/env python3
"""
Delta History Investigation -- Delta Data Verification (PySpark)

Verifies the product_metrics table produced by the delta-history-investigation demo.
35 rows: 4 products, 3 regions (Americas, EMEA, Asia-Pacific).
Americas revenue is inflated (doubled) -- this is the expected "bad" state.

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

def verify_product_metrics(spark, data_root, verbose=False):
    print_section("product_metrics -- Final State")

    table_path = os.path.join(data_root, "product_metrics")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 35
    if row_count == 35:
        ok(f"Row count is {row_count} (expected 35)")
    else:
        fail(f"Row count is {row_count} (expected 35)")

    # Assert distinct count for product = 4
    distinct_prod = df.select("product").distinct().count()
    if distinct_prod == 4:
        ok(f"Distinct 'product' count is {distinct_prod} (expected 4)")
    else:
        fail(f"Distinct 'product' count is {distinct_prod} (expected 4)")

    # Assert distinct count for region = 3
    distinct_region = df.select("region").distinct().count()
    if distinct_region == 3:
        ok(f"Distinct 'region' count is {distinct_region} (expected 3)")
    else:
        fail(f"Distinct 'region' count is {distinct_region} (expected 3)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta History Investigation -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "product_metrics")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_product_metrics(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
