#!/usr/bin/env python3
"""
Delta Unicode Partitioning -- Delta Data Verification (PySpark)

Verifies the cdn_content table: 26 rows (30 - 6 + 2),
5 locale partitions (Japanese, Arabic, Russian, French, Portuguese).

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

def verify_cdn_content(spark, data_root, verbose=False):
    print_section("cdn_content -- Final State")

    table_path = os.path.join(data_root, "cdn_content")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    if row_count == 26:
        ok(f"Row count is 26")
    else:
        fail(f"Expected 26 rows, got {row_count}")

    distinct_locale = df.select("locale").distinct().count()
    if distinct_locale == 5:
        ok(f"Distinct locale count is 5")
    else:
        fail(f"Expected 5 distinct locale values, got {distinct_locale}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Unicode Partitioning -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "cdn_content")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_cdn_content(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
