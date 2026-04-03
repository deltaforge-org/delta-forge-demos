#!/usr/bin/env python3
"""
Delta Collations -- Delta Data Verification (PySpark)

Verifies the global_contacts table: 40 rows,
18 countries, 16 languages.

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

def verify_global_contacts(spark, data_root, verbose=False):
    print_section("global_contacts -- Final State")

    table_path = os.path.join(data_root, "global_contacts")
    df = spark.read.format("delta").load(table_path)

    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta (PySpark)")

    if verbose:
        info(f"Columns: {df.columns}")
        df.show(10, truncate=False)

    # Row count
    if row_count == 40:
        ok("ROW_COUNT = 40")
    else:
        fail(f"ROW_COUNT = {row_count}, expected 40")

    # Distinct country count
    distinct_countries = df.select("country").distinct().count()
    if distinct_countries == 18:
        ok("DISTINCT country = 18")
    else:
        fail(f"DISTINCT country = {distinct_countries}, expected 18")

    # Distinct language count
    distinct_languages = df.select("language").distinct().count()
    if distinct_languages == 16:
        ok("DISTINCT language = 16")
    else:
        fail(f"DISTINCT language = {distinct_languages}, expected 16")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Collations -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "global_contacts")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_global_contacts(spark, data_root, verbose=verbose)
    finally:
        spark.stop()

    print_summary()
    exit_with_status()

if __name__ == "__main__":
    main()
