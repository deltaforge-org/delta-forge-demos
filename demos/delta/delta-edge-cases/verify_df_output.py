#!/usr/bin/env python3
"""
Delta Edge Cases -- Delta Data Verification (PySpark)

Verifies three edge-case tables:
  - config_singleton: 1 row (DELETE + re-INSERT), version=1, updated_by='sre-team'
  - wide_metrics: 18 rows (20 - 2 pruned), corrected revenue for id=1
  - empty_staging: 0 rows (inserted 3, then deleted all)

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
from pyspark.sql import functions as F

def verify_config_singleton(spark, data_root, verbose=False):
    print_section("config_singleton -- Final State")

    table_path = os.path.join(data_root, "config_singleton")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Exactly 1 row after DELETE + re-INSERT
    if row_count == 1:
        ok("Row count = 1")
    else:
        fail(f"Row count = {row_count}, expected 1")

    # version = 1 where config_key = 'app_settings'
    row = df.filter(df.config_key == "app_settings").select("version", "updated_by").first()
    if row is not None:
        if row["version"] == 1:
            ok("version = 1 where config_key='app_settings'")
        else:
            fail(f"version = {row['version']}, expected 1 where config_key='app_settings'")

        if row["updated_by"] == "sre-team":
            ok("updated_by = 'sre-team' where config_key='app_settings'")
        else:
            fail(f"updated_by = '{row['updated_by']}', expected 'sre-team' where config_key='app_settings'")
    else:
        fail("No row found with config_key='app_settings'")

def verify_wide_metrics(spark, data_root, verbose=False):
    print_section("wide_metrics -- Final State")

    table_path = os.path.join(data_root, "wide_metrics")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 18 rows (20 - 2 pruned provisional months)
    if row_count == 18:
        ok("Row count = 18")
    else:
        fail(f"Row count = {row_count}, expected 18")

    # Corrected January revenue for id=1
    row = df.filter(df.id == 1).select("m01_revenue").first()
    if row is not None:
        val = float(row["m01_revenue"])
        if val == 131000.0:
            ok("m01_revenue = 131000.0 where id=1")
        else:
            fail(f"m01_revenue = {val}, expected 131000.0 where id=1")
    else:
        fail("No row found with id=1")

    # Total profit after all mutations
    total_profit = df.agg(F.sum("m03_profit")).first()[0]
    total_profit = round(float(total_profit), 2) if total_profit is not None else 0.0
    if total_profit == 1039000.0:
        ok("SUM(m03_profit) = 1039000.0")
    else:
        fail(f"SUM(m03_profit) = {total_profit}, expected 1039000.0")

def verify_empty_staging(spark, data_root, verbose=False):
    print_section("empty_staging -- Final State")

    table_path = os.path.join(data_root, "empty_staging")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # 0 rows (cleared after processing)
    if row_count == 0:
        ok("Row count = 0")
    else:
        fail(f"Row count = {row_count}, expected 0")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Edge Cases -- Data Verification")
    print(f"  Data root: {data_root}")

    for tbl_name in ("config_singleton", "wide_metrics", "empty_staging"):
        tbl_dir = os.path.join(data_root, tbl_name)
        if not os.path.isdir(tbl_dir):
            print(f"\nError: {tbl_dir} not found")
            sys.exit(1)

    spark = get_spark()
    try:
        verify_config_singleton(spark, data_root, verbose=verbose)
        verify_wide_metrics(spark, data_root, verbose=verbose)
        verify_empty_staging(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
