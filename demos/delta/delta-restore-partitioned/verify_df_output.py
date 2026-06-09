#!/usr/bin/env python3
"""
Delta Restore Partitioned -- Delta Data Verification (PySpark)
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

def verify_quarterly_revenue(spark, data_root, verbose=False):
    print_section("quarterly_revenue -- Final State")

    table_path = os.path.join(data_root, "quarterly_revenue")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 45
    if row_count == 45:
        ok(f"Row count is 45")
    else:
        fail(f"Expected 45 rows, got {row_count}")

    # Assert 4 distinct quarters
    distinct_quarters = df.select("quarter").distinct().count()
    if distinct_quarters == 4:
        ok(f"Distinct 'quarter' count is 4")
    else:
        fail(f"Expected 4 distinct 'quarter', got {distinct_quarters}")

    # Assert counts per quarter
    for quarter, expected in [("Q1", 10), ("Q2", 10), ("Q3", 15), ("Q4", 10)]:
        actual = df.filter(col("quarter") == quarter).count()
        if actual == expected:
            ok(f"quarter='{quarter}' count is {expected}")
        else:
            fail(f"Expected {expected} rows where quarter='{quarter}', got {actual}")

    # Verify Q1 restored tax rate
    q1_taxes = [row.tax_rate for row in df.filter(col("quarter") == "Q1").select("tax_rate").collect()]
    if all(t == 0.08 for t in q1_taxes):
        ok("All Q1 rows have tax_rate=0.08 (restored)")
    else:
        fail(f"Expected all Q1 tax_rate=0.08, got {q1_taxes}")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta Restore Partitioned -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "quarterly_revenue")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_quarterly_revenue(spark, data_root, verbose=verbose)
        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
