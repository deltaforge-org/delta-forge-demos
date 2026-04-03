#!/usr/bin/env python3
"""
Delta GDPR Data Erasure -- Delta Data Verification (PySpark)

Verifies the customer_accounts table produced by the delta-gdpr-data-erasure demo.
30 rows total: 10 accounts with erased PII (NULL ssn/phone/mailing_address),
20 accounts with intact data.

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

def verify_customer_accounts(spark, data_root, verbose=False):
    print_section("customer_accounts -- Final State")

    table_path = os.path.join(data_root, "customer_accounts")
    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    col_count = len(df.columns)
    ok(f"Loaded {row_count} rows, {col_count} columns via Delta")

    if verbose:
        info(f"Columns: {df.columns}")

    # Assert row count = 30
    if row_count == 30:
        ok(f"Row count is {row_count} (expected 30)")
    else:
        fail(f"Row count is {row_count} (expected 30)")

    # Assert null count for ssn = 10
    ssn_nulls = df.filter(col("ssn").isNull()).count()
    if ssn_nulls == 10:
        ok(f"Null count for 'ssn' is {ssn_nulls} (expected 10)")
    else:
        fail(f"Null count for 'ssn' is {ssn_nulls} (expected 10)")

    # Assert null count for phone = 10
    phone_nulls = df.filter(col("phone").isNull()).count()
    if phone_nulls == 10:
        ok(f"Null count for 'phone' is {phone_nulls} (expected 10)")
    else:
        fail(f"Null count for 'phone' is {phone_nulls} (expected 10)")

def main():
    data_root, verbose = resolve_data_root()

    print_header("Delta GDPR Data Erasure -- Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "customer_accounts")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    spark = get_spark()
    try:
        verify_customer_accounts(spark, data_root, verbose=verbose)

        print_summary()
        exit_with_status()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
