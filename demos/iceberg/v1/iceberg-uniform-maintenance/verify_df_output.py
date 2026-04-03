#!/usr/bin/env python3
"""
Iceberg UniForm OPTIMIZE & VACUUM Maintenance — Data Verification
===================================================================
Reads the app_logs table through the Iceberg metadata chain and verifies
the final state after INSERT x3, OPTIMIZE, DELETE (DEBUG logs), and VACUUM.

Final state: 60 log entries (80 - 20 DEBUG), 4 services, 4 log levels.
  - No DEBUG logs remain
  - total_response_ms = 46208, avg = 770.13
  - Per-service: api-gateway=14, auth-service=15, notification-service=16,
    payment-service=15

Usage:
    python verify.py <data_root_path> [--verbose]

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (
    read_iceberg_table, ok, fail, info,
    assert_row_count, assert_sum, assert_avg, assert_distinct_count,
    assert_count_where, assert_value_where, assert_format_version,
)
from verify_lib import print_header, print_section, print_summary, exit_with_status
from verify_lib.assertions import CYAN, RESET


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_app_logs(data_root, verbose=False):
    print_section("app_logs — Post-Maintenance Final State")

    table_path = os.path.join(data_root, "app_logs")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    assert_format_version(metadata, 2)

    # Final: 80 - 20 DEBUG = 60
    assert_row_count(table, 60)

    # Grand totals
    print(f"\n  {CYAN}Grand totals:{RESET}")
    assert_distinct_count(table, "service_name", 4)
    assert_distinct_count(table, "log_level", 4)  # no DEBUG
    assert_sum(table, "response_time_ms", 46208)
    assert_avg(table, "response_time_ms", 770.13)

    # No DEBUG logs
    print(f"\n  {CYAN}DEBUG logs removed:{RESET}")
    assert_count_where(table, "log_level", "DEBUG", 0)

    # Per-level counts
    print(f"\n  {CYAN}Per-level counts:{RESET}")
    assert_count_where(table, "log_level", "ERROR", 10)
    assert_count_where(table, "log_level", "FATAL", 4)
    assert_count_where(table, "log_level", "INFO", 32)
    assert_count_where(table, "log_level", "WARN", 14)

    # Per-service counts
    print(f"\n  {CYAN}Per-service counts:{RESET}")
    assert_count_where(table, "service_name", "api-gateway", 14)
    assert_count_where(table, "service_name", "auth-service", 15)
    assert_count_where(table, "service_name", "notification-service", 16)
    assert_count_where(table, "service_name", "payment-service", 15)

    # Per-service response time totals
    print(f"\n  {CYAN}Per-service response time totals:{RESET}")
    import pyarrow.compute as pc
    for svc, expected_total in [("api-gateway", 7281), ("auth-service", 412),
                                 ("notification-service", 28505), ("payment-service", 10010)]:
        mask = pc.equal(table.column("service_name"), svc)
        filtered = table.filter(mask)
        actual = pc.sum(filtered.column("response_time_ms")).as_py()
        if actual == expected_total:
            ok(f"SUM(response_time_ms) WHERE service_name={svc!r} = {expected_total}")
        else:
            fail(f"SUM(response_time_ms) WHERE service_name={svc!r} = {actual}, expected {expected_total}")

    # Spot-checks
    print(f"\n  {CYAN}Seed data spot-check (log_id=1):{RESET}")
    assert_value_where(table, "service_name", "auth-service", "log_id", 1)
    assert_value_where(table, "log_level", "INFO", "log_id", 1)
    assert_value_where(table, "message", "User login successful", "log_id", 1)
    assert_value_where(table, "response_time_ms", 45, "log_id", 1)
    assert_value_where(table, "endpoint", "/api/auth/login", "log_id", 1)

    print(f"\n  {CYAN}Batch 2 & 3 spot-checks:{RESET}")
    assert_value_where(table, "service_name", "payment-service", "log_id", 43)
    assert_value_where(table, "message", "Duplicate transaction detected", "log_id", 43)
    assert_value_where(table, "response_time_ms", 95, "log_id", 43)
    assert_value_where(table, "service_name", "notification-service", "log_id", 48)
    assert_value_where(table, "log_level", "FATAL", "log_id", 48)
    assert_value_where(table, "service_name", "payment-service", "log_id", 67)
    assert_value_where(table, "message", "Payout batch completed", "log_id", 67)
    assert_value_where(table, "response_time_ms", 600, "log_id", 67)

    # Max response time preserved
    print(f"\n  {CYAN}Max response time:{RESET}")
    max_rt = pc.max(table.column("response_time_ms")).as_py()
    if max_rt == 15000:
        ok(f"MAX(response_time_ms) = 15000")
    else:
        fail(f"MAX(response_time_ms) = {max_rt}, expected 15000")

    # ERROR payments
    print(f"\n  {CYAN}ERROR payment-service logs:{RESET}")
    error_mask = pc.equal(table.column("log_level"), "ERROR")
    payment_mask = pc.equal(table.column("service_name"), "payment-service")
    combined = pc.and_(error_mask, payment_mask)
    actual = pc.sum(combined).as_py()
    if actual == 2:
        ok(f"COUNT(ERROR + payment-service) = 2")
    else:
        fail(f"COUNT(ERROR + payment-service) = {actual}, expected 2")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-maintenance demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing app_logs/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Maintenance — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "app_logs")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_app_logs(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
