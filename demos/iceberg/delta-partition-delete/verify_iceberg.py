#!/usr/bin/env python3
"""
Iceberg UniForm verification for delta-partition-delete demo.

Reads the warehouse_orders table via DuckDB's Iceberg extension and verifies
that all query results match the expected final state after three DELETE
operations (partition-scoped, cross-partition, conditional).

Expected final state (33 rows):
  - us-west:    10 rows (15 - 3 cancelled - 2 returned)
  - us-central: 13 rows (15 - 2 returned)
  - us-east:    10 rows (15 - 2 returned - 3 low-value pending)

Usage:
  python verify_iceberg.py [TABLE_PATH]

  TABLE_PATH defaults to B:/!demo/df-demo/delta-partition-delete/warehouse_orders
  On WSL, pass the /mnt/b/... equivalent.

Requirements:
  pip install duckdb
"""
import sys
import os

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_PATH = r"B:\!demo\df-demo\delta-partition-delete\warehouse_orders"
TABLE_BASE = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
TABLE_BASE = TABLE_BASE.replace("\\", "/")
METADATA_DIR = os.path.join(TABLE_BASE, "metadata")

# Find the latest metadata version
metadata_files = sorted(
    [f for f in os.listdir(METADATA_DIR) if f.endswith(".metadata.json")
     and not f.endswith(".local.metadata.json")],
    key=lambda f: int(f.split(".")[0][1:]),  # v7.metadata.json -> 7
)
LATEST_METADATA = os.path.join(METADATA_DIR, metadata_files[-1])

print(f"Table path:  {TABLE_BASE}")
print(f"Metadata:    {LATEST_METADATA}")
print(f"Versions:    {len(metadata_files)} ({metadata_files[0]} .. {metadata_files[-1]})")
print()

# ---------------------------------------------------------------------------
# Connect and load via DuckDB Iceberg extension
# ---------------------------------------------------------------------------
import duckdb

db = duckdb.connect()
db.execute("INSTALL iceberg; LOAD iceberg;")

# iceberg_scan reads the metadata JSON and resolves data/delete files
TABLE_SQL = f"iceberg_scan('{LATEST_METADATA}')"

# Quick smoke test — verify the table is readable
try:
    smoke = db.execute(f"SELECT COUNT(*) FROM {TABLE_SQL}").fetchone()[0]
    print(f"Iceberg table loaded: {smoke} rows")
except Exception as e:
    print(f"FATAL: Cannot read Iceberg table: {e}")
    sys.exit(2)

print()

# ---------------------------------------------------------------------------
# Test framework
# ---------------------------------------------------------------------------
passed = 0
failed = 0
errors = 0


def check(name: str, query: str, expected):
    """Run a query and compare the single scalar result to expected."""
    global passed, failed, errors
    try:
        result = db.execute(f"SELECT {query} FROM {TABLE_SQL}").fetchone()[0]
        ok = _compare(result, expected)
        if ok:
            passed += 1
            print(f"  PASS  {name}: {result}")
        else:
            failed += 1
            print(f"  FAIL  {name}: expected {expected}, got {result}")
    except Exception as e:
        errors += 1
        print(f"  ERROR {name}: {e}")


def check_query(name: str, full_query: str, expected):
    """Run a full SQL query and check scalar result."""
    global passed, failed, errors
    try:
        result = db.execute(full_query).fetchone()[0]
        ok = _compare(result, expected)
        if ok:
            passed += 1
            print(f"  PASS  {name}: {result}")
        else:
            failed += 1
            print(f"  FAIL  {name}: expected {expected}, got {result}")
    except Exception as e:
        errors += 1
        print(f"  ERROR {name}: {e}")


def check_absent(name: str, full_query: str):
    """Verify a query returns zero rows."""
    global passed, failed, errors
    try:
        rows = db.execute(full_query).fetchall()
        if len(rows) == 0:
            passed += 1
            print(f"  PASS  {name}: 0 rows (as expected)")
        else:
            failed += 1
            print(f"  FAIL  {name}: expected 0 rows, got {len(rows)}")
    except Exception as e:
        errors += 1
        print(f"  ERROR {name}: {e}")


def _compare(result, expected):
    if isinstance(expected, float):
        result = float(result) if result is not None else None
        return result is not None and abs(result - expected) < 0.01
    elif isinstance(expected, int):
        result = int(result) if result is not None else None
        return result == expected
    elif isinstance(expected, str):
        return str(result) == expected if result is not None else False
    return result == expected


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
T = TABLE_SQL

print("=" * 70)
print("1. TOTAL ROW COUNT")
print("=" * 70)
check("total_rows", "COUNT(*)", 33)

print()
print("=" * 70)
print("2. PER-REGION ROW COUNTS")
print("=" * 70)
check_query("us_west_count",
    f"SELECT COUNT(*) FROM {T} WHERE region = 'us-west'", 10)
check_query("us_central_count",
    f"SELECT COUNT(*) FROM {T} WHERE region = 'us-central'", 13)
check_query("us_east_count",
    f"SELECT COUNT(*) FROM {T} WHERE region = 'us-east'", 10)

print()
print("=" * 70)
print("3. PER-REGION TOTAL VALUES (quantity * unit_price)")
print("=" * 70)
check_query("us_west_total_value",
    f"SELECT ROUND(CAST(SUM(quantity * unit_price) AS DOUBLE), 2) FROM {T} WHERE region = 'us-west'",
    6959.19)
check_query("us_central_total_value",
    f"SELECT ROUND(CAST(SUM(quantity * unit_price) AS DOUBLE), 2) FROM {T} WHERE region = 'us-central'",
    7514.01)
check_query("us_east_total_value",
    f"SELECT ROUND(CAST(SUM(quantity * unit_price) AS DOUBLE), 2) FROM {T} WHERE region = 'us-east'",
    5223.13)

print()
print("=" * 70)
print("4. PER-STATUS COUNTS")
print("=" * 70)
check_query("fulfilled_count",
    f"SELECT COUNT(*) FROM {T} WHERE status = 'fulfilled'", 18)
check_query("pending_count",
    f"SELECT COUNT(*) FROM {T} WHERE status = 'pending'", 9)
check_query("cancelled_count",
    f"SELECT COUNT(*) FROM {T} WHERE status = 'cancelled'", 6)
check_query("returned_count",
    f"SELECT COUNT(*) FROM {T} WHERE status = 'returned'", 0)

print()
print("=" * 70)
print("5. DELETED ROWS ARE ABSENT")
print("=" * 70)
# IDs deleted by step 1 (cancelled from us-west): 4, 8, 13
# IDs deleted by step 2 (returned everywhere): 6, 10, 21, 24, 36, 39
# IDs deleted by step 3 (low-value pending us-east): 38, 43, 45
deleted_ids = [4, 8, 13, 6, 10, 21, 24, 36, 39, 38, 43, 45]
check_absent("deleted_ids_absent",
    f"SELECT * FROM {T} WHERE id IN ({','.join(str(i) for i in deleted_ids)})")

print()
print("=" * 70)
print("6. SPOT-CHECK SURVIVING ROWS (8 rows across all regions)")
print("=" * 70)
# Verify specific rows exist with exact column values
spot_checks = [
    # (id, region, product, quantity, unit_price, status)
    (1,  'us-west',    'Laptop Pro',          2,  899.99, 'fulfilled'),
    (3,  'us-west',    'Wireless Headphones', 10, 79.99,  'pending'),
    (7,  'us-west',    'USB-C Hub',           15, 49.99,  'pending'),
    (16, 'us-central', 'Monitor 27in',        3,  449.99, 'fulfilled'),
    (23, 'us-central', 'Bookshelf Oak',       1,  349.99, 'pending'),
    (31, 'us-east',    'Phone Case Premium',  20, 39.99,  'fulfilled'),
    (33, 'us-east',    'LED Desk Lamp',       8,  69.99,  'pending'),
    (41, 'us-east',    'HDMI Cable 6ft',      50, 12.99,  'fulfilled'),
]
for sid, sregion, sproduct, sqty, sprice, sstatus in spot_checks:
    check_query(f"row_id={sid} ({sproduct})",
        f"""SELECT COUNT(*) FROM {T}
            WHERE id = {sid}
              AND region = '{sregion}'
              AND product = '{sproduct}'
              AND quantity = {sqty}
              AND ROUND(CAST(unit_price AS DOUBLE), 2) = {sprice}
              AND status = '{sstatus}'""",
        1)

print()
print("=" * 70)
print("7. GRAND TOTAL VALUE")
print("=" * 70)
check_query("grand_total_value",
    f"SELECT ROUND(CAST(SUM(quantity * unit_price) AS DOUBLE), 2) FROM {T}",
    19696.33)

print()
print("=" * 70)
print("8. PARTITION INTEGRITY")
print("=" * 70)
check_absent("no_invalid_regions",
    f"SELECT * FROM {T} WHERE region NOT IN ('us-west', 'us-central', 'us-east')")

print()
print("=" * 70)
print("9. UNIQUENESS — NO DUPLICATE IDs")
print("=" * 70)
check_query("unique_ids",
    f"SELECT COUNT(*) - COUNT(DISTINCT id) FROM {T}", 0)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print()
print("=" * 70)
total = passed + failed + errors
if failed == 0 and errors == 0:
    print(f"ICEBERG VERIFICATION: {passed}/{total} passed  ===  ALL CHECKS PASSED")
    print("Iceberg UniForm data matches expected Delta state.")
else:
    print(f"ICEBERG VERIFICATION: {passed}/{total} passed, {failed} failed, {errors} errors")
print("=" * 70)

sys.exit(1 if (failed > 0 or errors > 0) else 0)
