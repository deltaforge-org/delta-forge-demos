#!/usr/bin/env python3
"""Analyze Iceberg table parquet files to compute values for ASSERT statements."""

import duckdb

DATA_DIR = "/home/chess/delta-forge/delta-forge-demos/demos/iceberg/iceberg-v2-complex-types/orders/data"

# Find the parquet file
import glob
parquet_files = glob.glob(f"{DATA_DIR}/*.parquet")
assert len(parquet_files) == 1, f"Expected 1 parquet file, found {len(parquet_files)}"
PARQUET = parquet_files[0]

con = duckdb.connect()

# Load data
con.execute(f"""
CREATE TABLE orders AS
SELECT * FROM read_parquet('{PARQUET}')
""")

# Schema
print("=== SCHEMA ===")
print(con.execute("DESCRIBE SELECT * FROM orders").fetchdf().to_string())

print(f"\n=== Total rows: {con.execute('SELECT COUNT(*) FROM orders').fetchone()[0]} ===")

# Q1: Full scan — ROW_COUNT=100
print("\n=== Query 1: Full scan ===")
print(f"ROW_COUNT = {con.execute('SELECT COUNT(*) FROM orders').fetchone()[0]}")
# Spot check first few order_ids
print("First 5 order_ids:")
print(con.execute("SELECT order_id FROM orders ORDER BY order_id LIMIT 5").fetchdf().to_string())

# Q2: Shipping address city breakdown
print("\n=== Query 2: City breakdown ===")
city_df = con.execute("""
    SELECT shipping_address.city AS city, COUNT(*) AS order_count
    FROM orders
    GROUP BY shipping_address.city
    ORDER BY order_count DESC, city
""").fetchdf()
print(city_df.to_string())

# Q3: Explode items — total line items
print("\n=== Query 3: Total line items (unnested) ===")
total_items = con.execute("""
    SELECT COUNT(*) AS total_line_items
    FROM orders, UNNEST(items) AS t(item)
""").fetchone()[0]
print(f"total_line_items = {total_items}")

# Q4: Status breakdown
print("\n=== Query 4: Status breakdown ===")
status_df = con.execute("""
    SELECT status,
           COUNT(*) AS order_count,
           ROUND(AVG(order_total), 2) AS avg_total
    FROM orders
    GROUP BY status
    ORDER BY status
""").fetchdf()
print(status_df.to_string())

# Q5: Top products by quantity
print("\n=== Query 5: Top products by quantity ===")
product_df = con.execute("""
    SELECT item.product_name AS product_name,
           SUM(item.quantity) AS total_qty
    FROM orders, UNNEST(items) AS t(item)
    GROUP BY item.product_name
    ORDER BY total_qty DESC
""").fetchdf()
print(product_df.to_string())

# Q6: State analysis
print("\n=== Query 6: State analysis ===")
state_df = con.execute("""
    SELECT shipping_address.state AS state,
           COUNT(*) AS order_count,
           ROUND(SUM(order_total), 2) AS total_revenue
    FROM orders
    GROUP BY shipping_address.state
    ORDER BY total_revenue DESC
""").fetchdf()
print(state_df.to_string())

# VERIFY: Grand totals
print("\n=== VERIFY: Grand totals ===")
verify = con.execute("""
    SELECT
        COUNT(*) AS total_orders,
        ROUND(SUM(order_total), 2) AS sum_order_total,
        COUNT(DISTINCT shipping_address.city) AS distinct_cities,
        (SELECT COUNT(*) FROM orders, UNNEST(items) AS t(item)) AS total_items
    FROM orders
""").fetchdf()
print(verify.to_string())

# Also: null notes count
null_notes = con.execute("SELECT COUNT(*) FROM orders WHERE notes IS NULL").fetchone()[0]
print(f"\nNull notes count: {null_notes}")

print("\n" + "=" * 70)
print("SUMMARY FOR ASSERT STATEMENTS")
print("=" * 70)
print(f"\nQ1: ROW_COUNT = 100")
print(f"\nQ2: City breakdown (ROW_COUNT = {len(city_df)}):")
for _, row in city_df.iterrows():
    print(f"  city='{row['city']}', order_count={row['order_count']}")
print(f"\nQ3: total_line_items = {total_items}")
print(f"\nQ4: Status breakdown (ROW_COUNT = {len(status_df)}):")
for _, row in status_df.iterrows():
    print(f"  status='{row['status']}', order_count={row['order_count']}, avg_total={row['avg_total']}")
print(f"\nQ5: Top products (ROW_COUNT = {len(product_df)}):")
for _, row in product_df.iterrows():
    print(f"  product_name='{row['product_name']}', total_qty={row['total_qty']}")
print(f"\nQ6: State analysis (ROW_COUNT = {len(state_df)}):")
for _, row in state_df.iterrows():
    print(f"  state='{row['state']}', order_count={row['order_count']}, total_revenue={row['total_revenue']}")
print(f"\nVERIFY:")
v = verify.iloc[0]
print(f"  total_orders = {v['total_orders']}")
print(f"  sum_order_total = {v['sum_order_total']}")
print(f"  distinct_cities = {v['distinct_cities']}")
print(f"  total_items = {v['total_items']}")
