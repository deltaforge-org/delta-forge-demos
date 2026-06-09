#!/usr/bin/env python3
"""
Analyze the generated Iceberg trips table by reading Parquet data files
with DuckDB. Computes all assertion values needed for queries.sql.
"""
import os
import duckdb

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_GLOB = os.path.join(DEMO_DIR, "trips", "data", "**", "*.parquet")

con = duckdb.connect()
con.execute(f"CREATE VIEW trips AS SELECT * FROM read_parquet('{PARQUET_GLOB}', hive_partitioning=false)")

print("=== Q1: Total Row Count ===")
row = con.execute("SELECT COUNT(*) AS total FROM trips").fetchone()
print(f"  total_rows: {row[0]}")

print("\n=== Q2: Monthly Breakdown ===")
rows = con.execute("""
    SELECT MONTH(pickup_date) AS m, COUNT(*) AS cnt
    FROM trips
    GROUP BY MONTH(pickup_date)
    ORDER BY m
""").fetchall()
for r in rows:
    print(f"  month {r[0]}: {r[1]}")

print("\n=== Q3: March 2025 filter ===")
row = con.execute("""
    SELECT COUNT(*) AS cnt
    FROM trips
    WHERE pickup_date >= DATE '2025-03-01' AND pickup_date <= DATE '2025-03-31'
""").fetchone()
print(f"  march_rows: {row[0]}")

print("\n=== Q4: Per-City Breakdown ===")
rows = con.execute("""
    SELECT city, COUNT(*) AS cnt
    FROM trips GROUP BY city ORDER BY city
""").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")

print("\n=== Q5: Payment Analysis ===")
rows = con.execute("""
    SELECT
        payment_type,
        COUNT(*) AS cnt,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        SUM(CASE WHEN tip_amount IS NULL THEN 1 ELSE 0 END) AS null_tips
    FROM trips
    GROUP BY payment_type ORDER BY payment_type
""").fetchall()
for r in rows:
    print(f"  {r[0]}: count={r[1]}, avg_fare={r[2]}, null_tips={r[3]}")

print("\n=== Q6: City by Avg Distance ===")
rows = con.execute("""
    SELECT city, ROUND(AVG(distance_miles), 2) AS avg_dist, COUNT(*) AS cnt
    FROM trips GROUP BY city ORDER BY avg_dist DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]}: avg_dist={r[1]}, cnt={r[2]}")

print("\n=== Q7: Driver Performance (top 10) ===")
rows = con.execute("""
    SELECT driver_id, COUNT(*) AS trip_count
    FROM trips GROUP BY driver_id ORDER BY trip_count DESC, driver_id LIMIT 10
""").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")

print("\n=== Distinct driver count ===")
row = con.execute("SELECT COUNT(DISTINCT driver_id) AS cnt FROM trips").fetchone()
print(f"  distinct_drivers: {row[0]}")

print("\n=== VERIFY: Grand Totals ===")
row = con.execute("""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT city) AS city_count,
        COUNT(DISTINCT payment_type) AS payment_type_count,
        COUNT(DISTINCT driver_id) AS driver_count,
        ROUND(SUM(fare_amount), 2) AS total_fare,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(SUM(distance_miles), 1) AS total_distance,
        SUM(CASE WHEN tip_amount IS NULL THEN 1 ELSE 0 END) AS null_tip_count,
        SUM(CASE WHEN pickup_date >= DATE '2025-03-01' AND pickup_date <= DATE '2025-03-31' THEN 1 ELSE 0 END) AS march_rows
    FROM trips
""").fetchone()
print(f"  total_rows: {row[0]}")
print(f"  city_count: {row[1]}")
print(f"  payment_type_count: {row[2]}")
print(f"  driver_count: {row[3]}")
print(f"  total_fare: {row[4]}")
print(f"  avg_fare: {row[5]}")
print(f"  total_distance: {row[6]}")
print(f"  null_tip_count: {row[7]}")
print(f"  march_rows: {row[8]}")

print("\n=== Fare stats ===")
row = con.execute("""
    SELECT
        ROUND(MIN(fare_amount), 2) AS min_fare,
        ROUND(MAX(fare_amount), 2) AS max_fare,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(SUM(fare_amount), 2) AS total_fare
    FROM trips
""").fetchone()
print(f"  min_fare: {row[0]}")
print(f"  max_fare: {row[1]}")
print(f"  avg_fare: {row[2]}")
print(f"  total_fare: {row[3]}")

print("\n=== Cash payment stats ===")
row = con.execute("""
    SELECT
        COUNT(*) AS cnt,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        SUM(CASE WHEN tip_amount IS NULL THEN 1 ELSE 0 END) AS null_tips
    FROM trips WHERE payment_type = 'Cash'
""").fetchone()
print(f"  cash_count: {row[0]}, avg_fare: {row[1]}, null_tips: {row[2]}")

row = con.execute("""
    SELECT
        COUNT(*) AS cnt,
        ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM trips WHERE payment_type = 'Credit Card'
""").fetchone()
print(f"  credit_card_count: {row[0]}, avg_fare: {row[1]}")

row = con.execute("""
    SELECT
        COUNT(*) AS cnt,
        ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM trips WHERE payment_type = 'Digital Wallet'
""").fetchone()
print(f"  digital_wallet_count: {row[0]}, avg_fare: {row[1]}")

con.close()
print("\nDone!")
