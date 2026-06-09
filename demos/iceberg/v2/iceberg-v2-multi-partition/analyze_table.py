#!/usr/bin/env python3
"""Analyze Iceberg V2 multi-partition table parquet files with DuckDB.

Reads all partitioned Parquet data files and computes the exact values
needed for ASSERT statements in queries.sql.
"""
import os
import duckdb

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DEMO_DIR, "weather_readings", "data")

con = duckdb.connect()

# Load all partitioned parquet files without hive partitioning
# (hive partitioning URL-encodes spaces as +, but actual parquet data has proper values)
con.execute(f"""
CREATE TABLE readings AS
SELECT * FROM read_parquet('{DATA_DIR}/**/*.parquet', hive_partitioning=false)
""")

total = con.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
print(f"Total rows: {total}")

# Schema
print("\n=== SCHEMA ===")
print(con.execute("DESCRIBE SELECT * FROM readings").fetchdf().to_string())

# Sample
print("\n=== SAMPLE (5 rows) ===")
print(con.execute("SELECT * FROM readings ORDER BY reading_id LIMIT 5").fetchdf().to_string())

# ── Query 1: Full scan ────────────────────────────────────────────────
print(f"\n=== Q1: Full scan ROW_COUNT ===")
print(f"  ROW_COUNT = {total}")

# ── Query 2: Region breakdown ─────────────────────────────────────────
print("\n=== Q2: Region breakdown ===")
regions = con.execute("""
SELECT region, COUNT(*) as cnt
FROM readings
GROUP BY region ORDER BY region
""").fetchdf()
print(regions.to_string())

# ── Query 3: Region+Year partition filter (Europe, 2024) ──────────────
print("\n=== Q3: Europe 2024 filter ===")
europe_2024 = con.execute("""
SELECT COUNT(*) as cnt
FROM readings
WHERE region = 'Europe'
  AND observation_date >= '2024-01-01'
  AND observation_date < '2025-01-01'
""").fetchone()[0]
print(f"  Europe 2024 count: {europe_2024}")

# Show the actual rows for Europe 2024
europe_2024_detail = con.execute("""
SELECT ROUND(AVG(temperature_c), 2) AS avg_temp,
       ROUND(AVG(humidity_pct), 2) AS avg_humidity,
       ROUND(AVG(wind_speed_kmh), 2) AS avg_wind
FROM readings
WHERE region = 'Europe'
  AND observation_date >= '2024-01-01'
  AND observation_date < '2025-01-01'
""").fetchone()
print(f"  Europe 2024 avg_temp: {europe_2024_detail[0]}")
print(f"  Europe 2024 avg_humidity: {europe_2024_detail[1]}")
print(f"  Europe 2024 avg_wind: {europe_2024_detail[2]}")

# ── Query 4: Per-station aggregation ──────────────────────────────────
print("\n=== Q4: Per-station aggregation ===")
station_stats = con.execute("""
SELECT station_id,
       COUNT(*) as reading_count,
       ROUND(AVG(temperature_c), 2) AS avg_temp,
       ROUND(AVG(humidity_pct), 2) AS avg_humidity
FROM readings
GROUP BY station_id ORDER BY station_id
""").fetchdf()
print(station_stats.to_string())
station_count = len(station_stats)
print(f"  ROW_COUNT = {station_count}")

# ── Query 5: Weather condition distribution ───────────────────────────
print("\n=== Q5: Condition distribution ===")
conditions = con.execute("""
SELECT condition, COUNT(*) as cnt
FROM readings
GROUP BY condition ORDER BY condition
""").fetchdf()
print(conditions.to_string())
cond_count = len(conditions)
print(f"  ROW_COUNT = {cond_count}")

# ── Query 6: Year-over-year ──────────────────────────────────────────
print("\n=== Q6: Year-over-year avg temperature ===")
yoy = con.execute("""
SELECT YEAR(observation_date) as obs_year,
       ROUND(AVG(temperature_c), 2) AS avg_temp,
       COUNT(*) as reading_count
FROM readings
GROUP BY YEAR(observation_date) ORDER BY obs_year
""").fetchdf()
print(yoy.to_string())
yoy_count = len(yoy)
print(f"  ROW_COUNT = {yoy_count}")

# ── Query 7: Extreme readings ────────────────────────────────────────
print("\n=== Q7: Extreme readings (>35 or <-5) ===")
extreme = con.execute("""
SELECT COUNT(*) as cnt
FROM readings
WHERE temperature_c > 35 OR temperature_c < -5
""").fetchone()[0]
print(f"  Extreme count: {extreme}")

# ── VERIFY: Grand totals ─────────────────────────────────────────────
print("\n=== VERIFY: Grand totals ===")
verify = con.execute("""
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT station_id) AS station_count,
    COUNT(DISTINCT condition) AS condition_count,
    SUM(CASE WHEN temperature_c > 35 OR temperature_c < -5 THEN 1 ELSE 0 END) AS extreme_count,
    SUM(CASE WHEN precipitation_mm IS NULL THEN 1 ELSE 0 END) AS null_precip_count
FROM readings
""").fetchone()
print(f"  total_rows: {verify[0]}")
print(f"  region_count: {verify[1]}")
print(f"  station_count: {verify[2]}")
print(f"  condition_count: {verify[3]}")
print(f"  extreme_count: {verify[4]}")
print(f"  null_precip_count: {verify[5]}")

print("\nDone!")
