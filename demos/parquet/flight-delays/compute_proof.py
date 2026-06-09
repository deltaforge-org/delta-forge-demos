"""Compute proof values for ASSERT statements in queries.sql."""

import duckdb
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
con = duckdb.connect()

# Read all 3 files
con.execute(f"CREATE VIEW q1 AS SELECT *, NULL::VARCHAR AS delay_reason, NULL::VARCHAR AS carrier_code FROM read_parquet('{DATA_DIR}/flights_2025_q1.parquet')")
con.execute(f"CREATE VIEW q2 AS SELECT *, NULL::VARCHAR AS carrier_code FROM read_parquet('{DATA_DIR}/flights_2025_q2.parquet')")
con.execute(f"CREATE VIEW q3 AS SELECT * FROM read_parquet('{DATA_DIR}/flights_2025_q3.parquet')")
con.execute("CREATE VIEW all_flights AS SELECT * FROM q1 UNION ALL SELECT * FROM q2 UNION ALL SELECT * FROM q3")

print("=== QUERY 1: Full scan total ===")
r = con.execute("SELECT COUNT(*) FROM all_flights").fetchone()
print(f"  total_rows = {r[0]}")

print("\n=== QUERY 2: Q1 only ===")
r = con.execute("SELECT COUNT(*) FROM q1").fetchone()
print(f"  q1_rows = {r[0]}")

print("\n=== QUERY 3: Schema evolution — NULL counts by quarter ===")
rows = con.execute("""
    SELECT 'q1' AS quarter,
           COUNT(*) FILTER (WHERE delay_reason IS NULL) AS null_delay_reason,
           COUNT(*) FILTER (WHERE carrier_code IS NULL) AS null_carrier_code,
           COUNT(*) AS total
    FROM q1
    UNION ALL
    SELECT 'q2',
           COUNT(*) FILTER (WHERE delay_reason IS NULL),
           COUNT(*) FILTER (WHERE carrier_code IS NULL),
           COUNT(*)
    FROM q2
    UNION ALL
    SELECT 'q3',
           COUNT(*) FILTER (WHERE delay_reason IS NULL),
           COUNT(*) FILTER (WHERE carrier_code IS NULL),
           COUNT(*)
    FROM q3
    ORDER BY quarter
""").fetchall()
for row in rows:
    print(f"  {row[0]}: null_delay_reason={row[1]}, null_carrier_code={row[2]}, total={row[3]}")

print("\n=== QUERY 4: Airline performance ===")
rows = con.execute("""
    SELECT airline,
           ROUND(AVG(delay_minutes), 1) AS avg_delay,
           COUNT(*) AS total_flights,
           COUNT(*) FILTER (WHERE status = 'On Time') AS on_time,
           COUNT(*) FILTER (WHERE status = 'Delayed') AS delayed,
           COUNT(*) FILTER (WHERE status = 'Cancelled') AS cancelled
    FROM all_flights
    GROUP BY airline
    ORDER BY avg_delay DESC
""").fetchall()
num_airlines = len(rows)
print(f"  ROW_COUNT = {num_airlines}")
for row in rows:
    print(f"  {row[0]}: avg_delay={row[1]}, total={row[2]}, on_time={row[3]}, delayed={row[4]}, cancelled={row[5]}")

print("\n=== QUERY 5: Top 5 busiest routes by passenger count ===")
rows = con.execute("""
    SELECT origin, destination,
           SUM(passengers) AS total_passengers,
           COUNT(*) AS flight_count
    FROM all_flights
    GROUP BY origin, destination
    ORDER BY total_passengers DESC
    LIMIT 5
""").fetchall()
for row in rows:
    print(f"  {row[0]} -> {row[1]}: passengers={row[2]}, flights={row[3]}")

print("\n=== QUERY 6: Delay reason breakdown (Q2+Q3, non-NULL only) ===")
rows = con.execute("""
    SELECT delay_reason, COUNT(*) AS cnt,
           ROUND(AVG(delay_minutes), 1) AS avg_delay
    FROM all_flights
    WHERE delay_reason IS NOT NULL
    GROUP BY delay_reason
    ORDER BY cnt DESC
""").fetchall()
num_reasons = len(rows)
print(f"  ROW_COUNT = {num_reasons}")
for row in rows:
    print(f"  {row[0]}: count={row[1]}, avg_delay={row[2]}")

print("\n=== QUERY 7: File metadata — distinct file count ===")
print("  (file_metadata comes from DeltaForge, not duckdb — just verify 3 files expected)")

print("\n=== VERIFY: Grand totals ===")
r = con.execute("""
    SELECT
        COUNT(*) AS total_rows,
        ROUND(AVG(delay_minutes), 2) AS avg_delay,
        SUM(passengers) AS total_passengers,
        COUNT(*) FILTER (WHERE delay_reason IS NULL) AS null_delay_reasons,
        COUNT(*) FILTER (WHERE carrier_code IS NULL) AS null_carrier_codes
    FROM all_flights
""").fetchone()
print(f"  total_rows = {r[0]}")
print(f"  avg_delay = {r[1]}")
print(f"  total_passengers = {r[2]}")
print(f"  null_delay_reasons = {r[3]}")
print(f"  null_carrier_codes = {r[4]}")

# Additional: check cancelled count
r2 = con.execute("SELECT COUNT(*) FROM all_flights WHERE status = 'Cancelled'").fetchone()
print(f"  cancelled_count = {r2[0]}")

# Check on_time count
r3 = con.execute("SELECT COUNT(*) FROM all_flights WHERE status = 'On Time'").fetchone()
print(f"  on_time_count = {r3[0]}")

# Check delayed count
r4 = con.execute("SELECT COUNT(*) FROM all_flights WHERE status = 'Delayed'").fetchone()
print(f"  delayed_count = {r4[0]}")

# Top route
r5 = con.execute("""
    SELECT origin || '->' || destination AS route, SUM(passengers) AS p
    FROM all_flights GROUP BY route ORDER BY p DESC LIMIT 1
""").fetchone()
print(f"  top_route = {r5[0]}, passengers = {r5[1]}")
