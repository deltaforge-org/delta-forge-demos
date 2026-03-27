"""Generate 3 Parquet files with schema evolution for the flight-delays demo."""

import duckdb
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

con = duckdb.connect()

# Helper: on-time indices (about 50% on-time per quarter)
# Q1: i=1..40, on-time if i in {1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39} (odd)
# Cancelled: i in {2,20} for Q1, {42,60} for Q2, {82,100} for Q3
# Delayed: all even except cancelled

# --- Q1: Jan-Mar 2025, basic schema, rows 1-40 ---
con.execute("""
CREATE TABLE q1 AS
SELECT
    'FL-' || LPAD(CAST(i AS VARCHAR), 4, '0') AS flight_id,
    (CASE (i - 1) % 5
        WHEN 0 THEN 'American Airlines'
        WHEN 1 THEN 'United Airlines'
        WHEN 2 THEN 'Delta Air Lines'
        WHEN 3 THEN 'Southwest Airlines'
        WHEN 4 THEN 'JetBlue'
    END) AS airline,
    (['JFK','LAX','ORD','ATL','DFW','SFO','MIA','SEA'])[(((i - 1) % 8) + 1)] AS origin,
    (['JFK','LAX','ORD','ATL','DFW','SFO','MIA','SEA'])[(((i + 3) % 8) + 1)] AS destination,
    CAST(DATE '2025-01-01' + INTERVAL ((i - 1) * 2) DAY AS DATE) AS departure_date,
    TIME '06:00:00' + INTERVAL ((i * 37) % 720) MINUTE AS scheduled_time,
    TIME '06:00:00' + INTERVAL (((i * 37) % 720) + (CASE
        WHEN i % 2 = 1 THEN 0  -- odd = on-time
        WHEN i IN (2,20) THEN 0  -- cancelled (delay_minutes=0)
        ELSE ((i * 13 + 7) % 170) + 10
    END)) MINUTE AS actual_time,
    (CASE
        WHEN i % 2 = 1 THEN 0
        WHEN i IN (2,20) THEN 0
        ELSE ((i * 13 + 7) % 170) + 10
    END)::INTEGER AS delay_minutes,
    (80 + (i * 41) % 141)::INTEGER AS passengers,
    (CASE
        WHEN i IN (2,20) THEN 'Cancelled'
        WHEN i % 2 = 1 THEN 'On Time'
        ELSE 'Delayed'
    END) AS status
FROM generate_series(1, 40) AS t(i)
""")

# --- Q2: Apr-Jun 2025, adds delay_reason, rows 41-80 ---
con.execute("""
CREATE TABLE q2 AS
SELECT
    'FL-' || LPAD(CAST(i AS VARCHAR), 4, '0') AS flight_id,
    (CASE (i - 1) % 5
        WHEN 0 THEN 'American Airlines'
        WHEN 1 THEN 'United Airlines'
        WHEN 2 THEN 'Delta Air Lines'
        WHEN 3 THEN 'Southwest Airlines'
        WHEN 4 THEN 'JetBlue'
    END) AS airline,
    (['JFK','LAX','ORD','ATL','DFW','SFO','MIA','SEA'])[(((i - 1) % 8) + 1)] AS origin,
    (['JFK','LAX','ORD','ATL','DFW','SFO','MIA','SEA'])[(((i + 3) % 8) + 1)] AS destination,
    CAST(DATE '2025-04-01' + INTERVAL (((i - 41) * 2)) DAY AS DATE) AS departure_date,
    TIME '06:00:00' + INTERVAL ((i * 37) % 720) MINUTE AS scheduled_time,
    TIME '06:00:00' + INTERVAL (((i * 37) % 720) + (CASE
        WHEN i % 2 = 1 THEN 0
        WHEN i IN (42,60) THEN 0
        ELSE ((i * 13 + 7) % 170) + 10
    END)) MINUTE AS actual_time,
    (CASE
        WHEN i % 2 = 1 THEN 0
        WHEN i IN (42,60) THEN 0
        ELSE ((i * 13 + 7) % 170) + 10
    END)::INTEGER AS delay_minutes,
    (80 + (i * 41) % 141)::INTEGER AS passengers,
    (CASE
        WHEN i IN (42,60) THEN 'Cancelled'
        WHEN i % 2 = 1 THEN 'On Time'
        ELSE 'Delayed'
    END) AS status,
    (CASE
        WHEN i % 2 = 1 THEN NULL  -- on-time flights have no delay reason
        WHEN i IN (42,60) THEN NULL  -- cancelled flights have no delay reason
        ELSE (['Weather','Mechanical','ATC','Crew'])[(((i // 2) % 4) + 1)]
    END)::VARCHAR AS delay_reason
FROM generate_series(41, 80) AS t(i)
""")

# --- Q3: Jul-Sep 2025, adds carrier_code + delay_reason, rows 81-120 ---
con.execute("""
CREATE TABLE q3 AS
SELECT
    'FL-' || LPAD(CAST(i AS VARCHAR), 4, '0') AS flight_id,
    (CASE (i - 1) % 5
        WHEN 0 THEN 'American Airlines'
        WHEN 1 THEN 'United Airlines'
        WHEN 2 THEN 'Delta Air Lines'
        WHEN 3 THEN 'Southwest Airlines'
        WHEN 4 THEN 'JetBlue'
    END) AS airline,
    (['JFK','LAX','ORD','ATL','DFW','SFO','MIA','SEA'])[(((i - 1) % 8) + 1)] AS origin,
    (['JFK','LAX','ORD','ATL','DFW','SFO','MIA','SEA'])[(((i + 3) % 8) + 1)] AS destination,
    CAST(DATE '2025-07-01' + INTERVAL (((i - 81) * 2)) DAY AS DATE) AS departure_date,
    TIME '06:00:00' + INTERVAL ((i * 37) % 720) MINUTE AS scheduled_time,
    TIME '06:00:00' + INTERVAL (((i * 37) % 720) + (CASE
        WHEN i % 2 = 1 THEN 0
        WHEN i IN (82,100) THEN 0
        ELSE ((i * 13 + 7) % 170) + 10
    END)) MINUTE AS actual_time,
    (CASE
        WHEN i % 2 = 1 THEN 0
        WHEN i IN (82,100) THEN 0
        ELSE ((i * 13 + 7) % 170) + 10
    END)::INTEGER AS delay_minutes,
    (80 + (i * 41) % 141)::INTEGER AS passengers,
    (CASE
        WHEN i IN (82,100) THEN 'Cancelled'
        WHEN i % 2 = 1 THEN 'On Time'
        ELSE 'Delayed'
    END) AS status,
    (CASE
        WHEN i % 2 = 1 THEN NULL
        WHEN i IN (82,100) THEN NULL
        ELSE (['Weather','Mechanical','ATC','Crew'])[(((i // 2) % 4) + 1)]
    END)::VARCHAR AS delay_reason,
    (['AA','UA','DL','SW','B6'])[((i - 1) % 5 + 1)]::VARCHAR AS carrier_code
FROM generate_series(81, 120) AS t(i)
""")

# Write Parquet files
con.execute(f"COPY q1 TO '{DATA_DIR}/flights_2025_q1.parquet' (FORMAT PARQUET)")
con.execute(f"COPY q2 TO '{DATA_DIR}/flights_2025_q2.parquet' (FORMAT PARQUET)")
con.execute(f"COPY q3 TO '{DATA_DIR}/flights_2025_q3.parquet' (FORMAT PARQUET)")

# Verify
for q, name in [("q1", "Q1"), ("q2", "Q2"), ("q3", "Q3")]:
    cnt = con.execute(f"SELECT COUNT(*) FROM {q}").fetchone()[0]
    print(f"{name}: {cnt} rows")

print("\nParquet files written successfully.")
