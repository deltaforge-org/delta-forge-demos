-- ============================================================================
-- Parquet Flight Delays — Verification Queries
-- ============================================================================
-- Each query verifies a specific aspect of Parquet schema evolution,
-- NULL filling, file_filter, file_metadata, and analytical capabilities.
-- ============================================================================


-- ============================================================================
-- 1. FULL SCAN — 120 flights across 3 quarterly files
-- ============================================================================
-- Verifies all 3 files are read. Columns delay_reason (added Q2) and
-- carrier_code (added Q3) will be NULL for earlier quarters.

ASSERT ROW_COUNT = 120
SELECT *
FROM {{zone_name}}.parquet_flights.all_flights;


-- ============================================================================
-- 2. Q1-ONLY FILTER — 40 flights from January–March 2025
-- ============================================================================
-- Verifies file_filter '*q1*' correctly selects only the Q1 file.

ASSERT ROW_COUNT = 40
SELECT *
FROM {{zone_name}}.parquet_flights.q1_flights;


-- ============================================================================
-- 3. SCHEMA EVOLUTION PROOF — NULL counts by source file
-- ============================================================================
-- Q1 file: delay_reason = ALL NULL (40), carrier_code = ALL NULL (40)
-- Q2 file: delay_reason = 22 NULL (on-time + cancelled), carrier_code = ALL NULL (40)
-- Q3 file: delay_reason = 22 NULL (on-time + cancelled), carrier_code = 0 NULL

ASSERT ROW_COUNT = 3
SELECT df_file_name,
       COUNT(*) AS total_rows,
       COUNT(*) FILTER (WHERE delay_reason IS NULL) AS null_delay_reason,
       COUNT(*) FILTER (WHERE carrier_code IS NULL) AS null_carrier_code
FROM {{zone_name}}.parquet_flights.all_flights
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. AIRLINE PERFORMANCE — Average delay and status breakdown
-- ============================================================================
-- Groups by airline to compare on-time performance, average delays,
-- and cancellation rates across the 5 carriers.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_delay = 52.0 WHERE airline = 'Southwest Airlines'
SELECT airline,
       ROUND(AVG(delay_minutes), 1) AS avg_delay,
       COUNT(*) AS total_flights,
       COUNT(*) FILTER (WHERE status = 'On Time') AS on_time,
       COUNT(*) FILTER (WHERE status = 'Delayed') AS delayed,
       COUNT(*) FILTER (WHERE status = 'Cancelled') AS cancelled
FROM {{zone_name}}.parquet_flights.all_flights
GROUP BY airline
ORDER BY avg_delay DESC;


-- ============================================================================
-- 5. ROUTE ANALYSIS — Top 5 busiest routes by total passengers
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE total_passengers = 2490 WHERE origin = 'SEA' AND destination = 'ATL'
SELECT origin, destination,
       SUM(passengers) AS total_passengers,
       COUNT(*) AS flight_count
FROM {{zone_name}}.parquet_flights.all_flights
GROUP BY origin, destination
ORDER BY total_passengers DESC
LIMIT 5;


-- ============================================================================
-- 6. DELAY REASON BREAKDOWN — Q2+Q3 flights with known delay reasons
-- ============================================================================
-- Only Q2 and Q3 files contain the delay_reason column. On-time and
-- cancelled flights have NULL delay_reason even in those files.

ASSERT ROW_COUNT = 4
ASSERT VALUE cnt = 10 WHERE delay_reason = 'Weather'
SELECT delay_reason,
       COUNT(*) AS cnt,
       ROUND(AVG(delay_minutes), 1) AS avg_delay
FROM {{zone_name}}.parquet_flights.all_flights
WHERE delay_reason IS NOT NULL
GROUP BY delay_reason
ORDER BY cnt DESC;


-- ============================================================================
-- 7. FILE METADATA — Verify df_file_name values (3 distinct files)
-- ============================================================================

ASSERT ROW_COUNT = 3
SELECT df_file_name, COUNT(*) AS row_count
FROM {{zone_name}}.parquet_flights.all_flights
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- VERIFY: Grand totals — cross-cutting sanity checks
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_rows_120'
ASSERT VALUE result = 'PASS' WHERE check_name = 'avg_delay_42_33'
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_passengers_18069'
ASSERT VALUE result = 'PASS' WHERE check_name = 'null_delay_reasons_84'
ASSERT VALUE result = 'PASS' WHERE check_name = 'null_carrier_codes_80'
ASSERT VALUE result = 'PASS' WHERE check_name = 'q1_filter_40'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_count_3'
SELECT check_name, result FROM (

    -- Check 1: Total row count = 120
    SELECT 'total_rows_120' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.parquet_flights.all_flights) = 120
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Average delay = 42.33
    SELECT 'avg_delay_42_33' AS check_name,
           CASE WHEN (SELECT ROUND(AVG(delay_minutes), 2) FROM {{zone_name}}.parquet_flights.all_flights) = 42.33
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Total passengers = 18,069
    SELECT 'total_passengers_18069' AS check_name,
           CASE WHEN (SELECT SUM(passengers) FROM {{zone_name}}.parquet_flights.all_flights) = 18069
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: NULL delay_reasons = 84 (40 from Q1 + 22 from Q2 + 22 from Q3)
    SELECT 'null_delay_reasons_84' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.parquet_flights.all_flights WHERE delay_reason IS NULL) = 84
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: NULL carrier_codes = 80 (40 from Q1 + 40 from Q2)
    SELECT 'null_carrier_codes_80' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.parquet_flights.all_flights WHERE carrier_code IS NULL) = 80
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Q1 file_filter returns 40 rows
    SELECT 'q1_filter_40' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.parquet_flights.q1_flights) = 40
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: 3 distinct source files
    SELECT 'file_count_3' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.parquet_flights.all_flights) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
