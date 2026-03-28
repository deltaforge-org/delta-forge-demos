-- ============================================================================
-- Demo: ORC Energy Meters — Utility Billing Analytics
-- ============================================================================
-- Proves advanced aggregation features on a 1,500-row ORC dataset.
-- Covers HAVING, COUNT DISTINCT, FILTER clause, multi-aggregate GROUP BY,
-- and CTE-based analytics at volume.

-- ============================================================================
-- Query 1: Full Scan — 1,500 readings across 3 monthly files
-- ============================================================================

ASSERT ROW_COUNT = 1500
SELECT *
FROM {{zone_name}}.orc_energy.readings;

-- ============================================================================
-- Query 2: Rate Plan Distribution — GROUP BY with multiple aggregates
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE reading_count = 510 WHERE rate_plan = 'Commercial'
ASSERT VALUE reading_count = 510 WHERE rate_plan = 'Industrial'
ASSERT VALUE reading_count = 480 WHERE rate_plan = 'Residential'
SELECT rate_plan,
       COUNT(*) AS reading_count,
       ROUND(SUM(kwh_consumed), 3) AS total_kwh,
       ROUND(AVG(kwh_consumed), 3) AS avg_kwh
FROM {{zone_name}}.orc_energy.readings
GROUP BY rate_plan
ORDER BY reading_count DESC;

-- ============================================================================
-- Query 3: HAVING — meters with total consumption above 2,000 kWh
-- ============================================================================

ASSERT ROW_COUNT = 44
SELECT meter_id,
       ROUND(SUM(kwh_consumed), 3) AS total_kwh,
       COUNT(*) AS reading_count
FROM {{zone_name}}.orc_energy.readings
GROUP BY meter_id
HAVING SUM(kwh_consumed) > 2000
ORDER BY total_kwh DESC;

-- ============================================================================
-- Query 4: COUNT DISTINCT — unique meters and rate plans
-- ============================================================================

ASSERT VALUE distinct_meters = 50
ASSERT VALUE distinct_plans = 3
SELECT COUNT(DISTINCT meter_id) AS distinct_meters,
       COUNT(DISTINCT rate_plan) AS distinct_plans
FROM {{zone_name}}.orc_energy.readings;

-- ============================================================================
-- Query 5: FILTER clause — peak vs off-peak counts per rate plan
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE peak_count = 263 WHERE rate_plan = 'Commercial'
ASSERT VALUE peak_count = 261 WHERE rate_plan = 'Industrial'
ASSERT VALUE peak_count = 267 WHERE rate_plan = 'Residential'
SELECT rate_plan,
       COUNT(*) FILTER (WHERE is_peak_hour = true) AS peak_count,
       COUNT(*) FILTER (WHERE is_peak_hour = false) AS offpeak_count
FROM {{zone_name}}.orc_energy.readings
GROUP BY rate_plan
ORDER BY rate_plan;

-- ============================================================================
-- Query 6: NULL handling — power_factor NULL count
-- ============================================================================

ASSERT VALUE null_pf = 152
ASSERT VALUE non_null_pf = 1348
SELECT COUNT(*) FILTER (WHERE power_factor IS NULL) AS null_pf,
       COUNT(*) FILTER (WHERE power_factor IS NOT NULL) AS non_null_pf
FROM {{zone_name}}.orc_energy.readings;

-- ============================================================================
-- Query 7: Monthly Totals — file-level aggregation
-- ============================================================================

ASSERT ROW_COUNT = 3
SELECT df_file_name,
       COUNT(*) AS reading_count,
       ROUND(SUM(kwh_consumed), 3) AS monthly_kwh
FROM {{zone_name}}.orc_energy.readings
GROUP BY df_file_name
ORDER BY df_file_name;

-- ============================================================================
-- Query 8: CTE — Top 5 meters by consumption with rate plan context
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE meter_id = 'MTR-125' WHERE usage_rank = 1
WITH meter_totals AS (
    SELECT meter_id, rate_plan,
           ROUND(SUM(kwh_consumed), 3) AS total_kwh,
           COUNT(*) AS readings,
           ROW_NUMBER() OVER (ORDER BY SUM(kwh_consumed) DESC) AS usage_rank
    FROM {{zone_name}}.orc_energy.readings
    GROUP BY meter_id, rate_plan
)
SELECT meter_id, rate_plan, total_kwh, readings, usage_rank
FROM meter_totals
WHERE usage_rank <= 5
ORDER BY usage_rank;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_rows_1500'
ASSERT VALUE result = 'PASS' WHERE check_name = 'distinct_meters_50'
ASSERT VALUE result = 'PASS' WHERE check_name = 'high_usage_meters_44'
ASSERT VALUE result = 'PASS' WHERE check_name = 'peak_readings_791'
ASSERT VALUE result = 'PASS' WHERE check_name = 'null_power_factor_152'
SELECT check_name, result FROM (

    SELECT 'total_rows_1500' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_energy.readings) = 1500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'distinct_meters_50' AS check_name,
           CASE WHEN (
               SELECT COUNT(DISTINCT meter_id) FROM {{zone_name}}.orc_energy.readings
           ) = 50 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'high_usage_meters_44' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM (
                   SELECT meter_id FROM {{zone_name}}.orc_energy.readings
                   GROUP BY meter_id HAVING SUM(kwh_consumed) > 2000
               ) sub
           ) = 44 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'peak_readings_791' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_energy.readings WHERE is_peak_hour = true
           ) = 791 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'null_power_factor_152' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_energy.readings WHERE power_factor IS NULL
           ) = 152 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'three_files' AS check_name,
           CASE WHEN (
               SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.orc_energy.readings
           ) = 3 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
