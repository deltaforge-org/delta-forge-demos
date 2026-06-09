-- ============================================================================
-- Delta MERGE — Composite Keys & Subquery Source — Educational Queries
-- ============================================================================
-- WHAT: MERGE INTO with a multi-column ON condition (composite key) and
--       a staging table as the source for atomic deduplication + upsert.
-- WHY:  Real-world tables almost always have composite keys — IoT telemetry
--       is identified by (device + timestamp), multi-tenant data by
--       (tenant + entity), fleet tracking by (vehicle + date). Single-column
--       MERGE demos do not prepare you for this. The composite ON condition
--       is how you match on multiple columns simultaneously.
-- HOW:  The ON clause joins target and source on BOTH vehicle_id AND
--       reading_date. Rows that match on both columns are updated;
--       rows in the source with no target match are inserted. The entire
--       operation is atomic — no partial state is ever visible.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Current Fleet Summary
-- ============================================================================
-- The fleet_daily_summary table has 20 rows: 5 vehicles x 4 days.
-- Each row is uniquely identified by (vehicle_id, reading_date) — neither
-- column alone is sufficient to identify a row.

ASSERT ROW_COUNT = 20
SELECT vehicle_id, reading_date, total_miles, total_fuel_gallons,
       avg_speed_mph, max_speed_mph, stop_count, idle_minutes, last_sync
FROM {{zone_name}}.delta_demos.fleet_daily_summary
ORDER BY vehicle_id, reading_date;


-- ============================================================================
-- PREVIEW: Incoming Telemetry Batch
-- ============================================================================
-- The telemetry_batch has 15 rows:
--   - 10 corrections for existing (vehicle_id, reading_date) pairs
--     (last_sync = '2025-03-05 08:00:00' — late-arriving GPS recalculations)
--   - 5 new rows for 2025-03-05 (all vehicles, end-of-day summaries)

ASSERT ROW_COUNT = 15
SELECT vehicle_id, reading_date, total_miles, total_fuel_gallons,
       avg_speed_mph, max_speed_mph, stop_count, idle_minutes, last_sync,
       CASE
           WHEN reading_date = '2025-03-05' THEN 'NEW'
           ELSE 'CORRECTION'
       END AS batch_type
FROM {{zone_name}}.delta_demos.telemetry_batch
ORDER BY vehicle_id, reading_date;


-- ============================================================================
-- MERGE: Composite Key Upsert
-- ============================================================================
-- KEY EDUCATIONAL POINT: The ON condition uses TWO columns.
--
--   ON target.vehicle_id = source.vehicle_id
--      AND target.reading_date = source.reading_date
--
-- This is the composite key match. A row is considered "matched" ONLY when
-- BOTH columns agree. If only vehicle_id matches but reading_date differs,
-- it is NOT a match — the source row will be inserted as a new record.
--
-- Expected outcome:
--   - 10 rows MATCHED (corrections for existing vehicle+date pairs) → UPDATE
--   - 5 rows NOT MATCHED (new 2025-03-05 data) → INSERT
--   - Total rows affected: 15

ASSERT ROW_COUNT = 15
MERGE INTO {{zone_name}}.delta_demos.fleet_daily_summary AS target
USING {{zone_name}}.delta_demos.telemetry_batch AS source
ON target.vehicle_id = source.vehicle_id AND target.reading_date = source.reading_date
WHEN MATCHED THEN
    UPDATE SET
        total_miles        = source.total_miles,
        total_fuel_gallons = source.total_fuel_gallons,
        avg_speed_mph      = source.avg_speed_mph,
        max_speed_mph      = source.max_speed_mph,
        stop_count         = source.stop_count,
        idle_minutes       = source.idle_minutes,
        last_sync          = source.last_sync
WHEN NOT MATCHED THEN
    INSERT (vehicle_id, reading_date, total_miles, total_fuel_gallons, avg_speed_mph,
            max_speed_mph, stop_count, idle_minutes, last_sync)
    VALUES (source.vehicle_id, source.reading_date, source.total_miles, source.total_fuel_gallons,
            source.avg_speed_mph, source.max_speed_mph, source.stop_count, source.idle_minutes,
            source.last_sync);


-- ============================================================================
-- EXPLORE: Fleet Summary After Merge
-- ============================================================================
-- The table now has 25 rows: the original 20 + 5 new rows for 2025-03-05.
-- The 10 corrected rows have updated values and last_sync = '2025-03-05 08:00:00'.

ASSERT ROW_COUNT = 25
SELECT vehicle_id, reading_date, total_miles, total_fuel_gallons,
       avg_speed_mph, max_speed_mph, stop_count, idle_minutes, last_sync
FROM {{zone_name}}.delta_demos.fleet_daily_summary
ORDER BY vehicle_id, reading_date;


-- ============================================================================
-- LEARN: Composite Key Matching — Corrections Applied
-- ============================================================================
-- The 10 corrected rows are identified by last_sync = '2025-03-05 08:00:00'.
-- These are existing (vehicle_id, reading_date) pairs whose readings were
-- updated by the MERGE because late-arriving GPS data changed the totals.
--
-- Notice VH-101 on 2025-03-03: total_miles went from 155.7 to 158.2
-- (a +2.5 mile correction from GPS recalculation).

ASSERT ROW_COUNT = 10
ASSERT VALUE total_miles = 158.2 WHERE vehicle_id = 'VH-101' AND reading_date = '2025-03-03'
SELECT vehicle_id, reading_date, total_miles, total_fuel_gallons,
       avg_speed_mph, max_speed_mph, stop_count, idle_minutes, last_sync
FROM {{zone_name}}.delta_demos.fleet_daily_summary
WHERE last_sync = '2025-03-05 08:00:00'
ORDER BY vehicle_id, reading_date;


-- ============================================================================
-- EXPLORE: Per-Vehicle Weekly Summary
-- ============================================================================
-- Aggregate across all 5 days per vehicle. Every vehicle now has exactly
-- 5 days of data (4 original + 1 new day from the batch).

ASSERT ROW_COUNT = 5
ASSERT VALUE total_miles = 709.3 WHERE vehicle_id = 'VH-101'
ASSERT VALUE total_miles = 552.2 WHERE vehicle_id = 'VH-102'
ASSERT VALUE total_miles = 881.5 WHERE vehicle_id = 'VH-103'
ASSERT VALUE total_miles = 461.0 WHERE vehicle_id = 'VH-104'
ASSERT VALUE total_miles = 805.8 WHERE vehicle_id = 'VH-105'
SELECT vehicle_id,
       COUNT(*)                           AS days_tracked,
       ROUND(SUM(total_miles), 2)         AS total_miles,
       ROUND(SUM(total_fuel_gallons), 2)  AS total_fuel,
       ROUND(AVG(avg_speed_mph), 2)       AS avg_speed,
       MAX(max_speed_mph)                 AS top_speed,
       SUM(stop_count)                    AS total_stops,
       SUM(idle_minutes)                  AS total_idle_min,
       ROUND(SUM(total_miles) / SUM(total_fuel_gallons), 2) AS miles_per_gallon
FROM {{zone_name}}.delta_demos.fleet_daily_summary
GROUP BY vehicle_id
ORDER BY vehicle_id;


-- ============================================================================
-- LEARN: New Day Inserted
-- ============================================================================
-- The 5 rows for 2025-03-05 had no matching composite key in the target
-- (no existing row with reading_date = '2025-03-05'), so the MERGE
-- inserted them via the WHEN NOT MATCHED clause.

ASSERT ROW_COUNT = 5
ASSERT VALUE total_miles = 145.8 WHERE vehicle_id = 'VH-101'
ASSERT VALUE total_miles = 110.2 WHERE vehicle_id = 'VH-102'
ASSERT VALUE total_miles = 178.6 WHERE vehicle_id = 'VH-103'
ASSERT VALUE total_miles = 93.5 WHERE vehicle_id = 'VH-104'
ASSERT VALUE total_miles = 162.3 WHERE vehicle_id = 'VH-105'
SELECT vehicle_id, reading_date, total_miles, total_fuel_gallons,
       avg_speed_mph, max_speed_mph, stop_count, idle_minutes, last_sync
FROM {{zone_name}}.delta_demos.fleet_daily_summary
WHERE reading_date = '2025-03-05'
ORDER BY vehicle_id;


-- ============================================================================
-- EXPLORE: Daily Fleet Totals
-- ============================================================================
-- Aggregate by reading_date across all 5 vehicles. We now have 5 days
-- of fleet-wide totals. The corrected days (03 and 04) reflect updated values.

ASSERT ROW_COUNT = 5
ASSERT VALUE fleet_miles = 662.2 WHERE reading_date = '2025-03-01'
ASSERT VALUE fleet_miles = 655.1 WHERE reading_date = '2025-03-02'
ASSERT VALUE fleet_miles = 711.0 WHERE reading_date = '2025-03-03'
ASSERT VALUE fleet_miles = 691.1 WHERE reading_date = '2025-03-04'
ASSERT VALUE fleet_miles = 690.4 WHERE reading_date = '2025-03-05'
SELECT reading_date,
       COUNT(*)                           AS vehicle_count,
       ROUND(SUM(total_miles), 2)         AS fleet_miles,
       ROUND(SUM(total_fuel_gallons), 2)  AS fleet_fuel,
       ROUND(AVG(avg_speed_mph), 2)       AS avg_fleet_speed,
       MAX(max_speed_mph)                 AS fleet_top_speed,
       SUM(stop_count)                    AS fleet_stops,
       SUM(idle_minutes)                  AS fleet_idle_min
FROM {{zone_name}}.delta_demos.fleet_daily_summary
GROUP BY reading_date
ORDER BY reading_date;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 20 original + 5 inserted = 25
ASSERT ROW_COUNT = 25
SELECT * FROM {{zone_name}}.delta_demos.fleet_daily_summary;

-- Verify corrected_rows: 10 rows updated with last_sync = '2025-03-05 08:00:00'
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.fleet_daily_summary
WHERE last_sync = '2025-03-05 08:00:00';

-- Verify new_day_rows: 5 rows inserted for reading_date = '2025-03-05'
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.fleet_daily_summary
WHERE reading_date = '2025-03-05';

-- Verify vh101_corrected: VH-101 on 2025-03-03 has corrected total_miles
ASSERT VALUE total_miles = 158.2
SELECT total_miles
FROM {{zone_name}}.delta_demos.fleet_daily_summary
WHERE vehicle_id = 'VH-101' AND reading_date = '2025-03-03';

-- Verify vh103_corrected: VH-103 on 2025-03-04 has corrected total_miles
ASSERT VALUE total_miles = 173.8
SELECT total_miles
FROM {{zone_name}}.delta_demos.fleet_daily_summary
WHERE vehicle_id = 'VH-103' AND reading_date = '2025-03-04';

-- Verify all_vehicles_five_days: Every vehicle has exactly 5 days of data
ASSERT ROW_COUNT = 5
ASSERT VALUE day_count = 5 WHERE vehicle_id = 'VH-101'
ASSERT VALUE day_count = 5 WHERE vehicle_id = 'VH-102'
ASSERT VALUE day_count = 5 WHERE vehicle_id = 'VH-103'
ASSERT VALUE day_count = 5 WHERE vehicle_id = 'VH-104'
ASSERT VALUE day_count = 5 WHERE vehicle_id = 'VH-105'
SELECT vehicle_id, COUNT(*) AS day_count
FROM {{zone_name}}.delta_demos.fleet_daily_summary
GROUP BY vehicle_id
ORDER BY vehicle_id;
