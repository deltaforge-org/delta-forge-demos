-- ============================================================================
-- Delta MERGE — Composite Keys & Subquery Source — Setup Script
-- ============================================================================
-- Creates the target and source tables for the composite-key MERGE demo.
--
-- Tables:
--   1. fleet_daily_summary — 20 rows (target), composite key: vehicle_id + reading_date
--      5 vehicles x 4 days (2025-03-01 through 2025-03-04)
--   2. telemetry_batch     — 15 rows (source): 10 corrections + 5 new day
--      10 rows overlap existing keys (late-arriving GPS corrections)
--      5 rows for new day 2025-03-05
--
-- The MERGE in queries.sql will:
--   - Update 10 existing rows (corrected telemetry readings)
--   - Insert 5 new rows (2025-03-05 data for all 5 vehicles)
--   - Final count: 20 + 5 = 25
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: fleet_daily_summary — 20 existing rows (target)
-- ============================================================================
-- Composite key: (vehicle_id, reading_date) — no single column uniquely
-- identifies a row. This is the standard pattern for time-series telemetry.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.fleet_daily_summary (
    vehicle_id         VARCHAR,
    reading_date       VARCHAR,
    total_miles        DOUBLE,
    total_fuel_gallons DOUBLE,
    avg_speed_mph      DOUBLE,
    max_speed_mph      DOUBLE,
    stop_count         INT,
    idle_minutes       INT,
    last_sync          VARCHAR
) LOCATION 'delta-merge-composite-keys/fleet_daily_summary';


INSERT INTO {{zone_name}}.delta_demos.fleet_daily_summary VALUES
    -- VH-101: Long-haul delivery van
    ('VH-101', '2025-03-01', 142.5, 18.2, 34.5, 62.0, 12, 45, '2025-03-01 23:00:00'),
    ('VH-101', '2025-03-02', 128.3, 16.8, 32.1, 58.0, 14, 52, '2025-03-02 23:00:00'),
    ('VH-101', '2025-03-03', 155.7, 19.5, 36.2, 65.0, 10, 38, '2025-03-03 23:00:00'),
    ('VH-101', '2025-03-04', 131.0, 17.1, 33.0, 60.0, 13, 48, '2025-03-04 23:00:00'),
    -- VH-102: Urban delivery truck
    ('VH-102', '2025-03-01',  98.4, 13.2, 28.5, 55.0, 18, 65, '2025-03-01 23:00:00'),
    ('VH-102', '2025-03-02', 112.6, 14.8, 30.2, 57.0, 16, 58, '2025-03-02 23:00:00'),
    ('VH-102', '2025-03-03', 105.1, 13.9, 29.0, 54.0, 17, 62, '2025-03-03 23:00:00'),
    ('VH-102', '2025-03-04', 118.9, 15.3, 31.5, 59.0, 15, 55, '2025-03-04 23:00:00'),
    -- VH-103: Highway freight carrier
    ('VH-103', '2025-03-01', 175.2, 22.5, 38.0, 68.0,  8, 30, '2025-03-01 23:00:00'),
    ('VH-103', '2025-03-02', 168.9, 21.8, 37.2, 66.0,  9, 33, '2025-03-02 23:00:00'),
    ('VH-103', '2025-03-03', 182.4, 23.1, 39.5, 70.0,  7, 28, '2025-03-03 23:00:00'),
    ('VH-103', '2025-03-04', 170.5, 22.0, 37.8, 67.0,  8, 31, '2025-03-04 23:00:00'),
    -- VH-104: Local delivery van (short routes, many stops)
    ('VH-104', '2025-03-01',  85.3, 11.5, 25.0, 48.0, 22, 78, '2025-03-01 23:00:00'),
    ('VH-104', '2025-03-02',  92.1, 12.3, 26.8, 50.0, 20, 72, '2025-03-02 23:00:00'),
    ('VH-104', '2025-03-03',  88.7, 11.9, 25.5, 49.0, 21, 75, '2025-03-03 23:00:00'),
    ('VH-104', '2025-03-04',  95.6, 12.7, 27.2, 52.0, 19, 70, '2025-03-04 23:00:00'),
    -- VH-105: Regional courier
    ('VH-105', '2025-03-01', 160.8, 20.5, 36.0, 64.0,  9, 35, '2025-03-01 23:00:00'),
    ('VH-105', '2025-03-02', 153.2, 19.8, 35.0, 63.0, 10, 40, '2025-03-02 23:00:00'),
    ('VH-105', '2025-03-03', 165.5, 21.0, 37.0, 66.0,  8, 32, '2025-03-03 23:00:00'),
    ('VH-105', '2025-03-04', 158.0, 20.2, 35.5, 64.0,  9, 36, '2025-03-04 23:00:00');


-- ============================================================================
-- TABLE 2: telemetry_batch — 15 incoming readings (source)
-- ============================================================================
-- This batch represents late-arriving GPS corrections and new-day data.
-- 10 rows overlap existing composite keys → will be MATCHED (updates)
-- 5 rows are for a new day (2025-03-05) → will be NOT MATCHED (inserts)
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.telemetry_batch (
    vehicle_id         VARCHAR,
    reading_date       VARCHAR,
    total_miles        DOUBLE,
    total_fuel_gallons DOUBLE,
    avg_speed_mph      DOUBLE,
    max_speed_mph      DOUBLE,
    stop_count         INT,
    idle_minutes       INT,
    last_sync          VARCHAR
) LOCATION 'delta-merge-composite-keys/telemetry_batch';


INSERT INTO {{zone_name}}.delta_demos.telemetry_batch VALUES
    -- Corrections for existing data (late-arriving GPS recalculations)
    ('VH-101', '2025-03-03', 158.2, 19.8, 36.8, 67.0, 10, 36, '2025-03-05 08:00:00'),  -- corrected: +2.5 miles, higher max speed
    ('VH-101', '2025-03-04', 134.5, 17.5, 33.8, 63.0, 12, 44, '2025-03-05 08:00:00'),  -- corrected: +3.5 miles
    ('VH-102', '2025-03-03', 108.6, 14.3, 29.8, 56.0, 16, 59, '2025-03-05 08:00:00'),  -- corrected: +3.5 miles
    ('VH-102', '2025-03-04', 122.4, 15.8, 32.0, 61.0, 14, 52, '2025-03-05 08:00:00'),  -- corrected: +3.5 miles
    ('VH-103', '2025-03-03', 185.0, 23.5, 40.0, 72.0,  7, 26, '2025-03-05 08:00:00'),  -- corrected: +2.6 miles
    ('VH-103', '2025-03-04', 173.8, 22.4, 38.2, 69.0,  8, 29, '2025-03-05 08:00:00'),  -- corrected: +3.3 miles
    ('VH-104', '2025-03-03',  91.2, 12.2, 26.0, 51.0, 20, 73, '2025-03-05 08:00:00'),  -- corrected: +2.5 miles
    ('VH-104', '2025-03-04',  98.9, 13.1, 27.8, 54.0, 18, 67, '2025-03-05 08:00:00'),  -- corrected: +3.3 miles
    ('VH-105', '2025-03-03', 168.0, 21.3, 37.5, 68.0,  8, 30, '2025-03-05 08:00:00'),  -- corrected: +2.5 miles
    ('VH-105', '2025-03-04', 161.5, 20.6, 36.0, 66.0,  9, 34, '2025-03-05 08:00:00'),  -- corrected: +3.5 miles
    -- New data for 2025-03-05 (end-of-day summaries)
    ('VH-101', '2025-03-05', 145.8, 18.8, 35.0, 64.0, 11, 42, '2025-03-05 23:00:00'),
    ('VH-102', '2025-03-05', 110.2, 14.5, 29.5, 56.0, 17, 60, '2025-03-05 23:00:00'),
    ('VH-103', '2025-03-05', 178.6, 22.8, 38.5, 69.0,  8, 29, '2025-03-05 23:00:00'),
    ('VH-104', '2025-03-05',  93.5, 12.5, 26.5, 50.0, 20, 74, '2025-03-05 23:00:00'),
    ('VH-105', '2025-03-05', 162.3, 20.8, 36.5, 65.0,  9, 33, '2025-03-05 23:00:00');
