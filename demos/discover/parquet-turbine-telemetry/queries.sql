-- ============================================================================
-- Demo: Turbine Telemetry Auto-Onboarding - Queries
-- ============================================================================
-- The landing folder holds 50 SCADA telemetry readings across two Parquet
-- drops:
--   scada_2026_06_21.parquet - 35 readings (north-bank + south-bank sites)
--   scada_2026_06_22.parquet - 15 readings (east-bank site)
-- DISCOVER registers ONE external table over both files, lifting the column
-- names and types directly from the embedded Parquet schema. Every assertion
-- value below was precomputed from the data files, not the engine.
-- ============================================================================

-- ============================================================================
-- Query 1: DISCOVER in PRINT mode - review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the landed
-- files, recognises the Parquet container by its PAR1 magic number (not the
-- .parquet extension), and reads the embedded schema to name and type every
-- column. FILE_METADATA adds the df_file_name / df_row_number provenance
-- columns to the generated table.

DISCOVER {{zone_name}}.discover_demos.turbine_telemetry
    PATH 'parquet-turbine-telemetry'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode - auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). The
-- evidence column records the Parquet magic-number match across both files.
-- This single statement replaces a hand-written CREATE EXTERNAL TABLE block.

DISCOVER {{zone_name}}.discover_demos.turbine_telemetry
    PATH 'parquet-turbine-telemetry'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: The unified feed - all 50 readings from both Parquet drops
-- ============================================================================
-- One table spans both files. The discovered columns are exactly the field
-- names embedded in the Parquet schema.

ASSERT ROW_COUNT = 50
SELECT
    reading_id,
    turbine_id,
    site,
    wind_speed_mps,
    power_kw,
    rotor_rpm,
    status,
    df_file_name
FROM {{zone_name}}.discover_demos.turbine_telemetry;

-- ============================================================================
-- Query 4: Operating-state mix - readings per status
-- ============================================================================
-- Proves the status column decoded correctly across both files. Counts are
-- exact and precomputed from the data.

ASSERT ROW_COUNT = 3
ASSERT VALUE reading_count = 32 WHERE status = 'online'
ASSERT VALUE reading_count = 12 WHERE status = 'curtailed'
ASSERT VALUE reading_count = 6 WHERE status = 'fault'
SELECT
    status,
    COUNT(*) AS reading_count
FROM {{zone_name}}.discover_demos.turbine_telemetry
GROUP BY status
ORDER BY reading_count DESC;

-- ============================================================================
-- Query 5: Site spread - readings per wind-farm site
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE reading_count = 18 WHERE site = 'north-bank'
ASSERT VALUE reading_count = 17 WHERE site = 'south-bank'
ASSERT VALUE reading_count = 15 WHERE site = 'east-bank'
SELECT
    site,
    COUNT(*) AS reading_count
FROM {{zone_name}}.discover_demos.turbine_telemetry
GROUP BY site
ORDER BY reading_count DESC, site;

-- ============================================================================
-- Query 6: Fault watch - exact count of fault readings
-- ============================================================================
-- A single integer aggregate: how many readings landed in the 'fault' state.
-- Asserted exactly, not as a float.

ASSERT ROW_COUNT = 1
ASSERT VALUE fault_readings = 6
SELECT
    COUNT(*) FILTER (WHERE status = 'fault') AS fault_readings
FROM {{zone_name}}.discover_demos.turbine_telemetry;

-- ============================================================================
-- Query 7: Provenance - which Parquet drop each reading came from
-- ============================================================================
-- df_file_name comes from the FILE_METADATA = true discovery option. The
-- 06-21 drop contributes 35 readings; the 06-22 drop contributes 15.

ASSERT ROW_COUNT = 2
ASSERT VALUE rows_from_file = 35 WHERE source_drop = '2026-06-21 drop'
ASSERT VALUE rows_from_file = 15 WHERE source_drop = '2026-06-22 drop'
SELECT
    CASE
        WHEN df_file_name LIKE '%2026_06_22%' THEN '2026-06-22 drop'
        ELSE '2026-06-21 drop'
    END AS source_drop,
    COUNT(*) AS rows_from_file
FROM {{zone_name}}.discover_demos.turbine_telemetry
GROUP BY 1
ORDER BY rows_from_file DESC;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total reading volume, the
-- distinct turbine fleet, the three sites, the fault tail, and the two-file
-- union.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_readings = 50
ASSERT VALUE distinct_turbines = 8
ASSERT VALUE distinct_sites = 3
ASSERT VALUE fault_readings = 6
ASSERT VALUE distinct_source_files = 2
SELECT
    COUNT(*)                                          AS total_readings,
    COUNT(DISTINCT turbine_id)                        AS distinct_turbines,
    COUNT(DISTINCT site)                              AS distinct_sites,
    COUNT(*) FILTER (WHERE status = 'fault')          AS fault_readings,
    COUNT(DISTINCT df_file_name)                      AS distinct_source_files
FROM {{zone_name}}.discover_demos.turbine_telemetry;
