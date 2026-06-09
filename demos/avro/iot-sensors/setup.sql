-- ============================================================================
-- Avro IoT Sensors — Setup Script
-- ============================================================================
-- Creates three external tables from 5 building-floor Avro files:
--   1. all_readings   — All 5 files, common v1 schema (2,500 rows)
--   2. floor4_only    — Single v2 floor via file_filter (500 rows)
--   3. readings_sample — Sampled subset via max_rows (50 per file)
--
-- Demonstrates:
--   - Multi-file reading: 5 Avro files in one table
--   - Self-describing schema: Avro file headers provide types automatically
--   - Mixed compression codecs: null (floors 1,3,5) and deflate (floors 2,4)
--   - file_filter: isolate v2 files to access extra columns
--   - max_rows: limit rows per file for data profiling
--   - file_metadata: df_file_name + df_row_number system columns
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.avro_demos
    COMMENT 'Avro-backed external tables';

-- ============================================================================
-- TABLE 1: all_readings — All 5 files, common v1 schema (8 data columns)
-- ============================================================================
-- Reads all Avro files from the directory. The detected schema uses the
-- common v1 fields shared by all files: sensor_id, floor, zone, timestamp,
-- temperature_c, humidity_pct, co2_ppm, occupancy.
-- V2-only columns (battery_pct, firmware_version) require file_filter
-- to isolate v2 files — see floor4_only below.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.avro_demos.all_readings
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: floor4_only — Single floor via file_filter (500 rows)
-- ============================================================================
-- Uses file_filter to read only floor4_sensors.avro, which uses schema v2
-- (includes battery_pct and firmware_version) with deflate compression.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.avro_demos.floor4_only
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'floor4*',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 3: readings_sample — Data profiling via max_rows (50 per file)
-- ============================================================================
-- Limits to 50 rows per file for quick data profiling. With 5 files,
-- produces approximately 250 rows — enough to inspect data quality
-- without reading the full 2,500-row dataset.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.avro_demos.readings_sample
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    max_rows = '50',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
