-- ============================================================================
-- Parquet Flight Delays — Setup Script
-- ============================================================================
-- Creates two external tables from 3 quarterly Parquet files (Q1–Q3 2025):
--   1. all_flights  — All 3 files with schema evolution (120 rows)
--   2. q1_flights   — Q1 only via file_filter (40 rows)
--
-- Demonstrates:
--   - Schema evolution: Q1 has base columns, Q2 adds delay_reason,
--     Q3 adds carrier_code. Missing columns are NULL-filled automatically.
--   - file_filter: glob pattern to select a single quarter
--   - file_metadata: df_file_name + df_row_number system columns
--   - Self-describing schema: Parquet metadata provides types automatically
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.parquet_flights
    COMMENT 'Parquet-backed flight delay tables with schema evolution';

-- ============================================================================
-- TABLE 1: all_flights — All 3 quarterly files (schema evolution)
-- ============================================================================
-- Reads flights_2025_q1.parquet, flights_2025_q2.parquet, and
-- flights_2025_q3.parquet. Columns added in later quarters (delay_reason
-- in Q2, carrier_code in Q3) are NULL-filled for earlier files.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.parquet_flights.all_flights
USING PARQUET
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: q1_flights — Q1 only via file_filter (40 rows)
-- ============================================================================
-- Uses file_filter to read only the Q1 file. This table will NOT have
-- delay_reason or carrier_code columns since Q1 predates those additions.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.parquet_flights.q1_flights
USING PARQUET
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '*q1*',
    file_metadata = '{"columns":["df_file_name"]}'
);
