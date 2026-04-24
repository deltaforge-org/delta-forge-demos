-- ==========================================================================
-- Demo: ORC Energy Meters — Utility Billing Analytics
-- Feature: Large dataset aggregation with HAVING, FILTER, COUNT DISTINCT
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.orc_energy
    COMMENT 'ORC-backed energy meter tables';

-- --------------------------------------------------------------------------
-- Table: readings — 3 monthly ORC files (1,500 rows total)
-- --------------------------------------------------------------------------
-- 50 meters × 10 days × 3 months. Columns:
--   string (meter_id, reading_timestamp, rate_plan)
--   float64 (kwh_consumed, voltage, power_factor — nullable)
--   bool (is_peak_hour)
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc_energy.readings
USING ORC
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.orc_energy.readings;
