-- ============================================================================
-- Demo: Turbine Telemetry Auto-Onboarding, DISCOVER over Parquet
-- Feature: DISCOVER auto-detects the file format from raw bytes (the Parquet
--          PAR1 magic number, not the file extension), reads the embedded
--          schema, and registers an external table in one statement. No
--          hand-written CREATE EXTERNAL TABLE, no OPTIONS.
-- ============================================================================
--
-- Real-world story: an offshore wind operator lands per-turbine SCADA
-- telemetry as Parquet into a single landing folder. Each drop is a columnar
-- Parquet file carrying its own schema (column names and types) in the
-- footer. Operations does not want to hand-maintain a CREATE EXTERNAL TABLE
-- for a feed whose shape is already self-describing on disk.
--
-- This is the Parquet entry in the DISCOVER category. Rather than declare the
-- columns by hand, this demo points DISCOVER at the folder and lets it work:
-- it reads the actual bytes, recognises the Parquet binary container by its
-- PAR1 magic number (classification is byte-based, not extension-based),
-- lifts the field names and types straight from the embedded schema, and
-- registers the table with zero OPTIONS.
--
-- This file declares ONLY the zone and schema. The external table itself is
-- created by DISCOVER in queries.sql (both PRINT and EXECUTE modes are shown
-- there), mirroring the json-clickstream demo.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.discover_demos
    COMMENT 'Tables auto-registered by the DISCOVER command';
