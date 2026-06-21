-- ============================================================================
-- Demo: CDN Edge Access Logs Auto-Onboarding, DISCOVER over ORC
-- Feature: DISCOVER auto-detects the file format from raw bytes, reads the
--          embedded ORC schema, and registers an external table in one
--          statement. No hand-written CREATE EXTERNAL TABLE, no OPTIONS.
-- ============================================================================
--
-- Real-world story: a CDN provider archives its edge access logs in columnar
-- ORC, one file per day per region rollup. ORC carries its own schema in the
-- file footer, so the column names and types are already known the moment the
-- file lands. DISCOVER recognises ORC by its trailing 'ORC' magic marker (not
-- the file extension), reads the embedded schema, and registers the table from
-- it directly.
--
-- This is the ORC entry in the DISCOVER category. Rather than hand-write a
-- CREATE EXTERNAL TABLE with explicit columns, this demo points DISCOVER at the
-- folder and lets it read the ORC footer: the discovered columns are exactly
-- the embedded field names (authored lowercase snake_case so they round-trip
-- unchanged), with types taken straight from the ORC schema.
--
-- This file declares ONLY the zone and schema. The external table itself is
-- created by DISCOVER in queries.sql (both PRINT and EXECUTE modes are shown
-- there), mirroring the discover/json-clickstream demo.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.discover_demos
    COMMENT 'Tables auto-registered by the DISCOVER command';
