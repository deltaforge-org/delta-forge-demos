-- ============================================================================
-- Demo: Product Clickstream Auto-Onboarding, DISCOVER over JSON + NDJSON
-- Feature: DISCOVER auto-detects the file format from raw bytes, synthesizes
--          the json_flatten_config, and registers an external table in one
--          statement. No hand-written CREATE EXTERNAL TABLE, no OPTIONS.
-- ============================================================================
--
-- Real-world story: a SaaS product-analytics team lands its web clickstream
-- in a single landing folder. The quarterly backfill arrives as one JSON
-- array (events_2026_q1.json, 30 events) and the April increment arrives as
-- newline-delimited JSON (events_2026_apr.ndjson, 10 events). Two physical
-- shapes, one logical feed.
--
-- This is the JSON entry in the DISCOVER category. Rather than hand-write a
-- CREATE EXTERNAL TABLE with an explicit json_flatten_config (the approach
-- every other JSON demo takes), this demo points DISCOVER at the folder and
-- lets it do the work: it reads the actual bytes (text-shape classification,
-- not the file extension), recognises that .json (a top-level array) and .ndjson (one object per
-- line) both register USING JSON, unions
-- their discovered paths into one synthesized config, and registers the table.
--
-- This file declares ONLY the zone and schema. The external table itself is
-- created by DISCOVER in queries.sql (both PRINT and EXECUTE modes are
-- shown there), mirroring the api/openbrewerydb-territory-discovery demo.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.discover_demos
    COMMENT 'Tables auto-registered by the DISCOVER command';
