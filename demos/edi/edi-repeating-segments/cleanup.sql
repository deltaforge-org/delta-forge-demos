-- ============================================================================
-- EDI Repeating Segments — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
--
-- The schema and zone are shared across demos. DROP SCHEMA / DROP ZONE will
-- succeed silently if they are empty, or produce a warning (not an error) if
-- other tables / schemas still exist — so it is always safe to leave them in.
-- ============================================================================

-- STEP 1: Drop External Tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi.repeating_indexed WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi.repeating_concat WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi.repeating_json WITH FILES;

-- STEP 2: Drop Schema
DROP SCHEMA IF EXISTS {{zone_name}}.edi;

-- STEP 3: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
