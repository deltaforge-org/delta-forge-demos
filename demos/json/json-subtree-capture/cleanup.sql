-- ============================================================================
-- JSON Subtree Capture — Cleanup Script
-- ============================================================================
-- Drops tables in dependency order (tables → schema → zone).
-- Both tables are EXTERNAL so WITH FILES removes the source files too.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json_demos.listings_captured WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json_demos.listings_flattened WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.json_demos;

DROP ZONE IF EXISTS {{zone_name}};
