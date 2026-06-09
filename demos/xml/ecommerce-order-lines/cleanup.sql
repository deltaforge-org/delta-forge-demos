-- ============================================================================
-- XML E-Commerce Order Line Explosion — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml_demos.order_lines WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml_demos.order_summary WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.xml_demos;
DROP ZONE IF EXISTS {{zone_name}};
