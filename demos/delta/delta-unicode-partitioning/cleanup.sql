-- ============================================================================
-- Delta Unicode Partitioning — Non-ASCII Partition Keys — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.cdn_content WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
