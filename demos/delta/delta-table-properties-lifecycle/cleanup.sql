-- ============================================================================
-- Delta Table Properties — Configuration Lifecycle — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.props_demos.inventory_items WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.props_demos;
DROP ZONE IF EXISTS {{zone_name}};
