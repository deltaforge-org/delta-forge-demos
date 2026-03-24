-- ============================================================================
-- String Statistics — Truncation & Bloom Filter Bridge — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.string_demos.product_catalog WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.string_demos;
DROP ZONE IF EXISTS {{zone_name}};
