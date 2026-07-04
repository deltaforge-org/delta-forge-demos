-- ==========================================================================
-- Delta VACUUM with Deletion Vectors: Cleanup Script
-- ==========================================================================
-- Removes all objects created by setup.sql.
-- ==========================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.fulfillment_orders WITH FILES;

-- Remove the per-demo wrapper folder (now empty after the table drop)
DROP FOLDER 'delta-vacuum-deletion-vectors' IF EXISTS IN ZONE {{zone_name}};

-- Shared resources (safe: will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
