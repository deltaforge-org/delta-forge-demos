-- ============================================================================
-- Delta Multi-Level Partitioning — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.quarter_dim WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.region_dim WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.sales_ml WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-multi-level-partitioning' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
