-- ============================================================================
-- Delta Views & Data Masking — Role-Based Access Layers — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql and queries.sql.
-- Views must be dropped before the base table.
-- ============================================================================

DROP VIEW IF EXISTS {{zone_name}}.delta_demos.orders_executive;
DROP VIEW IF EXISTS {{zone_name}}.delta_demos.orders_support;
DROP VIEW IF EXISTS {{zone_name}}.delta_demos.orders_analyst;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.customer_orders WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-views-data-masking' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
