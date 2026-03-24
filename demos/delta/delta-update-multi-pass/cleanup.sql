-- ============================================================================
-- Delta UPDATE Multi-Pass — ETL Pipeline Stages — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.pipeline_demos.order_pipeline WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.pipeline_demos;
DROP ZONE IF EXISTS {{zone_name}};
