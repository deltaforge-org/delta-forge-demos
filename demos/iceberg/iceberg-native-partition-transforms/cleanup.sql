-- ============================================================================
-- Iceberg Native Partition Transforms — Cleanup
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg.network_traffic WITH FILES;

-- STEP 2: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg;
DROP ZONE IF EXISTS {{zone_name}};
