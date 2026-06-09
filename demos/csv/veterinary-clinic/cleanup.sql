-- ============================================================================
-- Veterinary Clinic Patient Records — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_vet.all_visits WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_vet.north_only WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_vet.sampled_visits WITH FILES;

-- Drop schema and zone
DROP SCHEMA IF EXISTS {{zone_name}}.csv_vet;
DROP ZONE IF EXISTS {{zone_name}};
