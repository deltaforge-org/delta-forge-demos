-- ============================================================================
-- Avro Insurance Claims — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.avro_insurance.all_claims WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.avro_insurance.auto_claims_only WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.avro_insurance.sampled_claims WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.avro_insurance;
DROP ZONE IF EXISTS {{zone_name}};
