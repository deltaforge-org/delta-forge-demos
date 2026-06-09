-- ============================================================================
-- Avro IoT Sensors — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.avro_demos.all_readings WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.avro_demos.floor4_only WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.avro_demos.readings_sample WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.avro_demos;
DROP ZONE IF EXISTS {{zone_name}};
