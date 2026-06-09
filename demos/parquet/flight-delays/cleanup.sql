-- ============================================================================
-- Parquet Flight Delays — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet_flights.all_flights WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet_flights.q1_flights WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.parquet_flights;
DROP ZONE IF EXISTS {{zone_name}};
