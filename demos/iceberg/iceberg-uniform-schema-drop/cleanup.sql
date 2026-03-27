-- ============================================================================
-- Iceberg UniForm Drop Columns (GDPR PII Removal) — Cleanup
-- ============================================================================

-- STEP 1: Drop Iceberg read-back verification table
DROP TABLE IF EXISTS {{zone_name}}.iceberg_demos.user_profiles_iceberg;

-- STEP 2: Drop Delta table (includes Delta log + Iceberg metadata/ directory)
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.user_profiles WITH FILES;

-- STEP 3: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
