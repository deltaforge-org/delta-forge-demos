-- ============================================================================
-- Iceberg UniForm RESTORE (Compliance Recovery) — Cleanup
-- ============================================================================

-- STEP 1: Drop Iceberg read-back verification table
-- NOTE: no WITH FILES — it shares LOCATION with the Delta table and those
--       files are removed in STEP 2.
DROP TABLE IF EXISTS {{zone_name}}.iceberg_demos.compliance_iceberg;

-- STEP 2: Drop Delta table (includes Delta log + Iceberg metadata/ directory)
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.compliance_records WITH FILES;

-- STEP 3: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
