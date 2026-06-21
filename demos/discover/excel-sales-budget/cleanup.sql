-- ============================================================================
-- Cleanup: Regional Sales-Budget Auto-Onboarding
-- ============================================================================
-- sales_budget was registered by DISCOVER (not a hand-written CREATE EXTERNAL
-- TABLE), but it is an ordinary external table and drops the same way. IF
-- EXISTS keeps this harmless when a prior run failed before the DISCOVER
-- EXECUTE block.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.discover_demos.sales_budget WITH FILES;

-- Remove the per-demo landing folder (now empty after the table drop).
DROP FOLDER 'discover-excel-sales-budget' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.discover_demos;
