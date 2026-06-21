-- ============================================================================
-- Cleanup: EDI X12 Auto-Onboarding
-- ============================================================================
-- edi_documents was registered by DISCOVER (not a hand-written CREATE
-- EXTERNAL TABLE), but it is an ordinary external table and drops the same
-- way. IF EXISTS keeps this harmless when a prior run failed before the
-- DISCOVER EXECUTE block.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.discover_demos.edi_documents WITH FILES;

-- Remove the per-demo landing folder (now empty after the table drop).
DROP FOLDER 'discover-edi-x12-onboarding' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.discover_demos;
