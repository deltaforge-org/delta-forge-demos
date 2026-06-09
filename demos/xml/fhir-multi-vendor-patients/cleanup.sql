-- ============================================================================
-- FHIR Multi-Vendor Patients (XML) — Cleanup Script
-- ============================================================================
-- Drops the silver Delta table, then the bronze external table, then the
-- schema, then the zone. Order is important: tables -> schema -> zone.
-- WITH FILES removes the underlying parquet (silver) + XML files (bronze).
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.hie.patients_silver WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.hie.patients WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'fhir-multi-vendor-patients' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.hie;
DROP ZONE IF EXISTS {{zone_name}};
