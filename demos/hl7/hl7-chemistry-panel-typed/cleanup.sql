-- ============================================================================
-- HL7 Chemistry Panel — Typed Multi-OBX Ingestion (Cleanup)
-- ============================================================================
-- Removes the bronze external table and the demo schema. The shared zone
-- ({{zone_name}}) is intentionally left in place because other HL7 demos
-- reuse the same zone — DROP SCHEMA / DROP ZONE will succeed silently if
-- empty, or warn (not error) if other tables still reference them.
-- ============================================================================

-- STEP 1: Drop External Table (also removes its catalog metadata)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.hl7_demos.chem_panels_typed WITH FILES;

-- STEP 2: Drop Schema (no-op / warning if other demos still own tables here)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'hl7-chemistry-panel-typed' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.hl7_demos;
