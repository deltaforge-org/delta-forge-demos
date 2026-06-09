-- ============================================================================
-- HL7 Chemistry Panel — Typed Multi-OBX Ingestion (Setup)
-- ============================================================================
-- Real-world scenario: a clinical chemistry lab transmits ORU^R01 messages
-- containing Comprehensive Metabolic Panels (CMP). Each panel carries 14 OBX
-- segments (sodium, potassium, chloride, CO2, BUN, creatinine, glucose,
-- calcium, total protein, albumin, bilirubin, alk phos, AST, ALT) — one
-- analyte per OBX, all under a single ORU message.
--
-- This demo exercises the just-landed HL7 v2 engine fixes:
--   - infer_timestamps          → MSH-7 / OBR-7 / OBX-14 become typed
--                                 Timestamp(Microsecond) columns.
--   - infer_integers            → patient DOB (PID-7) becomes Int64; other
--                                 numeric-shaped fields auto-promote when
--                                 every value parses cleanly.
--   - repeating_segment_mode    → 'to_json' keeps every OBX occurrence as a
--                                 JSON-array string per OBX-N column, so
--                                 analysts can later UNNEST or use
--                                 json_array_length / get_json_object.
--   - materialized_paths        → flat OBX_2..OBX_14 columns for direct
--                                 SQL access without JSON walking.
--
-- Variables (auto-injected):
--   data_path     — directory containing the *.hl7 files
--   current_user  — username of the current logged-in user
--   zone_name     — target zone (defaults to 'external')
-- ============================================================================

-- --------------------------------------------------------------------------
-- STEP 1: Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.hl7_demos
    COMMENT 'HL7 v2 message-backed external tables';

-- --------------------------------------------------------------------------
-- STEP 2: External Table — Typed Multi-OBX Chemistry Panels
-- --------------------------------------------------------------------------
-- One row per ORU message. MSH-7 / OBR-7 / OBX-14 are inferred as Timestamp;
-- PID-7 (DOB, 8-digit) is inferred as Int64. Repeating OBX segments collapse
-- into per-field JSON arrays (e.g. obx_5 = '["88","14","0.9", ...]') so each
-- message stays on a single row while preserving every analyte value.
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hl7_demos.chem_panels_typed
USING HL7
LOCATION 'hl7-chemistry-panel-typed/*.hl7'
OPTIONS (
    hl7_config = '{
        "infer_timestamps": true,
        "infer_integers": true,
        "repeating_segment_mode": "to_json",
        "flatten_subcomponents": false,
        "materialized_paths": [
            "pid_3", "pid_5", "pid_7", "pid_8",
            "obr_4", "obr_7",
            "obx_2", "obx_3", "obx_5", "obx_6", "obx_7", "obx_8", "obx_14"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

