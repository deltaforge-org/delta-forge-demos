-- ============================================================================
-- HL7 Clinical Workflows — Setup Script
-- ============================================================================
-- Ingests 4 HL7 v2 messages covering clinical workflows beyond ADT and lab:
--   MDM^T02 — Medical document (History & Physical narrative)
--   SIU^S12 — Appointment scheduling (2 messages from different systems)
--   ADT^A01 — Edge cases (escape sequences, empty fields, special chars)
--
-- Two tables demonstrate different analytical needs:
--   1. clinical_messages     — Compact: MSH header + JSON for all 4 messages
--   2. clinical_materialized — Materialized: key PID/TXA/SCH fields extracted
--
-- Variables (auto-injected by DeltaForge):
--   data_path     — Local or cloud path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--   zone_name     — Target zone name (defaults to 'external')
--
-- Naming convention: zone_name.format.table
--   zone   = {{zone_name}}  (defaults to 'external')
--   schema = 'hl7'          (the file format)
--   table  = object name
-- ============================================================================
-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.hl7_demos
    COMMENT 'HL7 v2 message-backed external tables';
-- ============================================================================
-- TABLE 1: clinical_messages — All 4 messages, compact view
-- ============================================================================
-- Default HL7 output: MSH header fields, df_message_json, df_message_id.
-- All message types (MDM, SIU, ADT edge cases) in one table.
-- Use df_message_json for deep access to TXA, SCH, OBX, etc.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hl7_demos.clinical_messages
USING HL7
LOCATION 'hl7-clinical-workflows/*.hl7'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


-- ============================================================================
-- TABLE 2: clinical_materialized — Key fields extracted
-- ============================================================================
-- Materializes common clinical fields: patient name (PID_5), visit info
-- (PV1_2, PV1_3), and scheduling fields (SCH_1, SCH_7, SCH_25).
-- TXA and OBX segment data is accessible via df_message_json.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hl7_demos.clinical_materialized
USING HL7
LOCATION 'hl7-clinical-workflows/*.hl7'
OPTIONS (
    hl7_config = '{
        "materialized_paths": [
            "pid_3", "pid_5", "pid_7", "pid_8",
            "pv1_2", "pv1_3",
            "txa_2", "txa_12", "txa_14",
            "sch_1", "sch_7", "sch_10", "sch_25",
            "obx_2", "obx_3", "obx_5"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


