-- ============================================================================
-- HL7 Patient Administration — Setup Script
-- ============================================================================
-- Ingests 8 real-world HL7 v2 ADT (Admit-Discharge-Transfer) messages from
-- multiple EHR systems and HL7 versions (v2.3 through v2.6).
--
-- Patient lifecycle covered:
--   A01 — Admission  (6 messages from EPIC, Folio3, Ritten, AWS, MegaReg, Azure)
--   A08 — Update     (1 message: demographics/address change)
--   A03 — Discharge  (1 message: with procedure code)
--
-- Two tables demonstrate different views of the same ADT feed:
--   1. adt_messages      — Compact: MSH header + full JSON + message ID
--   2. adt_materialized  — Materialized: MSH header + key PID/PV1 fields extracted
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
-- TABLE 1: adt_messages — Compact view (8 messages)
-- ============================================================================
-- Default HL7 output: MSH header fields (MSH_1 through MSH_21),
-- df_message_json (full message as JSON), and df_message_id (unique hash).
-- Use df_message_json with JSON functions for deep segment access.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hl7_demos.adt_messages
USING HL7
LOCATION 'hl7-patient-admin/*.hl7'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


-- ============================================================================
-- TABLE 2: adt_materialized — Key patient fields extracted (8 messages)
-- ============================================================================
-- Uses materialized_paths to extract commonly-queried ADT fields as
-- first-class columns alongside the default MSH + JSON output.
--
-- Materialized columns:
--   PID_3  — Patient ID (medical record number)
--   PID_5  — Patient name (LAST^FIRST^MIDDLE format)
--   PID_7  — Date of birth
--   PID_8  — Gender (M/F)
--   PID_11 — Patient address
--   PV1_2  — Patient class (I=Inpatient, O=Outpatient, E=Emergency)
--   PV1_3  — Assigned location (ward/room/bed)
--   PV1_7  — Attending physician
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hl7_demos.adt_materialized
USING HL7
LOCATION 'hl7-patient-admin/*.hl7'
OPTIONS (
    hl7_config = '{
        "materialized_paths": [
            "pid_3", "pid_5", "pid_7", "pid_8", "pid_11",
            "pv1_2", "pv1_3", "pv1_7",
            "evn_1", "evn_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


