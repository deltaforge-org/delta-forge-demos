-- ============================================================================
-- HL7 Lab Orders & Results — Setup Script
-- ============================================================================
-- Ingests 8 real-world HL7 v2 ORM (Order) and ORU (Observation Result)
-- messages spanning HL7 v2.3 through v2.5.1, from multiple LIS and EHR
-- systems.
--
-- Message types:
--   ORM^O01 — Lab/radiology orders (3 messages: multi-test, single, radiology)
--   ORU^R01 — Observation results (5 messages: CMP, glucose, immunizations,
--             radiology report, simple result)
--
-- Two tables demonstrate different analytical views:
--   1. lab_orders     — Compact: MSH header + JSON (ORM messages only, via orm*.hl7)
--   2. lab_results    — Materialized: MSH header + key OBR/OBX fields (all 8 files)
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
-- TABLE 1: lab_orders — ORM messages, compact view (3 messages)
-- ============================================================================
-- Default HL7 output: MSH header fields, df_message_json, df_message_id.
-- The full order details (ORC, OBR, NTE) are accessible via df_message_json.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hl7_demos.lab_orders
USING HL7
LOCATION 'hl7-lab-orders-results/orm*.hl7'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


-- ============================================================================
-- TABLE 2: lab_results — All messages with materialized observation fields
-- ============================================================================
-- Extracts key observation fields as first-class columns via
-- materialized_paths. PID_5 (patient name), OBR_4 (test ordered),
-- OBX_2 (value type), OBX_3 (observation ID), OBX_5 (value),
-- OBX_6 (units), OBX_7 (reference range), OBX_8 (abnormal flag).
--
-- This table loads all 8 HL7 files (both ORM and ORU). The ORM messages
-- will have NULL values for OBX fields. Queries can filter by
-- MSH_9 LIKE 'ORU%' for result-only views.
--
-- Note: With default repeating_segment_mode (First), only the first OBX
-- segment is materialized. Use df_message_json for all OBX segments.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hl7_demos.lab_results
USING HL7
LOCATION 'hl7-lab-orders-results/*.hl7'
OPTIONS (
    hl7_config = '{
        "materialized_paths": [
            "pid_3", "pid_5",
            "obr_4",
            "obx_2", "obx_3", "obx_5", "obx_6", "obx_7", "obx_8"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


