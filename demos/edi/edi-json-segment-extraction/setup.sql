-- ============================================================================
-- EDI JSON Segment Extraction — Setup Script
-- ============================================================================
-- Ingests 14 real-world X12 EDI transactions into a single compact table
-- WITHOUT materialized_paths. The point of this demo is to show that SQL
-- JSON functions (json_array_length, json_typeof, json_extract_path_text,
-- jsonb_pretty, and the #>> path operator) eliminate the need for
-- pre-materializing segment fields — analysts can explore and extract any
-- segment on-demand from df_transaction_json.
--
-- Transaction types covered (same 14 files as supply-chain demo):
--   850 — Purchase Order            (3 files)
--   810 — Invoice                   (5 files)
--   855 — PO Acknowledgment         (1 file)
--   856 — Ship Notice / Manifest    (1 file)
--   857 — Shipment & Billing Notice (1 file)
--   861 — Receiving Advice          (1 file)
--   997 — Functional Acknowledgment (1 file)
--   824 — Application Advice        (1 file)
--
-- Only ONE table — no materialized_paths. JSON functions do the work.
--
-- Variables (auto-injected by DeltaForge):
--   data_path     — Local or cloud path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--   zone_name     — Target zone name (defaults to 'external')
--
-- Naming convention: zone_name.format.table
--   zone   = {{zone_name}}  (defaults to 'external')
--   schema = 'edi'          (the file format)
--   table  = object name
-- ============================================================================
-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.edi_demos
    COMMENT 'EDI transaction-backed external tables';
-- ============================================================================
-- TABLE 1: json_extraction_messages — Compact view (14 transactions)
-- ============================================================================
-- Default X12 output: ISA_1 through ISA_16, GS_1 through GS_8,
-- ST_1 (transaction set ID), ST_2 (transaction set control number),
-- df_transaction_json (full transaction body as JSON array), and
-- df_transaction_id (unique hash).
--
-- df_transaction_json contains body segments ONLY (between ST and SE).
-- Envelope segments (ISA, GS, ST, SE, GE, IEA) are extracted as separate
-- columns. Use SQL JSON functions for deep access to body segments.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.json_extraction_messages
USING EDI
LOCATION 'edi-json-segment-extraction/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "x12"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
