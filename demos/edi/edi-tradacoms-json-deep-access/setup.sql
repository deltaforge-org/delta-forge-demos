-- ============================================================================
-- EDI TRADACOMS — JSON Deep Segment Extraction — Setup Script
-- ============================================================================
-- Ingests 2 TRADACOMS files (product planning + purchase order) into a single
-- table WITHOUT materialized_paths. The point of this demo is to show that
-- SQL JSON functions eliminate the need for pre-materializing segment fields
-- when messages contain deeply nested, variable-length segment groups like
-- PDN/PLO/SFS/SFX in product planning forecasts.
--
-- Message types covered:
--   PPRHDR:2 / PPRDET:2 / PPRTLR:2 — Product Planning (3 messages)
--   ORDHDR:9 / ORDERS:9 / ORDTLR:9 — Purchase Order   (4 messages)
--   Total: 7 rows across 2 files
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
-- TABLE 1: tradacoms_json_messages — JSON-only view (7 messages)
-- ============================================================================
-- Default TRADACOMS output columns:
--   STX_1  — Syntax identifier (e.g. "ANA:1")
--   STX_2  — Sender identification
--   STX_3  — Recipient identification
--   STX_4  — Transmission date/time
--   STX_5  — Transmission reference
--   MHD_1  — Message reference number within transmission
--   MHD_2  — Message type and version (e.g. "ORDERS:9", "PPRDET:2")
--   df_transaction_json  — Full parsed message as JSON array of segment objects
--   df_transaction_id    — Unique hash for the transaction
--
-- NO materialized_paths — all segment data is accessed via JSON functions.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.tradacoms_json_messages
USING EDI
LOCATION 'edi-tradacoms-json-deep-access/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "tradacoms"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
