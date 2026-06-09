-- ============================================================================
-- EDI TRADACOMS UK Retail — Setup Script
-- ============================================================================
-- Ingests 4 TRADACOMS files demonstrating the UK-specific EDI standard used
-- in retail and utilities. TRADACOMS messages include purchase orders,
-- product planning forecasts, and utility bills — with escape character handling.
--
-- TRADACOMS is the dominant EDI format in UK retail, predating UN/EDIFACT
-- adoption. Each file contains an STX (start of transmission) envelope
-- wrapping one or more MHD (message header) segments. The executor produces
-- one row per MHD message within each file, so a single file may yield
-- multiple rows.
--
-- Message types covered:
--   ORDHDR:9 / ORDERS:9 / ORDTLR:9 — Purchase Order (tradacoms_order.edi)
--   PPRHDR:2 / PPRDET:2 / PPRTLR:2 — Product Planning (tradacoms_product_planning.edi)
--   UTLHDR:3 / UTLBIL:3 / UVATLR:3 / UTLTLR:3 — Utility Bill (x2 files)
--
-- Two tables demonstrate different views of the same TRADACOMS feed:
--   1. tradacoms_messages      — Compact: STX/MHD headers + full JSON
--   2. tradacoms_materialized  — Enriched: headers + key segment fields
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
-- TABLE 1: tradacoms_messages — Compact view
-- ============================================================================
-- Each TRADACOMS file may contain multiple MHD (Message Header) segments.
-- The executor produces one row per MHD message within each file.
--
-- Default TRADACOMS output columns:
--   STX_1  — Syntax identifier (e.g. "ANA:1")
--   STX_2  — Sender identification (e.g. ":ANY SHOP PLC")
--   STX_3  — Recipient identification (e.g. ":XYZ MANUFACTURING PLC")
--   STX_4  — Transmission date/time (e.g. "940321" or "180513:025446")
--   STX_5  — Transmission reference (e.g. "REFS" or "11488")
--   MHD_1  — Message reference number within transmission
--   MHD_2  — Message type and version (e.g. "ORDERS:9", "UTLBIL:3")
--   df_transaction_json  — Full parsed message as JSON
--   df_transaction_id    — Unique hash for the transaction
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.tradacoms_messages
USING EDI
LOCATION 'edi-tradacoms-uk-retail/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "tradacoms"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

-- ============================================================================
-- TABLE 2: tradacoms_materialized — Key retail fields extracted
-- ============================================================================
-- Uses materialized_paths to extract commonly-queried TRADACOMS fields as
-- first-class columns alongside the default STX/MHD + JSON output.
--
-- Materialized columns:
--   TYP_1  — Transaction type code (e.g. "0430" for orders)
--   TYP_2  — Transaction sub-type or version
--   SDT_1  — Supplier code (e.g. "5017416000006")
--   SDT_2  — Supplier name (e.g. "SUPPLIER NAME")
--   CDT_1  — Customer code
--   CDT_2  — Customer name (e.g. "GEORGE'S FRIED CHICKEN + SONS")
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.tradacoms_materialized
USING EDI
LOCATION 'edi-tradacoms-uk-retail/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "tradacoms",
        "materialized_paths": [
            "typ_1", "typ_2",
            "sdt_1", "sdt_2",
            "cdt_1", "cdt_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

