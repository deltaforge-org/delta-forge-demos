-- ============================================================================
-- EDI Supply Chain X12 — Setup Script
-- ============================================================================
-- Ingests 14 real-world X12 EDI transactions spanning the full supply chain
-- lifecycle: purchase orders, invoices, shipping notices, acknowledgments,
-- receiving advice, and functional/application advice.
--
-- Transaction types covered:
--   850 — Purchase Order            (3 files)
--   810 — Invoice                   (5 files)
--   855 — PO Acknowledgment         (1 file)
--   856 — Ship Notice / Manifest    (1 file)
--   857 — Shipment & Billing Notice (1 file)
--   861 — Receiving Advice          (1 file)
--   997 — Functional Acknowledgment (1 file)
--   824 — Application Advice        (1 file)
--
-- Two tables demonstrate different views of the same EDI feed:
--   1. supply_chain_messages      — Compact: ISA/GS/ST headers + full JSON
--   2. supply_chain_materialized  — Enriched: headers + key business fields
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
-- TABLE 1: supply_chain_messages — Compact view (14 transactions)
-- ============================================================================
-- Default X12 output: ISA_1 through ISA_16, GS_1 through GS_8,
-- ST_1 (transaction set ID), ST_2 (transaction set control number),
-- df_transaction_json (full transaction as JSON), and df_transaction_id
-- (unique hash). Use df_transaction_json with JSON functions for deep
-- segment access.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.supply_chain_messages
USING EDI
LOCATION 'edi-supply-chain-x12/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "x12"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: supply_chain_materialized — Key business fields extracted
-- ============================================================================
-- Uses materialized_paths to extract commonly-queried supply chain fields as
-- first-class columns alongside the default ISA/GS/ST + JSON output.
--
-- Materialized columns:
--   BEG_1  — Transaction Set Purpose Code (00=Original)
--   BEG_3  — Purchase Order Number (850s only)
--   BEG_5  — Purchase Order Date (850s only)
--   BIG_1  — Invoice Date (810s only)
--   BIG_2  — Invoice Number (810s only)
--   BSN_2  — Shipment ID (856s only)
--   BSN_3  — Shipment Date (856s only)
--   N1_1   — Entity Identifier Code (ST=Ship To, BY=Buyer, etc.)
--   N1_2   — Party Name (first N1 segment occurrence)
--   CTT_1  — Transaction Totals (number of line items)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.supply_chain_materialized
USING EDI
LOCATION 'edi-supply-chain-x12/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "beg_1", "beg_3", "beg_5",
            "big_1", "big_2",
            "bsn_2", "bsn_3",
            "n1_1", "n1_2",
            "ctt_1"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
