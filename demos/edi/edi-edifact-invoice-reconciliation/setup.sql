-- ============================================================================
-- EDIFACT Invoice Reconciliation — Setup Script
-- ============================================================================
-- Ingests 4 UN/EDIFACT files representing a procurement cycle: purchase
-- orders (ORDERS), order responses (ORDRSP), and invoices (INVOIC).
-- Accounts payable uses these to reconcile document numbers, trading
-- partners, line items, dates, monetary amounts, and tax across the cycle.
--
-- Message types covered:
--   ORDERS  — Purchase Order      (1 file, D:96A:UN)
--   ORDRSP  — Order Response      (1 file, D:96A:UN)
--   INVOIC  — Invoice             (2 files: D:96A:UN + D:01B:UN)
--
-- Two tables demonstrate different views of the same EDIFACT feed:
--   1. commerce_messages      — Compact: UNB/UNH headers + full JSON
--   2. commerce_materialized  — Enriched: headers + key commerce fields
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
-- TABLE 1: commerce_messages — Compact view (4 messages)
-- ============================================================================
-- Default EDIFACT output: UNB envelope fields (UNB_1 through UNB_5),
-- UNH message header (UNH_1 message reference, UNH_2 message type),
-- df_transaction_json (full message as JSON), and df_transaction_id
-- (unique hash). Use df_transaction_json with JSON functions for deep
-- segment access.
--
-- UNB fields:
--   UNB_1 = Syntax identifier (UNOB, UNOC)
--   UNB_2 = Interchange sender
--   UNB_3 = Interchange recipient
--   UNB_4 = Date/time of preparation
--   UNB_5 = Interchange control reference
--
-- UNH fields:
--   UNH_1 = Message reference number
--   UNH_2 = Message type (ORDERS, ORDRSP, INVOIC)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.commerce_messages
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "edifact"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: commerce_materialized — Key commerce fields extracted
-- ============================================================================
-- Uses materialized_paths to extract commonly-queried EDIFACT segments as
-- first-class columns alongside the default UNB/UNH + JSON output.
--
-- Materialized columns:
--   BGM_1  — Document/message name code (220=Order, 380=Commercial Invoice,
--            393=Factored Invoice, Z12=Order Response)
--   BGM_2  — Document/message number (PO number, invoice number)
--   NAD_1  — Party qualifier (SU=Supplier, BY=Buyer, DP=Delivery Party)
--   NAD_2  — Party identification (EAN/GLN code)
--   LIN_1  — Line item number (sequence within message)
--   LIN_3  — Item number identification (EAN/GTIN product code, composite)
--   DTM_1  — Date/time/period qualifier (2=Delivery, 137=Document, 171=Reference)
--   DTM_2  — Date/time/period value (YYYYMMDD or YYYYMMDDHHSS)
--   MOA_1  — Monetary amount (composite: qualifier:amount, e.g. "203:699.84")
--   TAX_1  — Duty/tax/fee type qualifier (7=Value Added Tax)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.commerce_materialized
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "edifact",
        "materialized_paths": [
            "bgm_1", "bgm_2",
            "nad_1", "nad_2",
            "lin_1", "lin_3",
            "dtm_1", "dtm_2",
            "moa_1",
            "tax_1"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
