-- ============================================================================
-- EDI TRADACOMS — Purchase Order Line Items — Setup Script
-- ============================================================================
-- Ingests a single TRADACOMS purchase order file containing 4 MHD messages
-- (ORDHDR, ORDERS x2, ORDTLR) with 5 product line items across 2 orders.
--
-- A UK grocery retailer (ANY SHOP PLC) sends purchase orders to their
-- supplier (XYZ MANUFACTURING PLC). The file follows the TRADACOMS
-- Header-Detail-Trailer pattern:
--   ORDHDR:9 — Order header with trading partner details (TYP, SDT, CDT)
--   ORDERS:9 — Order detail with line items (CLO, ORD, DIN, OLD, OTR)
--   ORDTLR:9 — Order trailer with file-level totals (OFT)
--
-- Two tables demonstrate different levels of field extraction:
--   1. tradacoms_order_compact — Default: STX/MHD headers + full JSON
--   2. tradacoms_order_lines   — Deep extraction: OLD, ORD, DIN, OTR, etc.
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
-- TABLE 1: tradacoms_order_compact — Default TRADACOMS output
-- ============================================================================
-- Each TRADACOMS file may contain multiple MHD (Message Header) segments.
-- The executor produces one row per MHD message within each file.
--
-- Default columns (always available):
--   STX_1  — Syntax identifier (e.g. "ANA:1")
--   STX_2  — Sender identification (e.g. ":ANY SHOP PLC")
--   STX_3  — Recipient identification (e.g. ":XYZ MANUFACTURING PLC")
--   STX_4  — Transmission date/time (e.g. "940321")
--   STX_5  — Transmission reference
--   MHD_1  — Message reference number within transmission
--   MHD_2  — Message type and version (e.g. "ORDERS:9")
--   df_transaction_json  — Full parsed message as JSON
--   df_transaction_id    — Unique hash for the transaction
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.tradacoms_order_compact
USING EDI
LOCATION 'edi-tradacoms-purchase-orders/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "tradacoms"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

-- ============================================================================
-- TABLE 2: tradacoms_order_lines — Deep field extraction via materialized_paths
-- ============================================================================
-- Uses materialized_paths to extract order detail fields as first-class SQL
-- columns. This enables direct SQL access to OLD (order line detail), ORD
-- (order reference), DIN (delivery instruction), OTR (order trailer), and
-- trading partner fields without parsing JSON.
--
-- Materialized columns:
--   OLD_1  — Line number          OLD_2  — Product EAN-13 code
--   OLD_3  — Supplier product code OLD_5  — Quantity ordered
--   OLD_6  — Unit price           OLD_10 — Product description
--   ORD_1  — Order reference number
--   CLO_1  — Customer location code
--   DIN_1  — Delivery date        DIN_4  — Delivery instruction text
--   OTR_1  — Declared line count  OTR_2  — Declared order total
--   TYP_1  — Transaction type code TYP_2  — Transaction type description
--   SDT_2  — Supplier name        CDT_2  — Customer name
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.tradacoms_order_lines
USING EDI
LOCATION 'edi-tradacoms-purchase-orders/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "tradacoms",
        "materialized_paths": [
            "old_1", "old_2", "old_3", "old_5", "old_6", "old_10",
            "ord_1", "clo_1",
            "din_1", "din_4",
            "otr_1", "otr_2",
            "typ_1", "typ_2",
            "sdt_2", "cdt_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

