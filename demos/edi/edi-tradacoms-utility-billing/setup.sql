-- ============================================================================
-- EDI TRADACOMS — UK Energy Billing & VAT Reconciliation — Setup Script
-- ============================================================================
-- Ingests two TRADACOMS utility bill files, each containing 4 MHD messages
-- (UTLHDR:3, UTLBIL:3, UVATLR:3, UTLTLR:3) — 8 messages total.
--
-- A UK energy company (SOME ELECTRIC COMPANY PLC) sends utility bills via
-- TRADACOMS. The file follows the Header-Detail-VAT-Trailer pattern:
--   UTLHDR:3 — Utility header with trading partner details (TYP, SDT, CDT)
--   UTLBIL:3 — Utility bill with charges (BCD, CCD, VAT, BTL)
--   UVATLR:3 — VAT trailer with VAT summaries (VTS)
--   UTLTLR:3 — Utility trailer with transmission totals (TTL)
--
-- Two tables demonstrate different levels of field extraction:
--   1. tradacoms_bills        — Default: STX/MHD headers + full JSON
--   2. tradacoms_bill_details — Deep extraction: BCD, CCD, VAT, BTL, VTS, TTL
--
-- The second file contains TRADACOMS escape characters (?', ?+, ??) in the
-- customer name, demonstrating escape-character decoding.
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
-- TABLE 1: tradacoms_bills — Default TRADACOMS output
-- ============================================================================
-- Each TRADACOMS file may contain multiple MHD (Message Header) segments.
-- The executor produces one row per MHD message within each file.
--
-- Default columns (always available):
--   STX_1  — Syntax identifier (e.g. "ANA:1")
--   STX_2  — Sender identification (e.g. "1011101000000:SOME ELECTRIC COMPANY PLC")
--   STX_3  — Recipient identification
--   STX_4  — Transmission date/time (e.g. "141218:075110")
--   STX_5  — Transmission reference
--   MHD_1  — Message reference number within transmission
--   MHD_2  — Message type and version (e.g. "UTLBIL:3")
--   df_transaction_json  — Full parsed message as JSON
--   df_transaction_id    — Unique hash for the transaction
--
-- Materialized header-level fields for cross-message queries:
--   TYP_1  — Transaction type code (e.g. "0715")
--   SDT_2  — Supplier name (e.g. "SITE 1")
--   CDT_2  — Customer name (demonstrates escape decoding in second file)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.tradacoms_bills
USING EDI
LOCATION 'edi-tradacoms-utility-billing/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "tradacoms",
        "materialized_paths": [
            "typ_1", "sdt_2", "cdt_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

-- ============================================================================
-- TABLE 2: tradacoms_bill_details — Deep field extraction via materialized_paths
-- ============================================================================
-- Uses materialized_paths to extract billing detail fields as first-class SQL
-- columns. This enables direct SQL access to BCD (billing control detail),
-- CCD (charge calculation detail), VAT, BTL (bill total), VTS (VAT summary),
-- TTL (transmission total), and trading partner fields without parsing JSON.
--
-- Materialized columns:
--   BCD_1  — Billing date           BCD_3  — Account number
--   BCD_8  — Billing period (start:end)
--   CCD_3  — Tariff description     CCD_12 — Volume
--   CCD_13 — Volume unit
--   VAT_2  — VAT rate code          VAT_6  — Net amount
--   VAT_7  — VAT amount             VAT_8  — Gross amount
--   BTL_2  — Total charges          BTL_3  — Total VAT
--   BTL_5  — Bill total
--   VTS_4  — Net value              VTS_5  — VAT value
--   VTS_6  — Gross value
--   TTL_1  — Total net              TTL_2  — Total VAT
--   TTL_5  — Total gross
--   TYP_1  — Transaction type code
--   SDT_2  — Supplier name          CDT_2  — Customer name
--   CLO_1  — Customer location code
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.tradacoms_bill_details
USING EDI
LOCATION 'edi-tradacoms-utility-billing/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "tradacoms",
        "materialized_paths": [
            "bcd_1", "bcd_3", "bcd_8",
            "ccd_3", "ccd_12", "ccd_13",
            "vat_2", "vat_6", "vat_7", "vat_8",
            "btl_2", "btl_3", "btl_5",
            "vts_4", "vts_5", "vts_6",
            "ttl_1", "ttl_2", "ttl_5",
            "typ_1", "sdt_2", "cdt_2",
            "clo_1"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

