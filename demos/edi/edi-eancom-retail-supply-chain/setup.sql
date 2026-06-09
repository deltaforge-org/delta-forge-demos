-- ============================================================================
-- EANCOM Retail Supply Chain — Setup Script
-- ============================================================================
-- Ingests 6 EANCOM files from a European retailer's EDI hub covering the
-- full retail supply chain: despatch advices, invoices, order responses,
-- price catalogues, transport status, and transport instructions.
--
-- EANCOM is the GS1 subset of UN/EDIFACT used by major European retailers.
-- Same syntax as EDIFACT (UNB/UNH envelope) but with GS1 association codes
-- (EAN004, EAN007, EAN009, EAN011) and EAN/GTIN article numbering in LIN
-- segments.
--
-- Message types covered (6 files, 1 message each):
--   DESADV   — Despatch Advice              (warehouse receiving)
--   IFTSTA   — Transport Status             (shipment tracking)
--   INVOIC   — Invoice                      (accounts payable)
--   ORDRSP   — Order Response               (procurement confirmation)
--   PRICAT   — Price Catalogue              (product master data)
--   IFTMIN   — Transport Instruction        (carrier booking)
--
-- Two tables are created:
--   1. eancom_messages      — Compact: UNB/UNH headers + full JSON
--   2. eancom_materialized  — Enriched: headers + BGM/NAD/LIN/STS/CPS/QTY
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
-- TABLE 1: eancom_messages — Compact view
-- ============================================================================
-- Default EDIFACT output: UNB envelope fields (UNB_1 through UNB_5),
-- UNH message header (UNH_1 message reference, UNH_2 message identifier),
-- df_transaction_json (full message as JSON), and df_transaction_id
-- (unique hash). Use df_transaction_json with JSON functions for deep
-- segment access.
--
-- UNB fields:
--   UNB_1 = Syntax identifier (UNOA or UNOB)
--   UNB_2 = Interchange sender (SENDER1 or SID)
--   UNB_3 = Interchange recipient (RECEIVER1 or RID)
--   UNB_4 = Date/time of preparation
--   UNB_5 = Interchange control reference
--
-- UNH fields:
--   UNH_1 = Message reference number
--   UNH_2 = Message identifier (e.g. DESADV:D:96A:UN:EAN007)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.eancom_messages
USING EDI
LOCATION 'edi-eancom-retail-supply-chain/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "edifact"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

-- ============================================================================
-- TABLE 2: eancom_materialized — Key retail fields extracted
-- ============================================================================
-- Uses materialized_paths to extract commonly-queried EANCOM segment fields
-- as first-class columns alongside the default UNB/UNH + JSON output.
--
-- Materialized columns:
--   BGM_1  — Document/message name code (351=DESADV, 380=INVOIC, etc.)
--   BGM_2  — Document/message number (e.g. DES587441, INV88712)
--   NAD_1  — Party qualifier (BY=buyer, SU=supplier, DP=delivery party, SH=shipper)
--   NAD_2  — Party identification (GLN/EAN-13, e.g. 5412345000013)
--   LIN_1  — Line item number (sequential within message)
--   LIN_3  — Item number identification (GTIN/EAN, e.g. 4000862141404)
--   STS_1  — Status event code (IFTSTA only, e.g. '1' = event reported)
--   CPS_1  — Hierarchical ID number (DESADV only, consignment packing)
--   QTY_1  — Quantity detail (DESADV only, composite e.g. "12:24:PCE")
--
-- Note: NAD, LIN, and QTY are repeating segments. Default config is
-- mode=First, max_repeating_segments=1, so the FIRST occurrence is
-- materialized. CPS is non-repeating, so the LAST CPS write wins when
-- multiple CPS segments exist in a message (e.g. DESADV hierarchical packing).
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.eancom_materialized
USING EDI
LOCATION 'edi-eancom-retail-supply-chain/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "edifact",
        "materialized_paths": [
            "bgm_1", "bgm_2",
            "nad_1", "nad_2",
            "lin_1", "lin_3",
            "sts_1",
            "cps_1",
            "qty_1"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

