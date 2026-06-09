-- ============================================================================
-- EDI EDIFACT International Trade — Setup Script
-- ============================================================================
-- Ingests 22 EDIFACT and EANCOM files covering international trade, logistics,
-- customs, and passenger data across multiple UN/EDIFACT directory versions.
--
-- Message types covered (EDIFACT — 16 files):
--   ORDERS   — Purchase Order                (1 file)
--   ORDRSP   — Order Response                (1 file)
--   INVOIC   — Invoice                       (2 files: D96A, D01B)
--   IFCSUM   — Forwarding & Consolidation    (1 file)
--   CUSCAR   — Customs Cargo Report          (2 files: D03B, D95B)
--   BAPLIE   — Bayplan / Stowage Plan        (1 file)
--   PAXLST   — Passenger List                (1 file)
--   PNRGOV   — Passenger Name Record         (1 file)
--   APERAK   — Application Error/Ack         (1 file)
--   CONTRL   — Syntax Acknowledgment         (1 file, 2 messages)
--   INFENT   — Payment Information            (1 file)
--   QUOTES   — Quote Message                 (2 files: basic, multi-message)
--   PAORES   — Travel Reservation Response   (1 file)
--
-- Message types covered (EANCOM — 6 files):
--   DESADV   — Despatch Advice               (1 file)
--   IFTSTA   — Transport Status              (1 file)
--   INVOIC   — Invoice                       (1 file)
--   ORDRSP   — Order Response                (1 file)
--   PRICAT   — Price Catalogue               (1 file)
--   IFTMIN   — Transport Instruction         (1 file)
--
-- One table is created:
--   edifact_messages — UNB/UNH headers + full JSON for every message
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
-- TABLE 1: edifact_messages — Compact view
-- ============================================================================
-- Default EDIFACT output: UNB envelope fields (UNB_1 through UNB_5),
-- UNH message header (UNH_1 message reference, UNH_2 message identifier),
-- df_transaction_json (full message as JSON), and df_transaction_id
-- (unique hash). Use df_transaction_json with JSON functions for deep
-- segment access.
--
-- UNB fields:
--   UNB_1 = Syntax identifier (UNOA, UNOB, UNOC, UNOL, IATB, IATA)
--   UNB_2 = Interchange sender
--   UNB_3 = Interchange recipient
--   UNB_4 = Date/time of preparation
--   UNB_5 = Interchange control reference
--
-- UNH fields:
--   UNH_1 = Message reference number
--   UNH_2 = Message identifier (e.g. ORDERS:D:96A:UN, INVOIC:D:01B:UN:EAN010)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.edifact_messages
USING EDI
LOCATION 'edi-edifact-international-trade/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "edifact"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

