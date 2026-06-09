-- ============================================================================
-- EDIFACT Customs & Border Control — Setup Script
-- ============================================================================
-- Ingests 5 UN/EDIFACT files used by a port authority customs system for
-- border security, trade compliance, and vessel operations.
--
-- Message types covered:
--   CUSCAR  — Customs Cargo Report           (2 files: D:95B:UN, D:03B:UN)
--   PAXLST  — Passenger List                 (1 file:  D:03B:UN)
--   PNRGOV  — Passenger Name Record          (1 file:  11:1:IA)
--   BAPLIE  — Bayplan / Stowage Plan         (1 file:  D:13B:UN:SMDG31)
--
-- Two tables demonstrate different views of the same EDIFACT feed:
--   1. customs_messages      — Compact: UNB/UNH headers + full JSON
--   2. customs_materialized  — Enriched: headers + key segment fields
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
-- TABLE 1: customs_messages — Compact view (5 messages)
-- ============================================================================
-- Default EDIFACT output: UNB envelope fields (UNB_1 through UNB_5),
-- UNH message header (UNH_1 message reference, UNH_2 message type),
-- df_transaction_json (full message as JSON), and df_transaction_id
-- (unique hash). Use df_transaction_json with JSON functions for deep
-- segment access.
--
-- UNB fields:
--   UNB_1 = Syntax identifier (UNOB, UNOC, IATA)
--   UNB_2 = Interchange sender
--   UNB_3 = Interchange recipient
--   UNB_4 = Date/time of preparation
--   UNB_5 = Interchange control reference
--
-- UNH fields:
--   UNH_1 = Message reference number
--   UNH_2 = Message type (CUSCAR, PAXLST, PNRGOV, BAPLIE — or NULL if malformed)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.customs_messages
USING EDI
LOCATION 'edi-edifact-customs-border/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "edifact"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: customs_materialized — Key border/customs fields extracted
-- ============================================================================
-- Uses materialized_paths to extract commonly-queried EDIFACT segments as
-- first-class columns alongside the default UNB/UNH + JSON output.
--
-- Materialized columns:
--   BGM_1  — Document/message name code (85=Customs declaration, 250=Passenger list)
--   BGM_2  — Document/message number (CRN reference, manifest ID)
--   TDT_1  — Transport stage qualifier (20=Main carriage transport)
--   TDT_2  — Conveyance reference number (voyage/flight number)
--   NAD_1  — Party qualifier (CA=Carrier, OS=Consignor, VW=Vessel master)
--   NAD_2  — Party identification
--   LOC_1  — Location qualifier (9=Loading, 28=Arrival, 91=Transport means)
--   LOC_2  — Location identification
--   CNI_1  — Consolidation item number (sequence within manifest)
--   CNI_2  — Consignment reference number (bill of lading)
--   GID_1  — Goods item number (sequence within consignment)
--   GID_2  — Number and type of packages
--   EQD_1  — Equipment qualifier (CN=Container, BI=Bill of lading)
--   EQD_2  — Equipment identification (container number)
--   DOC_1  — Document name code (39=Passport)
--   DOC_2  — Document identifier (passport/document number)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.customs_materialized
USING EDI
LOCATION 'edi-edifact-customs-border/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "edifact",
        "materialized_paths": [
            "bgm_1", "bgm_2",
            "tdt_1", "tdt_2",
            "nad_1", "nad_2",
            "loc_1", "loc_2",
            "cni_1", "cni_2",
            "gid_1", "gid_2",
            "eqd_1", "eqd_2",
            "doc_1", "doc_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
