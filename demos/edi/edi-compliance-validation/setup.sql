-- ============================================================================
-- EDI Compliance Validation — Setup Script
-- ============================================================================
-- Ingests 14 real-world X12 EDI transactions to demonstrate compliance
-- monitoring using 997 (Functional Acknowledgment) and 824 (Application
-- Advice) documents. While all 14 transactions are loaded, the queries
-- focus on the two compliance documents and their error-reporting segments.
--
-- Transaction types covered:
--   850 — Purchase Order            (3 files)
--   810 — Invoice                   (5 files)
--   855 — PO Acknowledgment         (1 file)
--   856 — Ship Notice / Manifest    (1 file)
--   857 — Shipment & Billing Notice (1 file)
--   861 — Receiving Advice          (1 file)
--   997 — Functional Acknowledgment (1 file)  ** compliance focus **
--   824 — Application Advice        (1 file)  ** compliance focus **
--
-- Two tables demonstrate different views of the same EDI feed:
--   1. compliance_messages  — Compact: ISA/GS/ST headers + full JSON
--   2. compliance_details   — Materialized: 997/824 error-reporting segments
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
-- TABLE 1: compliance_messages — Compact view (14 transactions)
-- ============================================================================
-- Default X12 output: ISA_1 through ISA_16, GS_1 through GS_8,
-- ST_1 (transaction set ID), ST_2 (transaction set control number),
-- df_transaction_json (full transaction as JSON), and df_transaction_id
-- (unique hash). Used for classification and counting — compliance
-- documents (997, 824) vs business documents (all others).
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.compliance_messages
USING EDI
LOCATION 'edi-compliance-validation/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "x12"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: compliance_details — 997/824 error-reporting segments extracted
-- ============================================================================
-- Uses materialized_paths to extract the acknowledgment and error segments
-- unique to 997 and 824 transactions as first-class columns:
--
-- 997 Functional Acknowledgment segments:
--   AK1_1  = Functional Group Code acknowledged (e.g. 'IN' = Invoice)
--   AK1_2  = Group Control Number acknowledged
--   AK5_1  = Transaction Set Acknowledgment Code (A=Accepted, R=Rejected)
--   AK9_1  = Functional Group Acknowledge Code (A/R/P)
--   AK9_2  = Number of Transaction Sets Included
--   AK9_3  = Number of Transaction Sets Received
--   AK9_4  = Number of Transaction Sets Accepted
--   AK3_1  = Segment ID Code with error (e.g. 'TXI')
--   AK3_2  = Segment Position in Transaction Set
--   AK3_3  = Bound Loop Identifier
--   AK4_1  = Element Position in Segment
--   AK4_2  = Element Reference Number
--   AK4_3  = Element Syntax Error Code
--
-- 824 Application Advice segments:
--   BGN_1  = Transaction Set Purpose Code (e.g. '11')
--   BGN_2  = Reference Identification
--   BGN_3  = Date
--   OTI_1  = Application Acknowledgment Code (e.g. 'IR' = Invalid Record)
--   OTI_2  = Reference Identification Qualifier
--   OTI_3  = Reference Identification (original document ref)
--   TED_1  = Technical Error Code
--   TED_2  = Free-form Description of Error
--   N1_1   = Entity Identifier Code (first occurrence)
--   N1_2   = Name (first occurrence)
--   REF_1  = Reference Identification Qualifier (first occurrence)
--   REF_2  = Reference Identification (first occurrence)
--
-- Non-997/824 rows will have NULL for all AK/OTI/TED/BGN fields.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.compliance_details
USING EDI
LOCATION 'edi-compliance-validation/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "ak1_1", "ak1_2",
            "ak5_1",
            "ak9_1", "ak9_2", "ak9_3", "ak9_4",
            "ak3_1", "ak3_2",
            "ak4_1", "ak4_2", "ak4_3",
            "bgn_1", "bgn_2", "bgn_3",
            "oti_1", "oti_2", "oti_3",
            "ted_1", "ted_2",
            "n1_1", "n1_2",
            "ref_1", "ref_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
