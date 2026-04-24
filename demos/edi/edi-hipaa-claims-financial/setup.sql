-- ============================================================================
-- EDI HIPAA Claims Financial — Setup Script
-- ============================================================================
-- Ingests 4 HIPAA X12 healthcare transactions for financial analysis:
--   3 × 837 claims (professional, dental, institutional)
--   1 × 835 remittance advice (claim payment/remittance)
--
-- Two tables extract different financial fields from the same EDI feed:
--   1. claims_header     — Claim charges + service line detail (CLM, SV1/SV2/SV3)
--   2. claims_remittance — Payment + adjustment detail (BPR, CLP, CAS, SVC)
--
-- Together they enable charge-to-payment reconciliation via JOIN.
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
-- TABLE 1: claims_header — Claim charges + first service line (4 transactions)
-- ============================================================================
-- Extracts claim-level and service-level fields for financial analysis:
--   CLM_1  — Claim submitter identifier (patient account number)
--   CLM_2  — Total claim charge amount
--   SV1_1  — Professional service procedure code (HC:xxxxx composite)
--   SV1_2  — Professional service line charge amount
--   SV2_1  — Institutional revenue code
--   SV2_2  — Institutional service procedure code (HC:xxxxx composite)
--   SV3_1  — Dental service procedure code (AD:Dxxxx composite)
--   SV3_2  — Dental service line charge amount
--
-- SV1/SV2/SV3 populate only for their respective claim types; others are NULL.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.claims_header
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "clm_1", "clm_2",
            "sv1_1", "sv1_2",
            "sv2_1", "sv2_2",
            "sv3_1", "sv3_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: claims_remittance — Payment + adjustment detail (4 transactions)
-- ============================================================================
-- Extracts remittance-specific fields for payment reconciliation:
--   BPR_1  — Transaction handling code (I=info only, C=payment)
--   BPR_2  — Total payment amount
--   CLP_1  — Claim reference number (maps to original CLM_1)
--   CLP_2  — Claim status code (1=processed as primary, 2=partial, etc.)
--   CLP_3  — Total charge amount as seen by payer
--   CAS_1  — Claim adjustment group code (OA=Other Adjustments, etc.)
--   CAS_2  — Claim adjustment reason code (23=contractual, 94=other)
--   CAS_3  — Adjustment amount
--   SVC_1  — Service payment procedure code
--   SVC_2  — Service line charged amount
--   SVC_3  — Service line paid amount
--
-- Only the 835 remittance file populates BPR/CLP/CAS/SVC; others are NULL.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.claims_remittance
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "bpr_1", "bpr_2",
            "clp_1", "clp_2", "clp_3",
            "cas_1", "cas_2", "cas_3",
            "svc_1", "svc_2", "svc_3"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
