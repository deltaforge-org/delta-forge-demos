-- ============================================================================
-- EDI HIPAA Claim Status Tracking — Setup Script
-- ============================================================================
-- Ingests 3 HIPAA X12 healthcare transactions for claim status tracking
-- and prior authorization analysis:
--   276 — Claim status request (inquiry about claim processing)
--   277 — Claim status response (payer's answer with STC status codes)
--   278 — Health services review (prior authorization / utilization management)
--
-- Two tables extract different tracking fields:
--   1. status_messages — Status codes, amounts, and trace numbers (STC, AMT, TRN)
--   2. status_details  — Service lines, utilization management, diagnosis (SVC, UM, HI)
--
-- Together they enable claim lifecycle tracking and authorization analysis.
--
-- Variables (auto-injected by Delta Forge):
--   data_path     — Local or cloud path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--   zone_name     — Target zone name (defaults to 'external')
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.edi
    COMMENT 'EDI transaction-backed external tables';


-- ============================================================================
-- TABLE 1: status_messages — Status tracking fields
-- ============================================================================
-- Extracts claim status and amount fields for tracking claim processing:
--   BHT_1  — Hierarchical structure code (0010 = claim status)
--   BHT_2  — Purpose code (13=request, 08=status notification)
--   NM1_1  — Entity identifier code (PR=payer, 1P=provider, IL=insured)
--   NM1_2  — Entity type qualifier
--   NM1_3  — Entity name
--   TRN_1  — Trace type code (1=current, 2=referenced)
--   TRN_2  — Trace number (links request to response)
--   STC_1  — Status category/code (composite: e.g. "P3:317")
--   STC_4  — Claim monetary amount associated with status
--   AMT_1  — Amount qualifier code (T3=total claim charge)
--   AMT_2  — Amount value
--   DTP_1  — Date/time qualifier (472=service date)
--   DTP_3  — Date value
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi.status_messages
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "bht_1", "bht_2",
            "nm1_1", "nm1_2", "nm1_3",
            "trn_1", "trn_2",
            "stc_1", "stc_4",
            "amt_1", "amt_2",
            "dtp_1", "dtp_3"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.edi.status_messages;

GRANT ADMIN ON TABLE {{zone_name}}.edi.status_messages TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: status_details — Service and authorization detail
-- ============================================================================
-- Extracts service-level and authorization fields:
--   BHT_1  — Hierarchical structure code
--   BHT_2  — Purpose code
--   NM1_1  — Entity identifier code
--   NM1_2  — Entity type qualifier
--   NM1_3  — Entity name
--   SVC_1  — Service procedure code (e.g. HC:99203)
--   SVC_2  — Service line charged amount
--   SVC_3  — Service line paid amount
--   UM_1   — Utilization management request type (SC=surgical, etc.)
--   UM_2   — Certification type (I=initial)
--   HI_1   — Health condition identifier (diagnosis code composite)
--   REF_1  — Reference identification qualifier
--   REF_2  — Reference identification value
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi.status_details
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "bht_1", "bht_2",
            "nm1_1", "nm1_2", "nm1_3",
            "svc_1", "svc_2", "svc_3",
            "um_1", "um_2",
            "hi_1",
            "ref_1", "ref_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.edi.status_details;

GRANT ADMIN ON TABLE {{zone_name}}.edi.status_details TO USER {{current_user}};
