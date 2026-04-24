-- ============================================================================
-- EDI HIPAA Eligibility & Enrollment — Setup Script
-- ============================================================================
-- Ingests 3 HIPAA X12 healthcare transactions for eligibility and enrollment
-- analysis:
--   270 — Eligibility inquiry (request for patient coverage info)
--   271 — Eligibility response (coverage details and benefits)
--   834 — Benefit enrollment maintenance (plan elections)
--
-- Two tables extract different aspects of the eligibility/enrollment lifecycle:
--   1. eligibility_messages — Eligibility request/response fields (BHT, TRN, EQ, DMG)
--   2. enrollment_details   — Enrollment and plan fields (BGN, INS, HD, COB)
--
-- Together they enable cross-referencing eligibility inquiries with enrollment data.
--
-- Variables (auto-injected by DeltaForge):
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

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.edi_demos
    COMMENT 'EDI transaction-backed external tables';
-- ============================================================================
-- TABLE 1: eligibility_messages — Eligibility request/response fields
-- ============================================================================
-- Extracts eligibility-specific fields for request/response analysis:
--   BHT_1  — Hierarchical structure code (0022 = eligibility)
--   BHT_2  — Purpose code (13=request, 11=response)
--   NM1_1  — Entity identifier code (PR=payer, 1P=provider, IL=insured)
--   NM1_2  — Entity type qualifier (1=person, 2=non-person entity)
--   NM1_3  — Name last or organization name
--   TRN_1  — Trace type code (1=current, 2=referenced)
--   TRN_2  — Trace number (links request to response)
--   EQ_1   — Eligibility/benefit inquiry code (30=surgical, etc.)
--   DMG_1  — Date format qualifier (D8=CCYYMMDD)
--   DMG_2  — Date of birth
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.eligibility_messages
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "bht_1", "bht_2",
            "nm1_1", "nm1_2", "nm1_3",
            "trn_1", "trn_2",
            "eq_1",
            "dmg_1", "dmg_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: enrollment_details — Enrollment and plan fields
-- ============================================================================
-- Extracts enrollment-specific fields for plan analysis:
--   BGN_1  — Transaction purpose code (00=original enrollment)
--   BGN_2  — Reference identification (enrollment control number)
--   INS_1  — Subscriber indicator (Y=subscriber, N=dependent)
--   INS_7  — Employment status code
--   NM1_1  — Entity identifier code
--   NM1_2  — Entity type qualifier
--   NM1_3  — Name last or organization name
--   HD_1   — Maintenance type code (021=addition)
--   HD_3   — Insurance line code (HLT=health, DEN=dental, VIS=vision)
--   DTP_1  — Date/time qualifier (356=enrollment date, 348=effective date)
--   DTP_3  — Date value
--   COB_1  — Payer responsibility code (P=primary, S=secondary)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.enrollment_details
USING EDI
LOCATION '{{data_path}}/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "bgn_1", "bgn_2",
            "ins_1", "ins_7",
            "nm1_1", "nm1_2", "nm1_3",
            "hd_1", "hd_3",
            "dtp_1", "dtp_3",
            "cob_1"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
