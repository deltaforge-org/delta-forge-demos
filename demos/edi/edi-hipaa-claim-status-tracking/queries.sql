-- ============================================================================
-- EDI HIPAA Claim Status Tracking — Demo Queries
-- ============================================================================
-- Claim lifecycle tracking and prior authorization analysis using LIKE
-- pattern matching on composite status codes, CASE expressions, and
-- multi-table JOINs — SQL patterns unique across all EDI demos.
--
-- Two tables:
--   status_messages — BHT/TRN/STC/AMT fields (status codes and amounts)
--   status_details  — SVC/UM/HI fields (services, authorization, diagnosis)
-- ============================================================================


-- ============================================================================
-- 1. All Status Transactions Overview
-- ============================================================================
-- Shows all 3 transactions: claim status request (276), claim status
-- response (277), and health services review / prior authorization (278).

ASSERT ROW_COUNT = 3
ASSERT VALUE st_1 = '276' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE st_1 = '277' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE st_1 = '278' WHERE df_file_name = 'hipaa_278_services_review.edi'
ASSERT VALUE bht_2 = '13' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE bht_2 = '08' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE bht_2 = '13' WHERE df_file_name = 'hipaa_278_services_review.edi'
ASSERT VALUE payer_name = 'ABC INSURANCE' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE payer_name = 'ABC INSURANCE' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE payer_name = 'MARYLAND CAPITAL INSURANCE COMPANY' WHERE df_file_name = 'hipaa_278_services_review.edi'
SELECT
    df_file_name,
    st_1,
    bht_2,
    nm1_3 AS payer_name,
    CASE st_1
        WHEN '276' THEN 'Claim Status Request'
        WHEN '277' THEN 'Claim Status Response'
        WHEN '278' THEN 'Health Services Review'
        ELSE 'Other'
    END AS transaction_name
FROM {{zone_name}}.edi_demos.status_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Transaction Type Distribution
-- ============================================================================
-- Each transaction type appears exactly once — 3 distinct types.

ASSERT ROW_COUNT = 3
ASSERT VALUE type_count = 1 WHERE transaction_type = '276'
ASSERT VALUE type_count = 1 WHERE transaction_type = '277'
ASSERT VALUE type_count = 1 WHERE transaction_type = '278'
SELECT
    st_1 AS transaction_type,
    CASE st_1
        WHEN '276' THEN 'Claim Status Request'
        WHEN '277' THEN 'Claim Status Response'
        WHEN '278' THEN 'Health Services Review'
        ELSE 'Other'
    END AS type_name,
    COUNT(*) AS type_count
FROM {{zone_name}}.edi_demos.status_messages
GROUP BY st_1
ORDER BY st_1;


-- ============================================================================
-- 3. Claim Status Code Analysis — LIKE on Composite Codes
-- ============================================================================
-- The STC_1 field contains composite status codes like "P3:317" where the
-- prefix indicates the category: P=Pending, F=Finalized, R=Request for
-- more information. This query uses LIKE to classify the status.
--
-- SQL features: LIKE pattern matching, CASE WHEN with LIKE

ASSERT ROW_COUNT = 1
ASSERT VALUE status_code = 'P3:317' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE claim_amount = '8513.88' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE status_category = 'Pending' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
SELECT
    df_file_name,
    stc_1 AS status_code,
    stc_4 AS claim_amount,
    CASE
        WHEN stc_1 LIKE 'P%' THEN 'Pending'
        WHEN stc_1 LIKE 'F0%' THEN 'Finalized — Forwarded'
        WHEN stc_1 LIKE 'F1%' THEN 'Finalized — Complete'
        WHEN stc_1 LIKE 'F2%' THEN 'Finalized — Adjusted'
        WHEN stc_1 LIKE 'R%' THEN 'Request for Information'
        ELSE 'Other'
    END AS status_category
FROM {{zone_name}}.edi_demos.status_messages
WHERE stc_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 4. Claim Amounts Under Inquiry
-- ============================================================================
-- Shows the AMT segment from the 276 request — the total claim charge
-- amount that the provider is inquiring about.

ASSERT ROW_COUNT = 1
ASSERT VALUE amount_qualifier = 'T3' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE claim_amount = '8513.88' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
SELECT
    df_file_name,
    amt_1 AS amount_qualifier,
    amt_2 AS claim_amount,
    CASE amt_1
        WHEN 'T3' THEN 'Total Claim Charge'
        WHEN 'YU' THEN 'Federal Medicare or Medicaid Payment'
        ELSE 'Other Amount'
    END AS amount_description
FROM {{zone_name}}.edi_demos.status_messages
WHERE amt_2 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 5. Request/Response Correlation — Trace Numbers
-- ============================================================================
-- Matches the 276 request with the 277 response using TRN_2 trace numbers.
-- The 276 uses TRN_1=1 (originator's trace) and the 277 echoes it back
-- with TRN_1=2 (referenced trace).

ASSERT ROW_COUNT = 2
ASSERT VALUE trn_1 = '1' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE trn_1 = '2' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE trace_number = 'ABCXYZ1' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE trace_number = 'ABCXYZ1' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE trace_type = 'Originator Trace' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE trace_type = 'Referenced Trace' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
SELECT
    df_file_name,
    st_1,
    trn_1,
    trn_2 AS trace_number,
    CASE trn_1
        WHEN '1' THEN 'Originator Trace'
        WHEN '2' THEN 'Referenced Trace'
        ELSE 'Other'
    END AS trace_type
FROM {{zone_name}}.edi_demos.status_messages
WHERE st_1 IN ('276', '277')
ORDER BY df_file_name;


-- ============================================================================
-- 6. Prior Authorization Review — UM and HI Segments
-- ============================================================================
-- Shows the utilization management (UM) and health information (HI) fields
-- from the 278 services review. UM_1 is the request type (SC=surgical),
-- UM_2 is the certification type (I=initial), and HI_1 is the diagnosis
-- code composite (BF:41090:D8:20050430).

ASSERT ROW_COUNT = 1
ASSERT VALUE review_type = 'SC' WHERE df_file_name = 'hipaa_278_services_review.edi'
ASSERT VALUE cert_type = 'I' WHERE df_file_name = 'hipaa_278_services_review.edi'
ASSERT VALUE diagnosis = 'BF:41090:D8:20050430' WHERE df_file_name = 'hipaa_278_services_review.edi'
ASSERT VALUE review_description = 'Surgical' WHERE df_file_name = 'hipaa_278_services_review.edi'
ASSERT VALUE certification_description = 'Initial' WHERE df_file_name = 'hipaa_278_services_review.edi'
SELECT
    df_file_name,
    um_1 AS review_type,
    um_2 AS cert_type,
    hi_1 AS diagnosis,
    CASE um_1
        WHEN 'SC' THEN 'Surgical'
        WHEN 'HS' THEN 'Health Services'
        WHEN 'AR' THEN 'Admission Review'
        ELSE 'Other'
    END AS review_description,
    CASE um_2
        WHEN 'I' THEN 'Initial'
        WHEN 'R' THEN 'Renewal'
        WHEN 'E' THEN 'Extension'
        ELSE 'Other'
    END AS certification_description
FROM {{zone_name}}.edi_demos.status_details
WHERE um_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 7. Service Line Detail from Responses
-- ============================================================================
-- Shows the SVC (service payment) segments from the 276 and 277 files.
-- SVC_1 is the procedure code, SVC_2 is the charged amount, and SVC_3
-- is the paid amount (0 if denied/pending).

ASSERT ROW_COUNT = 2
ASSERT VALUE procedure_code = 'HC:99203' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE charged_amount = '150' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE procedure_code = 'HC:99203' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE charged_amount = '150' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE paid_amount = '0' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
SELECT
    df_file_name,
    st_1,
    svc_1 AS procedure_code,
    svc_2 AS charged_amount,
    svc_3 AS paid_amount
FROM {{zone_name}}.edi_demos.status_details
WHERE svc_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 8. Cross-Table Unified View — JOIN
-- ============================================================================
-- JOINs status_messages (amounts and status codes) with status_details
-- (services and authorization) on df_file_name. Shows which transactions
-- have status codes, service lines, or authorization data.
--
-- SQL features: JOIN, CASE WHEN IS NOT NULL

ASSERT ROW_COUNT = 3
ASSERT VALUE has_status_code = 'Yes' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE has_status_code = 'No' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE has_status_code = 'No' WHERE df_file_name = 'hipaa_278_services_review.edi'
ASSERT VALUE has_authorization = 'Yes' WHERE df_file_name = 'hipaa_278_services_review.edi'
ASSERT VALUE has_authorization = 'No' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE has_authorization = 'No' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE has_service_line = 'Yes' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE has_service_line = 'Yes' WHERE df_file_name = 'hipaa_277_claim_status_response.edi'
ASSERT VALUE has_service_line = 'No' WHERE df_file_name = 'hipaa_278_services_review.edi'
SELECT
    m.df_file_name,
    m.st_1,
    CASE WHEN m.stc_1 IS NOT NULL THEN 'Yes' ELSE 'No' END AS has_status_code,
    CASE WHEN d.um_1 IS NOT NULL THEN 'Yes' ELSE 'No' END AS has_authorization,
    CASE WHEN d.svc_1 IS NOT NULL THEN 'Yes' ELSE 'No' END AS has_service_line
FROM {{zone_name}}.edi_demos.status_messages m
JOIN {{zone_name}}.edi_demos.status_details d
    ON m.df_file_name = d.df_file_name
ORDER BY m.df_file_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity checks for claim status and authorization data.

ASSERT ROW_COUNT = 4
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_files_3'
ASSERT VALUE result = 'PASS' WHERE check_name = 'transaction_types_3'
ASSERT VALUE result = 'PASS' WHERE check_name = 'status_response'
SELECT check_name, result FROM (

    -- Check 1: Total transaction count = 3
    SELECT 'total_files_3' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.status_messages) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 3 distinct transaction types
    SELECT 'transaction_types_3' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT st_1) FROM {{zone_name}}.edi_demos.status_messages) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 1 status response with STC code
    SELECT 'status_response' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.status_messages
                       WHERE stc_1 IS NOT NULL) = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: 1 authorization review with UM segment
    SELECT 'auth_review' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.status_details
                       WHERE um_1 IS NOT NULL) = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
