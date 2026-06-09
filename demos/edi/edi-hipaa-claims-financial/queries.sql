-- ============================================================================
-- EDI HIPAA Claims Financial — Demo Queries
-- ============================================================================
-- Financial analysis of HIPAA X12 healthcare claims: charge summaries,
-- service line detail, remittance reconciliation, and write-off analysis.
--
-- SQL features demonstrated (unique across all EDI demos):
--   SUM / CAST / ROUND — aggregate charge and payment totals
--   HAVING             — filter groups by aggregate conditions
--   JOIN               — correlate claim charges with remittance payments
--   COALESCE           — unify SV1/SV2/SV3 service codes into one column
--   Arithmetic         — compute write-off amounts and percentages
--
-- Two tables:
--   claims_header     — CLM charges + SV1/SV2/SV3 service lines
--   claims_remittance — BPR payment + CLP claim-level + CAS adjustments
-- ============================================================================


-- ============================================================================
-- 1. Claim Overview — All Transactions
-- ============================================================================
-- Shows all 4 transactions with their claim identifiers and charge amounts.
-- The 835 remittance file has no CLM segment (it's a payment, not a claim)
-- so clm_1 and clm_2 are NULL for that row.

ASSERT ROW_COUNT = 4
ASSERT VALUE st_1 = '837' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE st_1 = '837' WHERE df_file_name = 'hipaa_837D_dental_claim.edi'
ASSERT VALUE st_1 = '837' WHERE df_file_name = 'hipaa_837I_institutional_claim.edi'
ASSERT VALUE st_1 = '835' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE clm_1 = '26463774' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE clm_2 = '100' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE clm_1 = '26403774' WHERE df_file_name = 'hipaa_837D_dental_claim.edi'
ASSERT VALUE clm_2 = '150' WHERE df_file_name = 'hipaa_837D_dental_claim.edi'
ASSERT VALUE clm_1 = '756048Q' WHERE df_file_name = 'hipaa_837I_institutional_claim.edi'
ASSERT VALUE clm_2 = '89.93' WHERE df_file_name = 'hipaa_837I_institutional_claim.edi'
SELECT
    df_file_name,
    st_1,
    clm_1,
    clm_2
FROM {{zone_name}}.edi_demos.claims_header
ORDER BY df_file_name;


-- ============================================================================
-- 2. Claim Charge Summary — SUM and CAST
-- ============================================================================
-- Aggregates total charges across all 837 claim submissions using SUM and
-- CAST to convert the string CLM_2 to a numeric type. Only rows with
-- CLM_1 IS NOT NULL are actual claim submissions.
--
-- SQL features: CAST, SUM, ROUND, WHERE IS NOT NULL

ASSERT ROW_COUNT = 1
ASSERT VALUE claim_count = 3
ASSERT VALUE total_charges = '339.93'
SELECT
    COUNT(*) AS claim_count,
    CAST(SUM(CAST(clm_2 AS DOUBLE)) AS VARCHAR) AS total_charges
FROM {{zone_name}}.edi_demos.claims_header
WHERE clm_1 IS NOT NULL;


-- ============================================================================
-- 3. Service Line Detail — COALESCE across claim types
-- ============================================================================
-- Each claim type uses a different service segment: SV1 (professional),
-- SV2 (institutional), SV3 (dental). COALESCE unifies them into a single
-- service_code and service_charge column.
--
-- SQL features: COALESCE, WHERE IS NOT NULL

-- Note: Default repeating_segment_mode is 'first', so sv1_1/sv2_1/sv3_1 reflect
-- the FIRST occurrence of each segment within the transaction. For the 837I
-- institutional claim the first SV2 is SV2*0305*HC:85025*13.39*... so sv2_1
-- is the revenue code '0305' and sv2_2 is the procedure code 'HC:85025'.

ASSERT ROW_COUNT = 3
ASSERT VALUE service_code = 'HC:99213' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE service_charge = '40' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE service_code = 'AD:D2150' WHERE df_file_name = 'hipaa_837D_dental_claim.edi'
ASSERT VALUE service_charge = '100' WHERE df_file_name = 'hipaa_837D_dental_claim.edi'
ASSERT VALUE service_code = '0305' WHERE df_file_name = 'hipaa_837I_institutional_claim.edi'
ASSERT VALUE service_charge = 'HC:85025' WHERE df_file_name = 'hipaa_837I_institutional_claim.edi'
SELECT
    df_file_name,
    clm_1 AS claim_id,
    COALESCE(sv1_1, sv2_1, sv3_1) AS service_code,
    COALESCE(sv1_2, sv2_2, sv3_2) AS service_charge
FROM {{zone_name}}.edi_demos.claims_header
WHERE clm_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 4. Claim Type Distribution — GROUP BY with HAVING
-- ============================================================================
-- Groups transactions by ST_1 (transaction set identifier) and uses HAVING
-- to show only transaction types with more than one occurrence.
--
-- SQL features: GROUP BY, COUNT, HAVING

ASSERT ROW_COUNT = 1
ASSERT VALUE transaction_type = '837'
ASSERT VALUE type_count = 3
SELECT
    st_1 AS transaction_type,
    CASE st_1
        WHEN '835' THEN 'Claim Payment/Remittance'
        WHEN '837' THEN 'Healthcare Claim'
        ELSE 'Other'
    END AS type_name,
    COUNT(*) AS type_count
FROM {{zone_name}}.edi_demos.claims_header
GROUP BY st_1
HAVING COUNT(*) > 1
ORDER BY st_1;


-- ============================================================================
-- 5. Payment & Remittance Overview
-- ============================================================================
-- Reads from claims_remittance to show the BPR (bill payment/remittance)
-- fields. Only the 835 file contains payment information; other rows have
-- NULL for BPR fields.

ASSERT ROW_COUNT = 1
ASSERT VALUE handling_code = 'I' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE payment_amount = '34.00' WHERE df_file_name = 'hipaa_820_payment.edi'
SELECT
    df_file_name,
    bpr_1 AS handling_code,
    bpr_2 AS payment_amount,
    st_1 AS transaction_type
FROM {{zone_name}}.edi_demos.claims_remittance
WHERE bpr_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 6. Remittance Claim Detail — CLP Segment
-- ============================================================================
-- Shows the claim-level payment detail from the CLP segment in the 835
-- remittance: claim reference, status code, and total charge as seen by
-- the payer.
--
-- CLP_2 status codes: 1=processed as primary, 2=processed as secondary,
-- 3=denied, 4=reversal.

ASSERT ROW_COUNT = 1
ASSERT VALUE claim_ref = '0001000055' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE claim_status = '2' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE payer_charge = '541' WHERE df_file_name = 'hipaa_820_payment.edi'
SELECT
    df_file_name,
    clp_1 AS claim_ref,
    clp_2 AS claim_status,
    clp_3 AS payer_charge,
    CASE clp_2
        WHEN '1' THEN 'Processed as Primary'
        WHEN '2' THEN 'Processed as Secondary'
        WHEN '3' THEN 'Denied'
        WHEN '4' THEN 'Reversal'
        ELSE 'Other'
    END AS status_description
FROM {{zone_name}}.edi_demos.claims_remittance
WHERE clp_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 7. Adjustment Analysis — CAS Segment
-- ============================================================================
-- Shows claim adjustments from the CAS (Claim Adjustment Segment).
-- CAS_1 is the adjustment group code (OA=Other Adjustments, CO=Contractual
-- Obligations, PR=Patient Responsibility). CAS_2 is the reason code
-- (23=contractual adjustment, 94=processed in excess of charges).
-- CAS_3 is the adjustment amount.

ASSERT ROW_COUNT = 1
ASSERT VALUE adjustment_group = 'OA' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE reason_code = '23' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE adjustment_amount = '516' WHERE df_file_name = 'hipaa_820_payment.edi'
SELECT
    df_file_name,
    cas_1 AS adjustment_group,
    cas_2 AS reason_code,
    cas_3 AS adjustment_amount,
    CASE cas_1
        WHEN 'OA' THEN 'Other Adjustments'
        WHEN 'CO' THEN 'Contractual Obligations'
        WHEN 'PR' THEN 'Patient Responsibility'
        ELSE 'Other'
    END AS group_description
FROM {{zone_name}}.edi_demos.claims_remittance
WHERE cas_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 8. Charge-to-Payment Reconciliation — JOIN
-- ============================================================================
-- JOINs claims_header (charge side) with claims_remittance (payment side)
-- on df_file_name. This demonstrates multi-table correlation across two
-- EDI external tables — a pattern not used in any other EDI demo.
--
-- SQL features: JOIN, COALESCE

ASSERT ROW_COUNT = 4
ASSERT VALUE claim_charge = '100' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE claim_charge = '150' WHERE df_file_name = 'hipaa_837D_dental_claim.edi'
ASSERT VALUE claim_charge = '89.93' WHERE df_file_name = 'hipaa_837I_institutional_claim.edi'
ASSERT VALUE payment_amount = '34.00' WHERE df_file_name = 'hipaa_820_payment.edi'
SELECT
    h.df_file_name,
    h.st_1 AS transaction_type,
    h.clm_1 AS claim_id,
    h.clm_2 AS claim_charge,
    r.bpr_2 AS payment_amount
FROM {{zone_name}}.edi_demos.claims_header h
JOIN {{zone_name}}.edi_demos.claims_remittance r
    ON h.df_file_name = r.df_file_name
ORDER BY h.df_file_name;


-- ============================================================================
-- 9. Write-Off Analysis — CAST + Arithmetic
-- ============================================================================
-- For the remittance record (835), calculates the write-off amount and
-- percentage by comparing charged vs. paid amounts. Uses CAST to convert
-- string fields to numeric for arithmetic operations.
--
-- SQL features: CAST, arithmetic (subtraction, division, multiplication), ROUND

ASSERT ROW_COUNT = 1
ASSERT VALUE charged = 541.0
ASSERT VALUE paid = 34.0
ASSERT VALUE write_off = 507.0
ASSERT VALUE write_off_pct = 93.7
SELECT
    df_file_name,
    CAST(clp_3 AS DOUBLE) AS charged,
    CAST(bpr_2 AS DOUBLE) AS paid,
    CAST(clp_3 AS DOUBLE) - CAST(bpr_2 AS DOUBLE) AS write_off,
    ROUND((CAST(clp_3 AS DOUBLE) - CAST(bpr_2 AS DOUBLE)) / CAST(clp_3 AS DOUBLE) * 100, 1) AS write_off_pct
FROM {{zone_name}}.edi_demos.claims_remittance
WHERE clp_3 IS NOT NULL AND bpr_2 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: file counts, claim presence, payment presence.

ASSERT ROW_COUNT = 4
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_files_4'
ASSERT VALUE result = 'PASS' WHERE check_name = 'claims_with_charges'
ASSERT VALUE result = 'PASS' WHERE check_name = 'payment_records'
ASSERT VALUE result = 'PASS' WHERE check_name = 'charges_positive'
SELECT check_name, result FROM (

    -- Check 1: Total transaction count = 4
    SELECT 'total_files_4' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.claims_header) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 3 claims have charge amounts (CLM_1 IS NOT NULL)
    SELECT 'claims_with_charges' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.claims_header WHERE clm_1 IS NOT NULL) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 1 remittance has payment info (BPR_1 IS NOT NULL)
    SELECT 'payment_records' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.claims_remittance WHERE bpr_1 IS NOT NULL) = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: All claim charges are positive
    SELECT 'charges_positive' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.claims_header
                       WHERE clm_2 IS NOT NULL AND CAST(clm_2 AS DOUBLE) <= 0) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
