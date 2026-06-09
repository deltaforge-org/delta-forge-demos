-- ============================================================================
-- EDI HIPAA Healthcare — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge ingests HIPAA X12 healthcare
-- transactions across the complete claims lifecycle into queryable tables.
--
-- Two tables are available:
--   hipaa_messages      — Compact view: ISA/GS/ST headers + full JSON
--   hipaa_materialized  — Enriched view: headers + BHT/NM1/CLM/BPR columns
--
-- Column reference (always available — X12 default columns):
--   ISA_1 through ISA_16 = Interchange Control Header fields
--   ISA_6  = Interchange Sender ID
--   ISA_8  = Interchange Receiver ID
--   ISA_12 = Interchange Control Version (00501 for all HIPAA 5010)
--   GS_1   = Functional Identifier Code (HC=Health Care, BE=Benefit Enrollment)
--   GS_8   = Implementation Guide Version (e.g. 005010X222A1)
--   ST_1   = Transaction Set Identifier Code (270, 271, 276, 277, 278, 820, 834, 835, 837)
--   ST_2   = Transaction Set Control Number
--   df_transaction_json = Full transaction as JSON
--   df_transaction_id   = Unique transaction hash
--
-- Materialized columns (hipaa_materialized table only):
--   BHT_1  = Hierarchical structure code
--   BHT_2  = Transaction set purpose code
--   BHT_6  = Transaction type code
--   NM1_1  = Entity identifier code
--   NM1_2  = Entity type qualifier (1=person, 2=non-person)
--   NM1_3  = Name last or organization name
--   CLM_1  = Claim submitter identifier (patient account number)
--   CLM_2  = Total claim charge amount
--   BPR_1  = Transaction handling code
--   BPR_2  = Total payment amount
-- ============================================================================


-- ============================================================================
-- 1. All HIPAA Transactions — Header Overview
-- ============================================================================
-- This query reads from the compact table (hipaa_messages) to show the
-- ISA/GS/ST header of every HIPAA transaction. Each row is one X12
-- transaction set from one .edi file.
--
-- What you'll see:
--   - df_file_name:  The source .edi file this row came from
--   - sender_id:     ISA-06 interchange sender identifier
--   - transaction_type: ST-01 transaction set ID (270, 271, 837, etc.)
--   - impl_guide:    GS-08 implementation guide version (identifies the HIPAA standard)
--   - edi_version:   ISA-12 interchange control version (00501 = HIPAA 5010)

ASSERT ROW_COUNT = 11
ASSERT VALUE transaction_type = '270' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
ASSERT VALUE transaction_type = '271' WHERE df_file_name = 'hipaa_271_eligibility_response.edi'
ASSERT VALUE transaction_type = '837' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE transaction_type = '834' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
ASSERT VALUE impl_guide = '005010X279A1' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
ASSERT VALUE edi_version = '00501' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
ASSERT VALUE sender_id = '1234567' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
ASSERT VALUE sender_id = '386028429' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
ASSERT VALUE transaction_type = '835' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE transaction_type = '820' WHERE df_file_name = 'hipaa_820_payment_order.edi'
SELECT
    df_file_name,
    isa_6 AS sender_id,
    st_1 AS transaction_type,
    gs_8 AS impl_guide,
    isa_12 AS edi_version
FROM {{zone_name}}.edi_demos.hipaa_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Transaction Type Distribution
-- ============================================================================
-- Groups transactions by their ST_1 (Transaction Set Identifier Code) to
-- show the mix of HIPAA transaction types ingested.
--
-- What you'll see:
--   - transaction_type: The ST-01 code identifying the transaction kind
--   - type_name:        Human-readable name for the transaction type
--   - transaction_count: How many transactions of that type
--
-- 9 distinct types (270, 271, 276, 277, 278, 820, 834, 835, 837)
-- Note: ST_1 values come from the file content, not filenames. The 837 type
-- appears 3 times (professional, dental, institutional claims).

ASSERT ROW_COUNT = 9
ASSERT VALUE transaction_count = 3 WHERE transaction_type = '837'
ASSERT VALUE transaction_count = 1 WHERE transaction_type = '270'
ASSERT VALUE transaction_count = 1 WHERE transaction_type = '271'
ASSERT VALUE transaction_count = 1 WHERE transaction_type = '835'
ASSERT VALUE transaction_count = 1 WHERE transaction_type = '834'
SELECT
    st_1 AS transaction_type,
    CASE st_1
        WHEN '270' THEN 'Eligibility Inquiry'
        WHEN '271' THEN 'Eligibility Response'
        WHEN '276' THEN 'Claim Status Request'
        WHEN '277' THEN 'Claim Status Response'
        WHEN '278' THEN 'Health Services Review'
        WHEN '820' THEN 'Payment Order'
        WHEN '834' THEN 'Benefit Enrollment'
        WHEN '835' THEN 'Claim Payment/Remittance'
        WHEN '837' THEN 'Healthcare Claim'
        ELSE 'Unknown'
    END AS type_name,
    COUNT(*) AS transaction_count
FROM {{zone_name}}.edi_demos.hipaa_messages
GROUP BY st_1
ORDER BY st_1;


-- ============================================================================
-- 3. Implementation Guide Versions
-- ============================================================================
-- Groups by GS_8 (Implementation Guide Version) to show which HIPAA
-- implementation guides are represented. Each guide specifies the exact
-- segment/element structure for a transaction type.
--
-- What you'll see:
--   - impl_guide:        GS-08 version string (e.g. "005010X222A1")
--   - transaction_count: How many transactions use that guide
--
-- 9 distinct guides — each HIPAA transaction type has its own
-- implementation guide version:
--   005010X279A1 (270/271 eligibility)     = 2 transactions
--   005010X212   (276/277 claim status)    = 2 transactions
--   005010X217   (278 services review)     = 1 transaction
--   005010X218   (820 payment order)       = 1 transaction
--   005010X220A1 (834 enrollment)          = 1 transaction
--   005010X221A1 (835 payment/remittance)  = 1 transaction
--   005010X222A1 (837P professional claim) = 1 transaction
--   005010X223A2 (837I institutional)      = 1 transaction
--   005010X224A2 (837D dental)             = 1 transaction

ASSERT ROW_COUNT = 9
ASSERT VALUE transaction_count = 2 WHERE impl_guide = '005010X279A1'
ASSERT VALUE transaction_count = 2 WHERE impl_guide = '005010X212'
ASSERT VALUE transaction_count = 1 WHERE impl_guide = '005010X220A1'
ASSERT VALUE transaction_count = 1 WHERE impl_guide = '005010X221A1'
SELECT
    gs_8 AS impl_guide,
    COUNT(*) AS transaction_count
FROM {{zone_name}}.edi_demos.hipaa_messages
GROUP BY gs_8
ORDER BY gs_8;


-- ============================================================================
-- 4. HIPAA Transaction Categories
-- ============================================================================
-- Classifies each transaction into a business category using CASE WHEN on
-- ST_1. This shows how a clearinghouse might categorize incoming traffic.
--
-- What you'll see:
--   - category:          Business category (Eligibility, Claim Status, etc.)
--   - transaction_count: How many transactions in that category
--
-- Expected categories:
--   Eligibility    (270, 271)     = 2
--   Claim Status   (276, 277)     = 2
--   Claims         (837)          = 3
--   Payment        (835, 820)     = 2
--   Enrollment     (834)          = 1
--   Authorization  (278)          = 1

ASSERT ROW_COUNT = 6
ASSERT VALUE transaction_count = 3 WHERE category = 'Claims'
ASSERT VALUE transaction_count = 2 WHERE category = 'Eligibility'
ASSERT VALUE transaction_count = 2 WHERE category = 'Claim Status'
ASSERT VALUE transaction_count = 2 WHERE category = 'Payment'
ASSERT VALUE transaction_count = 1 WHERE category = 'Enrollment'
ASSERT VALUE transaction_count = 1 WHERE category = 'Authorization'
SELECT
    CASE
        WHEN st_1 IN ('270', '271') THEN 'Eligibility'
        WHEN st_1 IN ('276', '277') THEN 'Claim Status'
        WHEN st_1 = '837'           THEN 'Claims'
        WHEN st_1 IN ('835', '820') THEN 'Payment'
        WHEN st_1 = '834'           THEN 'Enrollment'
        WHEN st_1 = '278'           THEN 'Authorization'
        ELSE 'Other'
    END AS category,
    COUNT(*) AS transaction_count
FROM {{zone_name}}.edi_demos.hipaa_messages
GROUP BY category
ORDER BY transaction_count DESC;


-- ============================================================================
-- 5. Claim Details — Materialized View
-- ============================================================================
-- Reads from hipaa_materialized to show claim-specific fields that have
-- been extracted as first-class columns. Filters to rows where CLM_1
-- (claim submitter ID) is populated — these are the 837 claim transactions.
--
-- What you'll see:
--   - df_file_name:   Source file
--   - claim_id:       CLM-01 patient account / claim tracking number
--   - claim_amount:   CLM-02 total charge amount
--   - patient_name:   NM1-03 last name (from the first NM1 segment)
--   - transaction_type: ST-01 (should be 837 for all claim rows)

ASSERT ROW_COUNT = 3
ASSERT VALUE claim_id = '26463774' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE claim_amount = '100' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE transaction_type = '837' WHERE df_file_name = 'hipaa_835_claim_payment.edi'
ASSERT VALUE claim_id = '26403774' WHERE df_file_name = 'hipaa_837D_dental_claim.edi'
ASSERT VALUE claim_amount = '150' WHERE df_file_name = 'hipaa_837D_dental_claim.edi'
ASSERT VALUE claim_id = '756048Q' WHERE df_file_name = 'hipaa_837I_institutional_claim.edi'
ASSERT VALUE claim_amount = '89.93' WHERE df_file_name = 'hipaa_837I_institutional_claim.edi'
SELECT
    df_file_name,
    clm_1 AS claim_id,
    clm_2 AS claim_amount,
    nm1_3 AS patient_name,
    st_1 AS transaction_type
FROM {{zone_name}}.edi_demos.hipaa_materialized
WHERE clm_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 6. Payment Transactions
-- ============================================================================
-- Filters the materialized table to transactions that contain payment
-- information (BPR segment). BPR_1 is the transaction handling code and
-- BPR_2 is the payment amount.
--
-- What you'll see:
--   - df_file_name:      Source file
--   - handling_code:     BPR-01 (e.g. "I" = remittance info only, "C" = payment)
--   - payment_amount:    BPR-02 total payment amount in dollars
--   - transaction_type:  ST-01 (835 or 820)

ASSERT ROW_COUNT = 2
ASSERT VALUE handling_code = 'I' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE payment_amount = '34.00' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE transaction_type = '835' WHERE df_file_name = 'hipaa_820_payment.edi'
ASSERT VALUE handling_code = 'C' WHERE df_file_name = 'hipaa_820_payment_order.edi'
ASSERT VALUE payment_amount = '19000' WHERE df_file_name = 'hipaa_820_payment_order.edi'
ASSERT VALUE transaction_type = '820' WHERE df_file_name = 'hipaa_820_payment_order.edi'
SELECT
    df_file_name,
    bpr_1 AS handling_code,
    bpr_2 AS payment_amount,
    st_1 AS transaction_type
FROM {{zone_name}}.edi_demos.hipaa_materialized
WHERE bpr_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 7. Functional Group Analysis
-- ============================================================================
-- Groups by GS_1 (Functional Identifier Code) to show the functional
-- group breakdown. Most HIPAA healthcare transactions use "HC" (Health
-- Care), while benefit enrollment (834) uses "BE".
--
-- What you'll see:
--   - functional_group: GS-01 code
--   - group_name:       Human-readable name
--   - transaction_count: How many transactions in that group

ASSERT ROW_COUNT = 2
ASSERT VALUE transaction_count = 10 WHERE functional_group = 'HC'
ASSERT VALUE transaction_count = 1 WHERE functional_group = 'BE'
SELECT
    gs_1 AS functional_group,
    CASE gs_1
        WHEN 'HC' THEN 'Health Care'
        WHEN 'BE' THEN 'Benefit Enrollment and Maintenance'
        ELSE 'Other'
    END AS group_name,
    COUNT(*) AS transaction_count
FROM {{zone_name}}.edi_demos.hipaa_messages
GROUP BY gs_1
ORDER BY transaction_count DESC;


-- ============================================================================
-- 8. Full Transaction JSON — Deep Access via df_transaction_json
-- ============================================================================
-- Every row includes df_transaction_json containing the complete parsed X12
-- transaction as a JSON structure. This enables access to ANY segment/element
-- — including segments not materialized (HL hierarchy, SV1 service lines,
-- EB eligibility benefits, STC status codes, etc.).
--
-- What you'll see:
--   - df_file_name:        Source file name
--   - transaction_type:    ST-01 code
--   - df_transaction_json: Full transaction as JSON (click to expand in the UI)
--
-- Tip: The JSON contains every segment with its elements. Use JSON functions
-- for deep access without needing materialized_paths.

ASSERT ROW_COUNT = 3
ASSERT VALUE transaction_type = '270' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
ASSERT VALUE transaction_type = '271' WHERE df_file_name = 'hipaa_271_eligibility_response.edi'
ASSERT VALUE transaction_type = '276' WHERE df_file_name = 'hipaa_276_claim_status_request.edi'
ASSERT VALUE df_transaction_json IS NOT NULL WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
SELECT
    df_file_name,
    st_1 AS transaction_type,
    df_transaction_json
FROM {{zone_name}}.edi_demos.hipaa_messages
ORDER BY df_file_name
LIMIT 3;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, transaction types, and key invariants.
-- All checks should return PASS.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'transaction_count_11'
ASSERT VALUE result = 'PASS' WHERE check_name = 'source_files_11'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
SELECT check_name, result FROM (

    -- Check 1: Total transaction count = 11 (one per .edi file)
    SELECT 'transaction_count_11' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.hipaa_messages) = 11
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 11 distinct source files in df_file_name
    SELECT 'source_files_11' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi_demos.hipaa_messages) = 11
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: At least 7 distinct transaction types (actual: 9)
    SELECT 'transaction_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT st_1) FROM {{zone_name}}.edi_demos.hipaa_messages) >= 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Materialized table also has 11 rows (same files, different columns)
    SELECT 'materialized_count_11' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.hipaa_materialized) = 11
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: BHT_2 (transaction purpose code) is populated in at least some rows
    SELECT 'bht_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.hipaa_materialized
                       WHERE bht_2 IS NOT NULL AND bht_2 <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: df_transaction_json is populated for all 11 transactions
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.hipaa_messages
                       WHERE df_transaction_json IS NOT NULL) = 11
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
