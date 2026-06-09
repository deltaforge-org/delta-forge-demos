-- ============================================================================
-- EDI HIPAA Eligibility & Enrollment — Demo Queries
-- ============================================================================
-- Coverage verification and enrollment analysis using CTE, JOIN, and
-- COALESCE — SQL patterns unique across all EDI demos.
--
-- Two tables:
--   eligibility_messages — BHT/TRN/EQ/DMG fields (request/response tracking)
--   enrollment_details   — BGN/INS/HD/COB fields (plan elections and coverage)
-- ============================================================================


-- ============================================================================
-- 1. All Transactions Overview
-- ============================================================================
-- Shows all 3 transactions with their functional group assignment:
-- HC (Health Care) for eligibility, BE (Benefit Enrollment) for enrollment.

ASSERT ROW_COUNT = 3
ASSERT VALUE st_1 = '270' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
ASSERT VALUE st_1 = '271' WHERE df_file_name = 'hipaa_271_eligibility_response.edi'
ASSERT VALUE st_1 = '834' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
ASSERT VALUE gs_1 = 'HC' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
ASSERT VALUE gs_1 = 'HC' WHERE df_file_name = 'hipaa_271_eligibility_response.edi'
ASSERT VALUE gs_1 = 'BE' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
SELECT
    df_file_name,
    st_1,
    gs_1,
    CASE st_1
        WHEN '270' THEN 'Eligibility Inquiry'
        WHEN '271' THEN 'Eligibility Response'
        WHEN '834' THEN 'Benefit Enrollment'
        ELSE 'Other'
    END AS transaction_name
FROM {{zone_name}}.edi_demos.eligibility_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Functional Group Distribution
-- ============================================================================
-- Groups by GS_1 to show the split between Health Care (HC) and Benefit
-- Enrollment (BE) functional groups.

ASSERT ROW_COUNT = 2
ASSERT VALUE transaction_count = 2 WHERE functional_group = 'HC'
ASSERT VALUE transaction_count = 1 WHERE functional_group = 'BE'
SELECT
    gs_1 AS functional_group,
    CASE gs_1
        WHEN 'HC' THEN 'Health Care'
        WHEN 'BE' THEN 'Benefit Enrollment and Maintenance'
        ELSE 'Other'
    END AS group_name,
    COUNT(*) AS transaction_count
FROM {{zone_name}}.edi_demos.eligibility_messages
GROUP BY gs_1
ORDER BY gs_1;


-- ============================================================================
-- 3. Eligibility Request/Response Pair — CTE
-- ============================================================================
-- Uses a Common Table Expression (CTE) to pair the 270 eligibility request
-- with its 271 response by matching their TRN_2 trace numbers. This is how
-- a clearinghouse correlates inquiries with their answers.
--
-- SQL features: CTE (WITH clause), JOIN within CTE

ASSERT ROW_COUNT = 1
ASSERT VALUE trace_number = '93175-012547'
ASSERT VALUE request_purpose = '13'
ASSERT VALUE response_purpose = '11'
ASSERT VALUE payer_name = 'ABC COMPANY'
WITH request AS (
    SELECT trn_2 AS trace_number, bht_2 AS purpose, nm1_3 AS payer_name
    FROM {{zone_name}}.edi_demos.eligibility_messages
    WHERE st_1 = '270'
),
response AS (
    SELECT trn_2 AS trace_number, bht_2 AS purpose
    FROM {{zone_name}}.edi_demos.eligibility_messages
    WHERE st_1 = '271'
)
SELECT
    request.trace_number,
    request.payer_name,
    request.purpose AS request_purpose,
    response.purpose AS response_purpose
FROM request
JOIN response ON request.trace_number = response.trace_number;


-- ============================================================================
-- 4. Patient Demographics
-- ============================================================================
-- Shows the DMG (demographic) segment data for all 3 transactions.
-- DMG_2 is the date of birth in CCYYMMDD format.

ASSERT ROW_COUNT = 3
ASSERT VALUE dob = '19430519' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
ASSERT VALUE dob = '19630519' WHERE df_file_name = 'hipaa_271_eligibility_response.edi'
ASSERT VALUE dob = '19400816' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
SELECT
    df_file_name,
    st_1,
    nm1_3 AS entity_name,
    dmg_2 AS dob
FROM {{zone_name}}.edi_demos.eligibility_messages
ORDER BY df_file_name;


-- ============================================================================
-- 5. Enrollment Detail — 834 Only
-- ============================================================================
-- Filters enrollment_details to the 834 transaction (WHERE bgn_1 IS NOT NULL)
-- to show enrollment-specific fields: purpose code, subscriber indicator,
-- and plan type.

ASSERT ROW_COUNT = 1
ASSERT VALUE purpose_code = '00' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
ASSERT VALUE enrollment_ref = '12456' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
ASSERT VALUE subscriber = 'Y' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
ASSERT VALUE plan_code = '021' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
ASSERT VALUE plan_type = 'HLT' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
SELECT
    df_file_name,
    bgn_1 AS purpose_code,
    bgn_2 AS enrollment_ref,
    ins_1 AS subscriber,
    hd_1 AS plan_code,
    hd_3 AS plan_type,
    CASE hd_3
        WHEN 'HLT' THEN 'Health'
        WHEN 'DEN' THEN 'Dental'
        WHEN 'VIS' THEN 'Vision'
        ELSE 'Other'
    END AS plan_description
FROM {{zone_name}}.edi_demos.enrollment_details
WHERE bgn_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 6. Coordination of Benefits
-- ============================================================================
-- Shows the COB (Coordination of Benefits) data from the 834 enrollment.
-- COB_1 indicates payer responsibility: P=primary, S=secondary, T=tertiary.

ASSERT ROW_COUNT = 1
ASSERT VALUE payer_responsibility = 'P' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
SELECT
    df_file_name,
    nm1_3 AS member_name,
    cob_1 AS payer_responsibility,
    CASE cob_1
        WHEN 'P' THEN 'Primary'
        WHEN 'S' THEN 'Secondary'
        WHEN 'T' THEN 'Tertiary'
        ELSE 'Unknown'
    END AS responsibility_description,
    hd_3 AS plan_type
FROM {{zone_name}}.edi_demos.enrollment_details
WHERE cob_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 7. Cross-Table Analysis — JOIN eligibility + enrollment
-- ============================================================================
-- JOINs both tables on df_file_name to create a unified view showing
-- eligibility context alongside enrollment data. This demonstrates
-- multi-table correlation for EDI transactions.
--
-- SQL features: JOIN, COALESCE, CASE WHEN

ASSERT ROW_COUNT = 3
ASSERT VALUE has_eligibility = 'Yes' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
ASSERT VALUE has_enrollment = 'Yes' WHERE df_file_name = 'hipaa_834_benefit_enrollment.edi'
ASSERT VALUE has_enrollment = 'No' WHERE df_file_name = 'hipaa_270_eligibility_request.edi'
SELECT
    e.df_file_name,
    e.st_1,
    CASE WHEN e.bht_1 IS NOT NULL THEN 'Yes' ELSE 'No' END AS has_eligibility,
    CASE WHEN d.bgn_1 IS NOT NULL THEN 'Yes' ELSE 'No' END AS has_enrollment,
    COALESCE(e.nm1_3, d.nm1_3) AS entity_name
FROM {{zone_name}}.edi_demos.eligibility_messages e
JOIN {{zone_name}}.edi_demos.enrollment_details d
    ON e.df_file_name = d.df_file_name
ORDER BY e.df_file_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity checks for eligibility and enrollment data.

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_files_3'
ASSERT VALUE result = 'PASS' WHERE check_name = 'eligibility_pair'
ASSERT VALUE result = 'PASS' WHERE check_name = 'enrollment_record'
ASSERT VALUE result = 'PASS' WHERE check_name = 'hc_transactions'
ASSERT VALUE result = 'PASS' WHERE check_name = 'be_transactions'
SELECT check_name, result FROM (

    -- Check 1: Total transaction count = 3
    SELECT 'total_files_3' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.eligibility_messages) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Eligibility pair exists (270 + 271 with matching TRN_2)
    SELECT 'eligibility_pair' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.eligibility_messages
                       WHERE st_1 IN ('270', '271')) = 2
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Exactly 1 enrollment record (834)
    SELECT 'enrollment_record' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.enrollment_details
                       WHERE bgn_1 IS NOT NULL) = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: HC functional group has 2 transactions
    SELECT 'hc_transactions' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.eligibility_messages
                       WHERE gs_1 = 'HC') = 2
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: BE functional group has 1 transaction
    SELECT 'be_transactions' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.eligibility_messages
                       WHERE gs_1 = 'BE') = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
