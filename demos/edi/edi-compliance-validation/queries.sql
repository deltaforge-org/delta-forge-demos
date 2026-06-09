-- ============================================================================
-- EDI Compliance Validation — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge monitors EDI compliance using 997
-- (Functional Acknowledgment) and 824 (Application Advice) transactions.
-- These two X12 document types are the mechanism trading partners use to
-- report acceptance, rejection, and specific errors back to the sender.
--
-- Two tables are available:
--   compliance_messages  — Compact view: ISA/GS/ST headers + full JSON
--   compliance_details   — Materialized: 997/824 error-reporting segments
--
-- 997 Materialized columns (compliance_details table only):
--   AK1_1  = Functional Group Code      AK1_2  = Group Control Number
--   AK5_1  = Transaction Ack Code       AK9_1  = Group Ack Code
--   AK9_2  = Txn Sets Included          AK9_3  = Txn Sets Received
--   AK9_4  = Txn Sets Accepted
--   AK3_1  = Error Segment ID           AK3_2  = Segment Position
--   AK3_3  = Loop Identifier
--   AK4_1  = Element Position            AK4_2  = Element Ref Number
--   AK4_3  = Element Error Code
--
-- 824 Materialized columns (compliance_details table only):
--   BGN_1  = Purpose Code               BGN_2  = Reference ID
--   BGN_3  = Date
--   OTI_1  = Application Ack Code       OTI_2  = Ref ID Qualifier
--   OTI_3  = Reference ID
--   TED_1  = Technical Error Code       TED_2  = Error Description
--   N1_1   = Entity ID Code             N1_2   = Name
--   REF_1  = Reference Qualifier        REF_2  = Reference ID
-- ============================================================================


-- ============================================================================
-- 1. Compliance Document Inventory
-- ============================================================================
-- Classifies all 14 transactions as either 'Compliance' (997 and 824
-- documents used for acknowledgment/error reporting) or 'Business' (all
-- other transaction types — orders, invoices, shipping notices, etc.).
-- This gives an EDI operations team a quick view of the compliance-to-
-- business document ratio in their feed.
--
-- What you'll see:
--   - classification:  'Compliance' for 997/824, 'Business' for all others
--   - doc_count:       Number of transactions in each category

ASSERT ROW_COUNT = 2
ASSERT VALUE doc_count = 2 WHERE classification = 'Compliance'
ASSERT VALUE doc_count = 12 WHERE classification = 'Business'
SELECT
    CASE
        WHEN st_1 IN ('997', '824') THEN 'Compliance'
        ELSE 'Business'
    END AS classification,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi_demos.compliance_messages
GROUP BY
    CASE
        WHEN st_1 IN ('997', '824') THEN 'Compliance'
        ELSE 'Business'
    END
ORDER BY classification;


-- ============================================================================
-- 2. 997 Functional Acknowledgment Overview
-- ============================================================================
-- Examines the 997 Functional Acknowledgment document in detail. The 997
-- is sent by a receiver to acknowledge a functional group of transactions.
-- AK1 identifies WHICH functional group was acknowledged; AK9 summarizes
-- the accept/reject outcome for the entire group.
--
-- What you'll see:
--   - st_1:           Transaction set ID (997)
--   - ak1_1:          Functional group code acknowledged ('IN' = Invoice)
--   - ak1_2:          Group control number acknowledged
--   - ak5_1:          First transaction set ack code ('A' = Accepted)
--   - ak9_1:          Group-level result ('R' = Rejected)
--   - ak9_2:          Number of transaction sets included
--   - ak9_3:          Number of transaction sets received
--   - ak9_4:          Number of transaction sets accepted
--
-- Key insight: AK9_1='R' (group REJECTED), but AK9_4='1' meaning 1 of 2
-- transaction sets was accepted — this is a partial rejection.

ASSERT ROW_COUNT = 1
ASSERT VALUE ak1_1 = 'IN' WHERE st_1 = '997'
ASSERT VALUE ak9_1 = 'R' WHERE st_1 = '997'
ASSERT VALUE ak9_2 = '2' WHERE st_1 = '997'
ASSERT VALUE ak9_4 = '1' WHERE st_1 = '997'
SELECT
    st_1,
    ak1_1,
    ak1_2,
    ak5_1,
    ak9_1,
    ak9_2,
    ak9_3,
    ak9_4
FROM {{zone_name}}.edi_demos.compliance_details
WHERE st_1 = '997';


-- ============================================================================
-- 3. 997 Error Detail — Segment-Level Rejection
-- ============================================================================
-- Digs into the AK3 and AK4 segments of the 997 to identify exactly which
-- segment and element caused a transaction set to be rejected. AK3 points
-- to the offending segment; AK4 pinpoints the specific data element.
--
-- What you'll see:
--   - ak3_1:          Segment ID with error ('TXI' = Tax Information)
--   - ak3_2:          Position of the error segment in the transaction
--   - ak3_3:          Bound loop identifier (empty = not in a loop)
--   - ak4_1:          Element position within the segment ('1' = first)
--   - ak4_2:          Data element reference number ('963')
--   - ak4_3:          Syntax error code ('7' = invalid character)
--
-- This level of detail tells the sender exactly what to fix: element 1
-- of the TXI segment at position 54 contains an invalid value.

ASSERT ROW_COUNT = 1
ASSERT VALUE ak3_1 = 'TXI' WHERE st_1 = '997'
ASSERT VALUE ak4_1 = '1' WHERE st_1 = '997'
ASSERT VALUE ak4_3 = '7' WHERE st_1 = '997'
SELECT
    st_1,
    ak3_1,
    ak3_2,
    ak4_1,
    ak4_2,
    ak4_3
FROM {{zone_name}}.edi_demos.compliance_details
WHERE st_1 = '997';


-- ============================================================================
-- 4. 824 Application Advice Overview
-- ============================================================================
-- Examines the 824 Application Advice document. Unlike the 997 (which
-- acknowledges EDI structure), the 824 reports application-level issues —
-- business rule violations, data mismatches, or processing failures. BGN
-- provides the transaction context; OTI identifies what was rejected and
-- the original transaction reference.
--
-- What you'll see:
--   - st_1:           Transaction set ID (824)
--   - bgn_1:          Purpose code ('11' = Response to Request)
--   - bgn_2:          Reference identification
--   - bgn_3:          Date of the advice
--   - oti_1:          Application ack code ('IR' = Invalid Record)
--   - oti_2:          Reference ID qualifier ('SI')
--   - oti_3:          Original transaction reference ('62001')
--
-- OTI_1='IR' means the application found an invalid record in the
-- original transaction referenced by OTI_3.

ASSERT ROW_COUNT = 1
ASSERT VALUE oti_1 = 'IR' WHERE st_1 = '824'
ASSERT VALUE oti_3 = '62001' WHERE st_1 = '824'
ASSERT VALUE bgn_1 = '11' WHERE st_1 = '824'
SELECT
    st_1,
    bgn_1,
    bgn_2,
    bgn_3,
    oti_1,
    oti_2,
    oti_3
FROM {{zone_name}}.edi_demos.compliance_details
WHERE st_1 = '824';


-- ============================================================================
-- 5. 824 Error Description
-- ============================================================================
-- Shows the TED (Technical Error Description) and REF (Reference
-- Information) segments from the 824. TED contains the specific error
-- code and a human-readable description of what went wrong. REF segments
-- provide cross-references to the original document identifiers.
--
-- What you'll see:
--   - ted_1:          Technical error code ('201')
--   - ted_2:          Error description ('ASN-PART/PLANT/SUPLR')
--   - ref_1:          First reference qualifier ('BM' = Bill of Materials)
--   - ref_2:          First reference value ('62001')
--   - n1_1:           First entity identifier ('SU' = Supplier)
--   - n1_2:           Entity name (empty in this transaction)
--
-- The error description 'ASN-PART/PLANT/SUPLR' indicates an invalid
-- part number, plant, or supplier combination in the original ASN.

ASSERT ROW_COUNT = 1
ASSERT VALUE ted_1 = '201' WHERE st_1 = '824'
ASSERT VALUE ted_2 = 'ASN-PART/PLANT/SUPLR' WHERE st_1 = '824'
ASSERT VALUE ref_1 = 'BM' WHERE st_1 = '824'
ASSERT VALUE ref_2 = '62001' WHERE st_1 = '824'
SELECT
    st_1,
    ted_1,
    ted_2,
    ref_1,
    ref_2,
    n1_1,
    n1_2
FROM {{zone_name}}.edi_demos.compliance_details
WHERE st_1 = '824';


-- ============================================================================
-- 6. Compliance vs Business Ratio
-- ============================================================================
-- Calculates the percentage of EDI transactions that are compliance
-- documents (997/824) versus business documents. In a healthy EDI feed,
-- compliance documents are a small fraction of total volume — a rising
-- ratio may indicate increasing rejection rates or partner issues.
--
-- What you'll see:
--   - total_transactions:     Total count of all transactions (14)
--   - compliance_count:       Number of 997/824 documents (2)
--   - business_count:         Number of business documents (12)
--   - compliance_pct:         Compliance percentage (~14.3%)

ASSERT ROW_COUNT = 1
ASSERT VALUE total_transactions = 14
ASSERT VALUE compliance_count = 2
SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN st_1 IN ('997', '824') THEN 1 ELSE 0 END) AS compliance_count,
    SUM(CASE WHEN st_1 NOT IN ('997', '824') THEN 1 ELSE 0 END) AS business_count,
    ROUND(
        100.0 * SUM(CASE WHEN st_1 IN ('997', '824') THEN 1 ELSE 0 END)
        / COUNT(*),
        1
    ) AS compliance_pct
FROM {{zone_name}}.edi_demos.compliance_messages;


-- ============================================================================
-- 7. Acknowledgment Status Summary
-- ============================================================================
-- Decodes the 997 acknowledgment status codes into human-readable labels.
-- AK9_1 gives the overall group result; AK9_2/3/4 give the transaction
-- set breakdown. CASE expressions translate the single-letter codes into
-- meaningful descriptions for operations dashboards.
--
-- What you'll see:
--   - group_code_acknowledged:  Which functional group ('IN' = Invoice)
--   - group_status:             Decoded AK9_1 ('Rejected')
--   - txn_sets_included:        How many transaction sets in the group
--   - txn_sets_accepted:        How many were accepted
--   - txn_sets_rejected:        Calculated: included minus accepted
--   - assessment:               Summary interpretation of the result
--
-- Key insight: The group is REJECTED, but 1 of 2 transactions passed —
-- this is a partial rejection, not a complete failure.

ASSERT ROW_COUNT = 1
SELECT
    ak1_1 AS group_code_acknowledged,
    CASE ak9_1
        WHEN 'A' THEN 'Accepted'
        WHEN 'E' THEN 'Accepted with Errors'
        WHEN 'P' THEN 'Partially Accepted'
        WHEN 'R' THEN 'Rejected'
        ELSE ak9_1
    END AS group_status,
    ak9_2 AS txn_sets_included,
    ak9_4 AS txn_sets_accepted,
    CAST(CAST(ak9_2 AS INTEGER) - CAST(ak9_4 AS INTEGER) AS VARCHAR) AS txn_sets_rejected,
    CASE
        WHEN ak9_1 = 'R' AND CAST(ak9_4 AS INTEGER) > 0
            THEN 'Partial Rejection — ' || ak9_4 || ' of ' || ak9_2 || ' accepted'
        WHEN ak9_1 = 'R' AND CAST(ak9_4 AS INTEGER) = 0
            THEN 'Complete Rejection — no transaction sets accepted'
        WHEN ak9_1 = 'A'
            THEN 'Fully Accepted — all transaction sets passed'
        ELSE 'Status: ' || ak9_1
    END AS assessment
FROM {{zone_name}}.edi_demos.compliance_details
WHERE st_1 = '997';


-- ============================================================================
-- 8. VERIFY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly and
-- compliance-specific fields are properly extracted.
-- All checks should return PASS.

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = '997_count_is_1'
ASSERT VALUE result = 'PASS' WHERE check_name = '824_count_is_1'
ASSERT VALUE result = 'PASS' WHERE check_name = 'ak1_populated_for_997'
ASSERT VALUE result = 'PASS' WHERE check_name = 'oti_populated_for_824'
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_transactions_14'
SELECT check_name, result FROM (

    -- Check 1: Exactly one 997 Functional Acknowledgment
    SELECT '997_count_is_1' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.compliance_messages
                       WHERE st_1 = '997') = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Exactly one 824 Application Advice
    SELECT '824_count_is_1' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.compliance_messages
                       WHERE st_1 = '824') = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: AK1_1 is populated for the 997 row in compliance_details
    SELECT 'ak1_populated_for_997' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.compliance_details
                       WHERE st_1 = '997' AND ak1_1 IS NOT NULL AND ak1_1 <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: OTI_1 is populated for the 824 row in compliance_details
    SELECT 'oti_populated_for_824' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.compliance_details
                       WHERE st_1 = '824' AND oti_1 IS NOT NULL AND oti_1 <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Total transaction count = 14 across all files
    SELECT 'total_transactions_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.compliance_messages) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
