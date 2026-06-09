-- ============================================================================
-- EDI JSON Segment Extraction — Demo Queries
-- ============================================================================
-- Showcases SQL JSON functions for on-demand exploration and extraction of
-- X12 EDI transaction segments from df_transaction_json — without needing
-- materialized_paths.
--
-- JSON functions demonstrated:
--   json_array_length()       — Count body segments per transaction
--   json_typeof()             — Inspect JSON value types at any path
--   json_extract_path_text()  — Extract text value at a JSON path
--   json_extract_path_text()  — Extract nested text values at deeper paths
--
-- df_transaction_json structure:
--   A JSON array of segment objects (body segments only — no ISA/GS/ST/SE/GE/IEA):
--   [
--     {"segment": "BEG", "name": "...", "elements": [{"name": "...", "value": "..."}, ...]},
--     {"segment": "PO1", "name": "...", "elements": [{"name": "...", "value": "..."}, ...]},
--     ...
--   ]
--
-- Column reference (always available — ISA envelope fields):
--   ISA_6  = Interchange Sender ID    ISA_8  = Interchange Receiver ID
--   ISA_9  = Interchange Date         ISA_12 = Interchange Control Version
--
-- Column reference (always available — GS/ST fields):
--   GS_1   = Functional Identifier    ST_1   = Transaction Set ID (850, 810, etc.)
-- ============================================================================


-- ============================================================================
-- 1. Transaction Structure Overview — json_array_length
-- ============================================================================
-- Uses json_array_length(df_transaction_json) to count the number of body
-- segments in each transaction. This reveals transaction complexity without
-- reading any segment content. Body segments exclude envelope segments
-- (ISA, GS, ST, SE, GE, IEA) which are separate columns.
--
-- What you'll see:
--   - df_file_name:      Source .edi file
--   - txn_type:          X12 transaction set ID (850, 810, etc.)
--   - body_segment_count: Number of body segments in df_transaction_json

ASSERT ROW_COUNT = 14
ASSERT VALUE txn_type = '850' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE txn_type = '810' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE txn_type = '997' WHERE df_file_name = 'x12_997_functional_acknowledgment.edi'
ASSERT VALUE txn_type = '855' WHERE df_file_name = 'x12_855_purchase_order_ack.edi'
ASSERT VALUE txn_type = '856' WHERE df_file_name = 'x12_856_ship_notice.edi'
ASSERT VALUE txn_type = '857' WHERE df_file_name = 'x12_856_ship_bill_notice.edi'
ASSERT VALUE txn_type = '861' WHERE df_file_name = 'x12_861_receiving_advice.edi'
ASSERT VALUE txn_type = '824' WHERE df_file_name = 'x12_824_application_advice.edi'
-- Non-deterministic: exact body_segment_count depends on engine body-segment inclusion rules
ASSERT WARNING VALUE body_segment_count BETWEEN 5 AND 15 WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT WARNING VALUE body_segment_count BETWEEN 40 AND 80 WHERE df_file_name = 'x12_810_invoice_b.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    json_array_length(df_transaction_json) AS body_segment_count
FROM {{zone_name}}.edi_demos.json_extraction_messages
ORDER BY json_array_length(df_transaction_json) DESC, df_file_name;


-- ============================================================================
-- 2. Transaction Size Classification — CASE on json_array_length
-- ============================================================================
-- Classifies transactions into complexity tiers based on body segment count:
--   Simple  — fewer than 15 body segments
--   Medium  — 15 to 35 body segments
--   Complex — more than 35 body segments
--
-- This is useful for routing transactions: simple ones can be auto-processed,
-- complex ones may need manual review.
--
-- What you'll see:
--   - size_class:  Simple, Medium, or Complex
--   - txn_count:   Number of transactions in each class
--
-- Non-deterministic: class distribution depends on engine body segment counts

ASSERT WARNING ROW_COUNT = 3
ASSERT WARNING VALUE txn_count BETWEEN 1 AND 10 WHERE size_class = 'Simple'
ASSERT WARNING VALUE txn_count BETWEEN 1 AND 10 WHERE size_class = 'Complex'
SELECT
    CASE
        WHEN json_array_length(df_transaction_json) < 15 THEN 'Simple'
        WHEN json_array_length(df_transaction_json) <= 35 THEN 'Medium'
        ELSE 'Complex'
    END AS size_class,
    COUNT(*) AS txn_count
FROM {{zone_name}}.edi_demos.json_extraction_messages
GROUP BY size_class
ORDER BY
    CASE size_class
        WHEN 'Complex' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Simple' THEN 3
    END;


-- ============================================================================
-- 3. JSON Type Inspection — json_typeof
-- ============================================================================
-- Uses json_typeof(df_transaction_json) to confirm every transaction's JSON
-- is an array, and json_typeof on a single element to show it is an object.
-- This is the first step in any JSON exploration workflow — understanding the
-- shape of the data before extracting values.
--
-- What you'll see:
--   - df_file_name:         Source file
--   - txn_type:             Transaction set ID
--   - root_json_type:       Should be 'array' for all rows
--   - first_segment_name:   Name of the first body segment (proves elements are objects)

ASSERT ROW_COUNT = 14
ASSERT VALUE root_json_type = 'array' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE root_json_type = 'array' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE root_json_type = 'array' WHERE df_file_name = 'x12_997_functional_acknowledgment.edi'
ASSERT VALUE first_segment_name = 'BEG' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE first_segment_name = 'BIG' WHERE df_file_name = 'x12_810_invoice_a.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    json_typeof(df_transaction_json) AS root_json_type,
    json_extract_path_text(df_transaction_json, '0', 'segment') AS first_segment_name
FROM {{zone_name}}.edi_demos.json_extraction_messages
ORDER BY df_file_name;


-- ============================================================================
-- 4. First Body Segment Analysis — #>> path extraction
-- ============================================================================
-- Extracts the name of the first body segment from each transaction to reveal
-- what each document type starts with after the ST envelope. This uses the
-- #>> operator for JSON path extraction:
--   json_extract_path_text(df_transaction_json, '0', 'segment') — segment name at array index 0
--
-- Expected first segments by transaction type:
--   850 → BEG (Beginning Segment for Purchase Order)
--   810 → BIG (Beginning Segment for Invoice)
--   855 → BAK (Beginning Segment for PO Acknowledgment)
--   856 → BSN (Beginning Segment for Ship Notice)
--   857 → BHT (Beginning Hierarchical Transaction)
--   861 → BRA (Beginning Segment for Receiving Advice)
--   997 → AK1 (Functional Group Response Header)
--   824 → BGN (Beginning Segment for Application Advice)
--
-- What you'll see:
--   - df_file_name:      Source file
--   - txn_type:          Transaction set ID
--   - first_segment:     Name of the first body segment

ASSERT ROW_COUNT = 14
ASSERT VALUE first_segment = 'BEG' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE first_segment = 'BEG' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE first_segment = 'BEG' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE first_segment = 'BIG' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE first_segment = 'BIG' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE first_segment = 'AK1' WHERE df_file_name = 'x12_997_functional_acknowledgment.edi'
ASSERT VALUE first_segment = 'BAK' WHERE df_file_name = 'x12_855_purchase_order_ack.edi'
ASSERT VALUE first_segment = 'BSN' WHERE df_file_name = 'x12_856_ship_notice.edi'
ASSERT VALUE first_segment = 'BHT' WHERE df_file_name = 'x12_856_ship_bill_notice.edi'
ASSERT VALUE first_segment = 'BRA' WHERE df_file_name = 'x12_861_receiving_advice.edi'
ASSERT VALUE first_segment = 'BGN' WHERE df_file_name = 'x12_824_application_advice.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    json_extract_path_text(df_transaction_json, '0', 'segment') AS first_segment
FROM {{zone_name}}.edi_demos.json_extraction_messages
ORDER BY df_file_name;


-- ============================================================================
-- 5. Purchase Order Details via JSON Path — On-Demand Field Extraction
-- ============================================================================
-- For 850 Purchase Orders, extracts BEG segment element values using the #>>
-- operator on known JSON array positions. Since BEG is always the first body
-- segment in an 850, we access it at index 0.
--
-- BEG element layout (0-indexed):
--   elements[0] = Transaction Set Purpose Code (e.g., "00" = Original)
--   elements[1] = Purchase Order Type Code
--   elements[2] = Purchase Order Number
--   elements[3] = Release Number (optional)
--   elements[4] = Purchase Order Date
--
-- What you'll see:
--   - df_file_name:   Source file
--   - first_segment:  Should be 'BEG' for all 850s
--   - purpose_code:   "00" (Original) for most POs
--   - po_number:      Purchase order number
--   - po_date:        Purchase order date (YYYYMMDD)

ASSERT ROW_COUNT = 3
ASSERT VALUE first_segment = 'BEG' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE first_segment = 'BEG' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE first_segment = 'BEG' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE purpose_code = '00' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE purpose_code = '00' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE purpose_code = '00' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE po_number = '1000012' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po_date = '20090827' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po_number = '4600000406' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po_date = '20140724' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po_number = 'XX-1234' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE po_date = '20170301' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
SELECT
    df_file_name,
    json_extract_path_text(df_transaction_json, '0', 'segment') AS first_segment,
    json_extract_path_text(df_transaction_json, '0', 'elements', '0', 'value') AS purpose_code,
    json_extract_path_text(df_transaction_json, '0', 'elements', '2', 'value') AS po_number,
    json_extract_path_text(df_transaction_json, '0', 'elements', '4', 'value') AS po_date
FROM {{zone_name}}.edi_demos.json_extraction_messages
WHERE st_1 = '850'
ORDER BY df_file_name;


-- ============================================================================
-- 6. Invoice Details via json_extract_path_text — Alternative Extraction
-- ============================================================================
-- For 810 Invoices, extracts BIG segment values using json_extract_path_text
-- as an alternative to the #>> operator. Both achieve the same result — this
-- demonstrates the function-call style for teams that prefer it.
--
-- BIG element layout (0-indexed):
--   elements[0] = Invoice Date
--   elements[1] = Invoice Number
--
-- What you'll see:
--   - df_file_name:     Source file
--   - first_segment:    Should be 'BIG' for all 810s
--   - invoice_date:     Invoice date (YYYYMMDD)
--   - invoice_number:   Invoice identifier

ASSERT ROW_COUNT = 5
ASSERT VALUE first_segment = 'BIG' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE first_segment = 'BIG' WHERE df_file_name = 'x12_810_invoice_b.edi'
ASSERT VALUE first_segment = 'BIG' WHERE df_file_name = 'x12_810_invoice_c.edi'
ASSERT VALUE first_segment = 'BIG' WHERE df_file_name = 'x12_810_invoice_d.edi'
ASSERT VALUE first_segment = 'BIG' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE invoice_date = '20030310' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE invoice_number = 'DO091003TESTINV01' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE invoice_number = 'DO091003TESTINV01TAX' WHERE df_file_name = 'x12_810_invoice_b.edi'
ASSERT VALUE invoice_date = '20030310' WHERE df_file_name = 'x12_810_invoice_b.edi'
ASSERT VALUE invoice_number = 'DO091003TESTHDRINV01' WHERE df_file_name = 'x12_810_invoice_c.edi'
ASSERT VALUE invoice_number = 'DO091003TESTHDRINV02' WHERE df_file_name = 'x12_810_invoice_d.edi'
ASSERT VALUE invoice_number = 'SG427254' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE invoice_date = '20000513' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
SELECT
    df_file_name,
    json_extract_path_text(df_transaction_json, '0', 'segment') AS first_segment,
    json_extract_path_text(df_transaction_json, '0', 'elements', '0', 'value') AS invoice_date,
    json_extract_path_text(df_transaction_json, '0', 'elements', '1', 'value') AS invoice_number
FROM {{zone_name}}.edi_demos.json_extraction_messages
WHERE st_1 = '810'
ORDER BY df_file_name;


-- ============================================================================
-- 7. First Two Segments Sample — json_extract_path_text
-- ============================================================================
-- Extracts the segment name and description of the first two body segments
-- from a single transaction. This is useful for analysts exploring an
-- unfamiliar transaction structure — they can see what segments appear
-- before writing targeted element extraction queries.
--
-- What you'll see:
--   - df_file_name:               Source file (the simple 850)
--   - txn_type:                   '850'
--   - first_segment_name:         Segment code of the first body segment (e.g., BEG)
--   - first_segment_description:  Human-readable name of the first body segment
--   - second_segment_name:        Segment code of the second body segment
--   - second_segment_description: Human-readable name of the second body segment

ASSERT ROW_COUNT = 1
ASSERT VALUE txn_type = '850' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE first_segment_name = 'BEG' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE second_segment_name = 'REF' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE first_segment_description IS NOT NULL WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE second_segment_description IS NOT NULL WHERE df_file_name = 'x12_850_purchase_order.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    json_extract_path_text(df_transaction_json, '0', 'segment') AS first_segment_name,
    json_extract_path_text(df_transaction_json, '0', 'name') AS first_segment_description,
    json_extract_path_text(df_transaction_json, '1', 'segment') AS second_segment_name,
    json_extract_path_text(df_transaction_json, '1', 'name') AS second_segment_description
FROM {{zone_name}}.edi_demos.json_extraction_messages
WHERE df_file_name = 'x12_850_purchase_order.edi';


-- ============================================================================
-- 8. VERIFY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly and JSON
-- functions work as expected. All checks should return PASS.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'first_850_segment_beg'
ASSERT VALUE result = 'PASS' WHERE check_name = 'first_810_segment_big'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_is_array'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'segments_have_length'
ASSERT VALUE result = 'PASS' WHERE check_name = 'transaction_count_14'
SELECT check_name, result FROM (

    -- Check 1: Total transaction count = 14 (one per .edi file)
    SELECT 'transaction_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.json_extraction_messages) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: df_transaction_json is populated for all 14 transactions
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.json_extraction_messages
                       WHERE df_transaction_json IS NOT NULL) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: json_typeof confirms all are arrays
    SELECT 'json_is_array' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.json_extraction_messages
                       WHERE json_typeof(df_transaction_json) = 'array') = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: json_array_length returns > 0 for all transactions
    SELECT 'segments_have_length' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.json_extraction_messages
                       WHERE json_array_length(df_transaction_json) > 0) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: First body segment of 850s is BEG
    SELECT 'first_850_segment_beg' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.json_extraction_messages
                       WHERE st_1 = '850'
                         AND json_extract_path_text(df_transaction_json, '0', 'segment') = 'BEG') = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: First body segment of 810s is BIG
    SELECT 'first_810_segment_big' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.json_extraction_messages
                       WHERE st_1 = '810'
                         AND json_extract_path_text(df_transaction_json, '0', 'segment') = 'BIG') = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
