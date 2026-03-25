-- ============================================================================
-- EDI Repeating Segments — Demo Queries
-- ============================================================================
-- Queries showcasing three repeating_segment_mode options for handling
-- multi-occurrence X12 segments: First (default), Concatenate, and ToJson.
--
-- Three tables are available (all read the same 14 X12 files):
--   repeating_first   — Default mode: only first occurrence of each segment
--   repeating_concat  — Pipe-delimited: n1_2 = "Name1|Name2|Name3"
--   repeating_json    — JSON arrays:    n1_2 = ["Name1","Name2","Name3"]
--
-- Key data points (N1 party names per file):
--   850 purchase_order:   1 N1  — John Doe
--   850 purchase_order_a: 5 N1s — Transplace Laredo, Penjamo Cutting, Test Inc., '', Supplier Name
--   850 edifabric:        1 N1  — ABC AEROSPACE
--   810 invoice_a/b/d:    6 N1s — Aaron Copeland, XYZ Bank, Philadelphia, Music Insurance..., ...
--   810 invoice_c:        4 N1s — Aaron Copeland, XYZ Bank, Philadelphia, Music Insurance...
--   810 edifabric:        1 N1  — ABC AEROSPACE CORPORATION
--   855 po_ack:           2 N1s — XYZ MANUFACTURING CO, KOHLS DEPARTMENT STORES
--   856 ship_notice:      4 N1s — WAL-MART DC 6094J-JIT, SUPPLIER NAME, ...
--   856 ship_bill_notice: 4 N1s — (all empty names)
--   824 application_advice: 5 N1s — (all empty names)
--   861 receiving_advice: 1 N1  — (empty name)
--   997 functional_ack:   0 N1s
-- ============================================================================


-- ============================================================================
-- 1. First Mode — Single Party Overview
-- ============================================================================
-- Shows n1_1 (entity code) and n1_2 (party name) from the First-mode table.
-- For files with multiple N1 segments, only the FIRST occurrence appears.
-- This is the baseline — demonstrates what data is LOST when repeating
-- segments are not explicitly handled.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:     n1_2 = 'John Doe' (only 1 N1, nothing lost)
--   - x12_850_purchase_order_a.edi:   n1_2 = 'Transplace Laredo' (4 more N1s hidden)
--   - x12_810_invoice_a.edi:          n1_2 = 'Aaron Copeland' (5 more N1s hidden)
--   - x12_855_purchase_order_ack.edi: n1_2 = 'XYZ MANUFACTURING CO' (1 more hidden)

ASSERT ROW_COUNT = 14
ASSERT VALUE party_name = 'Transplace Laredo' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE party_name = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_name = 'Aaron Copeland' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE party_name = 'XYZ MANUFACTURING CO' WHERE df_file_name = 'x12_855_purchase_order_ack.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_1 AS entity_code,
    n1_2 AS party_name
FROM {{zone_name}}.edi.repeating_first
ORDER BY df_file_name;


-- ============================================================================
-- 2. First Mode — Transaction Type Distribution
-- ============================================================================
-- Groups transactions by type to show how many documents of each X12 type
-- exist in the 14-file feed. This confirms all 8 transaction types loaded.
--
-- What you'll see:
--   - 810 (Invoice): 5 files
--   - 850 (Purchase Order): 3 files
--   - Other types: 1 file each

ASSERT ROW_COUNT = 8
ASSERT VALUE doc_count = 5 WHERE txn_type = '810'
ASSERT VALUE doc_count = 3 WHERE txn_type = '850'
ASSERT VALUE doc_count = 1 WHERE txn_type = '855'
SELECT
    st_1 AS txn_type,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi.repeating_first
GROUP BY st_1
ORDER BY doc_count DESC, st_1;


-- ============================================================================
-- 3. Concatenate Mode — All Party Names
-- ============================================================================
-- Shows n1_2 from the Concatenate table — all party names from every N1
-- segment occurrence joined with a pipe (|) separator.
--
-- Files with a single N1 show just the name (no pipes). Files with no N1
-- segments return NULL.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:     "John Doe" (single — no pipes)
--   - x12_850_purchase_order_a.edi:   "Transplace Laredo|Penjamo Cutting|Test Inc.||Supplier Name"
--   - x12_855_purchase_order_ack.edi: "XYZ MANUFACTURING CO|KOHLS DEPARTMENT STORES"
--   - x12_810_invoice_a.edi:          "Aaron Copeland|XYZ Bank|Philadelphia|Music Insurance Co. - San Fran|..."

ASSERT ROW_COUNT = 14
ASSERT VALUE party_names = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_names = 'ABC AEROSPACE' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE party_names = 'ABC AEROSPACE CORPORATION' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_1 AS entity_codes,
    n1_2 AS party_names
FROM {{zone_name}}.edi.repeating_concat
ORDER BY df_file_name;


-- ============================================================================
-- 4. ToJson Mode — Party Names as Array
-- ============================================================================
-- Shows n1_2 from the ToJson table — all party names as a JSON array.
-- Each occurrence becomes an element in the array.
--
-- Single-occurrence files produce a single-element array.
-- Files with no N1 segments return NULL.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:     ["John Doe"]
--   - x12_850_purchase_order_a.edi:   ["Transplace Laredo","Penjamo Cutting","Test Inc.","","Supplier Name"]
--   - x12_855_purchase_order_ack.edi: ["XYZ MANUFACTURING CO","KOHLS DEPARTMENT STORES"]
--   - x12_810_invoice_edifabric.edi:  ["ABC AEROSPACE CORPORATION"]

ASSERT ROW_COUNT = 14
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_1 AS entity_codes_json,
    n1_2 AS party_names_json
FROM {{zone_name}}.edi.repeating_json
ORDER BY df_file_name;


-- ============================================================================
-- 5. Compare All Three Modes — Side-by-Side
-- ============================================================================
-- Shows how the SAME file (purchase_order_a.edi with 5 N1 segments) looks
-- in each of the three repeating_segment_mode options.
--
-- What you'll see:
--   - First:       n1_2 = 'Transplace Laredo' (only first of 5)
--   - Concatenate: n1_2 = 'Transplace Laredo|Penjamo Cutting|Test Inc.||Supplier Name'
--   - ToJson:      n1_2 = '["Transplace Laredo","Penjamo Cutting","Test Inc.","","Supplier Name"]'

ASSERT ROW_COUNT = 3
SELECT
    'First (default)' AS mode,
    f.n1_1 AS entity_codes,
    f.n1_2 AS party_names
FROM {{zone_name}}.edi.repeating_first f
WHERE f.df_file_name = 'x12_850_purchase_order_a.edi'

UNION ALL

SELECT
    'Concatenate' AS mode,
    c.n1_1 AS entity_codes,
    c.n1_2 AS party_names
FROM {{zone_name}}.edi.repeating_concat c
WHERE c.df_file_name = 'x12_850_purchase_order_a.edi'

UNION ALL

SELECT
    'ToJson' AS mode,
    j.n1_1 AS entity_codes,
    j.n1_2 AS party_names
FROM {{zone_name}}.edi.repeating_json j
WHERE j.df_file_name = 'x12_850_purchase_order_a.edi'

ORDER BY mode;


-- ============================================================================
-- 6. Concatenate vs First — Value Comparison
-- ============================================================================
-- Compares n1_2 (party name) values between First and Concatenate tables
-- for files with multiple N1 segments. In First mode, only one name appears;
-- in Concatenate mode, all names are pipe-delimited (if the feature is active).
--
-- What you'll see:
--   - For single-N1 files: both columns show the same value
--   - For multi-N1 files: first_party shows one name, concat_party may show all

ASSERT ROW_COUNT = 14
SELECT
    f.df_file_name,
    f.st_1 AS txn_type,
    f.n1_2 AS first_party,
    c.n1_2 AS concat_party
FROM {{zone_name}}.edi.repeating_first f
JOIN {{zone_name}}.edi.repeating_concat c ON f.df_file_name = c.df_file_name
ORDER BY f.df_file_name;


-- ============================================================================
-- 7. Multi-Party Detection
-- ============================================================================
-- Shows which files have multiple N1 party segments by checking the
-- Concatenate table for pipe characters. Files with pipes have more than
-- one N1 occurrence; files without pipes have exactly one (or NULL for none).
--
-- This is a practical pattern: use Concatenate mode to quickly detect
-- which transactions contain repeating segments, then drill into those.
--
-- What you'll see:
--   - Files with pipes (multiple N1s): purchase_order_a, invoice_a/b/c/d,
--     purchase_order_ack, ship_notice, ship_bill_notice, application_advice
--   - Files without pipes (single N1): purchase_order, edifabric files,
--     receiving_advice
--   - NULL (no N1 at all): functional_acknowledgment

ASSERT ROW_COUNT = 14
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_2 AS party_names_concat,
    CASE
        WHEN n1_2 IS NULL THEN 'No N1 segments'
        WHEN n1_2 LIKE '%|%' THEN 'Multiple parties'
        ELSE 'Single party'
    END AS party_status
FROM {{zone_name}}.edi.repeating_concat
ORDER BY df_file_name;


-- ============================================================================
-- 8. VERIFY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly and all
-- three repeating_segment_mode tables are producing expected results.
-- All checks should return PASS.

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = 'concat_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'first_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'first_is_first_only'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'three_tables_same_count'
SELECT check_name, result FROM (

    -- Check 1: First table has 14 rows (one per .edi file)
    SELECT 'first_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_first) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Concatenate table has 14 rows
    SELECT 'concat_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: ToJson table has 14 rows
    SELECT 'json_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_json) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: All three tables have the same row count
    SELECT 'three_tables_same_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_first)
                   = (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat)
                AND (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat)
                   = (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_json)
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: First mode returns only first N1 (not the full pipe list)
    SELECT 'first_is_first_only' AS check_name,
           CASE WHEN (SELECT n1_2 FROM {{zone_name}}.edi.repeating_first
                       WHERE df_file_name = 'x12_850_purchase_order_a.edi') = 'Transplace Laredo'
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
