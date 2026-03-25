-- ============================================================================
-- EDI Repeating Segments — Demo Queries
-- ============================================================================
-- Queries showcasing three repeating_segment_mode options for handling
-- multi-occurrence X12 segments: Indexed, Concatenate, and ToJson.
--
-- Three tables are available (all read the same 14 X12 files):
--   repeating_indexed  — Numbered columns: n1_1_2, n1_2_2, n1_3_2, ...
--   repeating_concat   — Pipe-delimited:   n1_2 = "Name1|Name2|Name3"
--   repeating_json     — JSON arrays:      n1_2 = ["Name1","Name2","Name3"]
--
-- Indexed column naming convention:
--   {segment}_{occurrence}_{element}  (all 1-based)
--   n1_1_2  = N1 segment, 1st occurrence, element 2 (party name)
--   n1_3_1  = N1 segment, 3rd occurrence, element 1 (entity code)
--   po1_2_1 = PO1 segment, 2nd occurrence, element 1 (line item number)
--
-- Key data points (N1 party names per file):
--   810 invoices (a,b,d): 6 N1s each — Aaron Copeland, XYZ Bank, Philadelphia, ...
--   810 invoice_c:        4 N1s — Aaron Copeland, XYZ Bank, Philadelphia, Music Insurance
--   810 edifabric:        1 N1  — ABC AEROSPACE CORPORATION
--   824 application:      5 N1s — entity codes SU/SF/ST/MA/CS (names empty)
--   850 purchase_order:   1 N1  — John Doe
--   850 purchase_order_a: 5 N1s — Transplace Laredo, Penjamo Cutting, Test Inc., ...
--   850 edifabric:        1 N1  — ABC AEROSPACE
--   855 po_ack:           2 N1s — XYZ MANUFACTURING CO, KOHLS DEPARTMENT STORES
--   856 ship_notice:      4 N1s — WAL-MART DC 6094J-JIT, SUPPLIER NAME, ...
--   856 ship_bill_notice: 4 N1s — entity codes SF/ST/BT/VN (names empty)
--   861 receiving_advice: 1 N1  — code=SU (name empty)
--   997 functional_ack:   0 N1s
-- ============================================================================


-- ============================================================================
-- 1. Indexed Mode — Multi-Address Overview
-- ============================================================================
-- Shows all N1 occurrence columns from the Indexed table. Each occurrence
-- of the N1 segment gets its own set of columns:
--   n1_1_1 / n1_1_2 = 1st N1 entity code / party name
--   n1_2_1 / n1_2_2 = 2nd N1 entity code / party name
--   n1_3_1 / n1_3_2 = 3rd N1 entity code / party name
--   ... up to n1_6_1 / n1_6_2 (max_repeating_segments = 6)
--
-- Files with fewer N1 occurrences have NULL in the higher columns.
-- The 997 functional acknowledgment has no N1 segments at all (all NULL).
--
-- What you'll see:
--   - For x12_850_purchase_order_a.edi:
--       n1_1_2 = 'Transplace Laredo', n1_2_2 = 'Penjamo Cutting',
--       n1_3_2 = 'Test Inc.', n1_5_2 = 'Supplier Name'
--   - For x12_855_purchase_order_ack.edi:
--       n1_1_2 = 'XYZ MANUFACTURING CO', n1_2_2 = 'KOHLS DEPARTMENT STORES'

ASSERT ROW_COUNT = 14
ASSERT VALUE n1_1_2 = 'Transplace Laredo' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE n1_2_2 = 'Penjamo Cutting' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE n1_3_2 = 'Test Inc.' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE n1_5_2 = 'Supplier Name' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE n1_1_2 = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE n1_1_2 = 'Aaron Copeland' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE n1_2_2 = 'XYZ Bank' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE n1_1_2 = 'XYZ MANUFACTURING CO' WHERE df_file_name = 'x12_855_purchase_order_ack.edi'
ASSERT VALUE n1_2_2 = 'KOHLS DEPARTMENT STORES' WHERE df_file_name = 'x12_855_purchase_order_ack.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    n1_1_1 AS n1_code_1,
    n1_1_2 AS n1_name_1,
    n1_2_1 AS n1_code_2,
    n1_2_2 AS n1_name_2,
    n1_3_1 AS n1_code_3,
    n1_3_2 AS n1_name_3,
    n1_4_1 AS n1_code_4,
    n1_4_2 AS n1_name_4,
    n1_5_1 AS n1_code_5,
    n1_5_2 AS n1_name_5,
    n1_6_1 AS n1_code_6,
    n1_6_2 AS n1_name_6
FROM {{zone_name}}.edi.repeating_indexed
ORDER BY df_file_name;


-- ============================================================================
-- 2. Indexed Mode — PO Line Items
-- ============================================================================
-- Shows PO1 occurrence columns for 850 Purchase Order transactions.
-- Each PO1 line item gets numbered columns:
--   po1_1_1 = 1st line item number,  po1_1_2 = 1st quantity
--   po1_1_3 = 1st unit of measure,   po1_1_4 = 1st unit price
--   po1_2_1 = 2nd line item number,  po1_2_2 = 2nd quantity, etc.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:   1 PO1 — line=1, qty=1, uom=EA, price=19.95
--   - x12_850_purchase_order_a.edi: 3 PO1s — 000100001/2500/2.53,
--                                             000200001/2000/3.41,
--                                             000200002/1000/3.41
--   - x12_850_purchase_order_edifabric.edi: 1 PO1 — line=1, qty=25, uom=EA, price=36

ASSERT ROW_COUNT = 3
ASSERT VALUE po1_1_1 = '1' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po1_1_2 = '1' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po1_1_4 = '19.95' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po1_1_1 = '000100001' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po1_1_2 = '2500' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po1_1_4 = '2.53' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po1_2_1 = '000200001' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po1_2_2 = '2000' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po1_3_1 = '000200002' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po1_3_2 = '1000' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
SELECT
    df_file_name,
    po1_1_1 AS line_1_number,
    po1_1_2 AS line_1_qty,
    po1_1_3 AS line_1_uom,
    po1_1_4 AS line_1_price,
    po1_2_1 AS line_2_number,
    po1_2_2 AS line_2_qty,
    po1_2_3 AS line_2_uom,
    po1_2_4 AS line_2_price,
    po1_3_1 AS line_3_number,
    po1_3_2 AS line_3_qty,
    po1_3_3 AS line_3_uom,
    po1_3_4 AS line_3_price
FROM {{zone_name}}.edi.repeating_indexed
WHERE st_1 = '850'
ORDER BY df_file_name;


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
--   - x12_850_purchase_order.edi: "John Doe" (1 N1)
--   - x12_850_purchase_order_a.edi: "Transplace Laredo|Penjamo Cutting|Test Inc.||Supplier Name"
--   - x12_855_purchase_order_ack.edi: "XYZ MANUFACTURING CO|KOHLS DEPARTMENT STORES"
--   - x12_810_invoice_a.edi: "Aaron Copeland|XYZ Bank|Philadelphia|Music Insurance Co. - San Fran|Philadelphia|Music Insurance Co. - San Fran"

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
--   - x12_850_purchase_order.edi: ["John Doe"]
--   - x12_855_purchase_order_ack.edi: ["XYZ MANUFACTURING CO","KOHLS DEPARTMENT STORES"]
--   - x12_810_invoice_edifabric.edi: ["ABC AEROSPACE CORPORATION"]

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
-- Compares the output of all three repeating_segment_mode options for the
-- same file (x12_850_purchase_order_a.edi — a complex PO with 5 N1 segments
-- and 3 PO1 line items).
--
-- What you'll see:
--   - Indexed:     Individual columns — n1_1_2='Transplace Laredo', n1_2_2='Penjamo Cutting'
--   - Concatenate: Pipe-delimited — "Transplace Laredo|Penjamo Cutting|Test Inc.||Supplier Name"
--   - ToJson:      JSON array — ["Transplace Laredo","Penjamo Cutting","Test Inc.","","Supplier Name"]

ASSERT ROW_COUNT = 3
SELECT
    'Indexed' AS mode,
    idx.n1_1_2 AS first_party,
    idx.n1_2_2 AS second_party,
    idx.n1_3_2 AS third_party,
    idx.po1_1_2 AS first_po1_qty,
    idx.po1_2_2 AS second_po1_qty
FROM {{zone_name}}.edi.repeating_indexed idx
WHERE idx.df_file_name = 'x12_850_purchase_order_a.edi'

UNION ALL

SELECT
    'Concatenate' AS mode,
    cat.n1_2 AS first_party,
    NULL AS second_party,
    NULL AS third_party,
    cat.po1_2 AS first_po1_qty,
    NULL AS second_po1_qty
FROM {{zone_name}}.edi.repeating_concat cat
WHERE cat.df_file_name = 'x12_850_purchase_order_a.edi'

UNION ALL

SELECT
    'ToJson' AS mode,
    jsn.n1_2 AS first_party,
    NULL AS second_party,
    NULL AS third_party,
    jsn.po1_2 AS first_po1_qty,
    NULL AS second_po1_qty
FROM {{zone_name}}.edi.repeating_json jsn
WHERE jsn.df_file_name = 'x12_850_purchase_order_a.edi'

ORDER BY mode;


-- ============================================================================
-- 6. Indexed PO1 Price Analysis — Line Item Totals
-- ============================================================================
-- Uses the Indexed table to compute line-item totals from PO1 occurrences.
-- Each PO1 occurrence provides quantity (element 2) and unit price (element 4).
-- Line total = quantity * unit price for each occurrence.
--
-- What you'll see:
--   - x12_850_purchase_order.edi:        line 1: 1 * 19.95 = 19.95
--   - x12_850_purchase_order_a.edi:      line 1: 2500 * 2.53 = 6325.00
--                                         line 2: 2000 * 3.41 = 6820.00
--                                         line 3: 1000 * 3.41 = 3410.00
--   - x12_850_purchase_order_edifabric:  line 1: 25 * 36 = 900.00

ASSERT ROW_COUNT = 3
SELECT
    df_file_name,
    po1_1_1 AS line_1_id,
    CAST(po1_1_2 AS DOUBLE) AS line_1_qty,
    CAST(po1_1_4 AS DOUBLE) AS line_1_price,
    ROUND(CAST(po1_1_2 AS DOUBLE) * CAST(po1_1_4 AS DOUBLE), 2) AS line_1_total,
    po1_2_1 AS line_2_id,
    CAST(po1_2_2 AS DOUBLE) AS line_2_qty,
    CAST(po1_2_4 AS DOUBLE) AS line_2_price,
    ROUND(CAST(po1_2_2 AS DOUBLE) * CAST(po1_2_4 AS DOUBLE), 2) AS line_2_total,
    po1_3_1 AS line_3_id,
    CAST(po1_3_2 AS DOUBLE) AS line_3_qty,
    CAST(po1_3_4 AS DOUBLE) AS line_3_price,
    ROUND(CAST(po1_3_2 AS DOUBLE) * CAST(po1_3_4 AS DOUBLE), 2) AS line_3_total,
    ROUND(
        COALESCE(CAST(po1_1_2 AS DOUBLE) * CAST(po1_1_4 AS DOUBLE), 0) +
        COALESCE(CAST(po1_2_2 AS DOUBLE) * CAST(po1_2_4 AS DOUBLE), 0) +
        COALESCE(CAST(po1_3_2 AS DOUBLE) * CAST(po1_3_4 AS DOUBLE), 0),
    2) AS order_total
FROM {{zone_name}}.edi.repeating_indexed
WHERE st_1 = '850'
ORDER BY df_file_name;


-- ============================================================================
-- 7. N1 Entity Role Distribution — Indexed Mode
-- ============================================================================
-- Analyzes the entity identifier codes from N1 segment occurrences in the
-- Indexed table. N1 element 1 is the entity identifier code that indicates
-- the role of each party:
--   ST = Ship To,  BY = Buyer,  SE = Seller,  SO = Sold To,
--   SF = Ship From, BT = Bill To, SU = Supplier, VN = Vendor,
--   MA = Party to Receive, CS = Consignee, RI = Remit To, etc.
--
-- This query unpacks all six indexed entity code columns and counts how
-- often each role appears across all 14 transactions.
--
-- What you'll see:
--   - Which party roles are most common in this EDI feed
--   - How many transactions include each type of trading partner

ASSERT ROW_COUNT >= 5
SELECT
    entity_code,
    COUNT(*) AS occurrence_count
FROM (
    SELECT n1_1_1 AS entity_code FROM {{zone_name}}.edi.repeating_indexed WHERE n1_1_1 IS NOT NULL
    UNION ALL
    SELECT n1_2_1 FROM {{zone_name}}.edi.repeating_indexed WHERE n1_2_1 IS NOT NULL
    UNION ALL
    SELECT n1_3_1 FROM {{zone_name}}.edi.repeating_indexed WHERE n1_3_1 IS NOT NULL
    UNION ALL
    SELECT n1_4_1 FROM {{zone_name}}.edi.repeating_indexed WHERE n1_4_1 IS NOT NULL
    UNION ALL
    SELECT n1_5_1 FROM {{zone_name}}.edi.repeating_indexed WHERE n1_5_1 IS NOT NULL
    UNION ALL
    SELECT n1_6_1 FROM {{zone_name}}.edi.repeating_indexed WHERE n1_6_1 IS NOT NULL
) all_codes
GROUP BY entity_code
ORDER BY occurrence_count DESC, entity_code;


-- ============================================================================
-- 8. VERIFY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly and all
-- three repeating_segment_mode tables are producing expected results.
-- All checks should return PASS.

ASSERT ROW_COUNT = 8
ASSERT VALUE result = 'PASS' WHERE check_name = 'concat_has_pipes'
ASSERT VALUE result = 'PASS' WHERE check_name = 'indexed_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'indexed_multi_n1'
ASSERT VALUE result = 'PASS' WHERE check_name = 'indexed_po1_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'concat_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_has_arrays'
ASSERT VALUE result = 'PASS' WHERE check_name = 'three_tables_same_count'
SELECT check_name, result FROM (

    -- Check 1: Indexed table has 14 rows (one per .edi file)
    SELECT 'indexed_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_indexed) = 14
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
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_indexed)
                   = (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat)
                AND (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat)
                   = (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_json)
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Indexed table has multiple N1 columns populated for multi-N1 files
    SELECT 'indexed_multi_n1' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_indexed
                       WHERE n1_2_2 IS NOT NULL) > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Indexed table has PO1 columns populated for 850 transactions
    SELECT 'indexed_po1_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_indexed
                       WHERE po1_1_1 IS NOT NULL AND st_1 = '850') = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Concatenate table has pipe-delimited values (contains '|')
    SELECT 'concat_has_pipes' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_concat
                       WHERE n1_2 LIKE '%|%') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: ToJson table has JSON arrays (contains '[')
    SELECT 'json_has_arrays' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi.repeating_json
                       WHERE n1_2 LIKE '[%') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
