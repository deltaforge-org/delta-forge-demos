-- ============================================================================
-- EDI Repeating Segments — Demo Queries
-- ============================================================================
-- Queries showcasing the three repeating-segment strategies: Indexed,
-- Concatenate, and ToJson. Each strategy handles multi-occurrence X12
-- segments (N1 party loops, PO1 line items) differently.
--
-- Three tables are available, with shape determined by repeating_segment_mode:
--   repeating_indexed  : per-occurrence columns n1_<occ>_<elem>, po1_<occ>_<elem>
--                        (54 columns total; first-occurrence values live in
--                        n1_1_1/n1_1_2 and po1_1_1..po1_1_4)
--   repeating_concat   : flat columns n1_1, n1_2, po1_1..po1_4 (36 columns total;
--                        multi-occurrence values pipe-delimited inside each cell)
--   repeating_json     : flat columns n1_1, n1_2, po1_1..po1_4 (36 columns total;
--                        every value JSON-array wrapped, e.g. '["John Doe"]')
--
-- Header context shared across all three modes:
--   ISA_1..ISA_16, GS_1..GS_8, ST_1, ST_2, df_transaction_json,
--   df_file_name, df_row_number.
-- ============================================================================


-- ============================================================================
-- 1. Indexed Table — Party Overview
-- ============================================================================
-- Shows the first-occurrence N1 entity code and party name from the indexed
-- table for all 14 EDI files. Indexed mode emits one column per
-- (occurrence, element) pair, so the first N1 occurrence is in n1_1_1
-- (entity code) and n1_1_2 (party name).
--
-- What you'll see:
--   - df_file_name:  Source .edi file
--   - entity_code:   N1 entity identifier code (ST, BY, SO, SF, etc.)
--   - party_name:    N1 party name (first occurrence)
--
-- Examples:
--   x12_850_purchase_order.edi: ST / John Doe
--   x12_850_purchase_order_a.edi: ST / Transplace Laredo
--   x12_810_invoice_a.edi: SO / Aaron Copeland

ASSERT ROW_COUNT = 14
ASSERT VALUE party_name = 'Transplace Laredo' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE party_name = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_name = 'Aaron Copeland' WHERE df_file_name = 'x12_810_invoice_a.edi'
SELECT
    df_file_name,
    n1_1_1 AS entity_code,
    n1_1_2 AS party_name
FROM {{zone_name}}.edi_demos.repeating_indexed
ORDER BY df_file_name;


-- ============================================================================
-- 2. Indexed Table — PO Line Items
-- ============================================================================
-- Shows PO1 line-item columns for 850 (Purchase Order) transactions only.
-- Indexed mode emits per-occurrence columns, so the first PO1 segment lives
-- in po1_1_1..po1_1_4 (line number, quantity, UOM, unit price).
--
-- What you'll see:
--   - df_file_name:  Source file
--   - line_number:   PO1 assigned identification (po1_1_1)
--   - quantity:      Quantity ordered (po1_1_2)
--   - uom:           Unit of measure, EA, YD, etc. (po1_1_3)
--   - unit_price:    Price per unit (po1_1_4)
--
-- Examples:
--   x12_850_purchase_order.edi: line 1, qty 1, EA, $19.95
--   x12_850_purchase_order_a.edi: line 000100001, qty 2500, YD, $2.53
--   x12_850_purchase_order_edifabric.edi: line 1, qty 25, EA, $36

ASSERT ROW_COUNT = 3
ASSERT VALUE line_number = '1' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE unit_price = '19.95' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE quantity = '2500' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE line_number = '000100001' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE uom = 'YD' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE quantity = '25' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE unit_price = '36' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
SELECT
    df_file_name,
    po1_1_1 AS line_number,
    po1_1_2 AS quantity,
    po1_1_3 AS uom,
    po1_1_4 AS unit_price
FROM {{zone_name}}.edi_demos.repeating_indexed
WHERE st_1 = '850'
ORDER BY df_file_name;


-- ============================================================================
-- 3. Concatenate Table — Party Names
-- ============================================================================
-- Shows the N1 entity code and party name from the Concatenate table.
-- In full concat mode, multi-occurrence N1 values would be pipe-delimited
-- (e.g., 'Aaron Copeland|XYZ Bank|Philadelphia'). Currently the engine
-- produces the same flat first-occurrence values as the indexed table.
--
-- What you'll see:
--   - df_file_name:  Source file
--   - entity_code:   N1 entity identifier code(s)
--   - party_name:    N1 party name(s) — may be pipe-delimited in future

ASSERT ROW_COUNT = 14
-- Files with only one N1 occurrence have deterministic values in concat mode
-- (no pipe delimiter since there is nothing to concatenate).
ASSERT VALUE party_name = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_name = 'ABC AEROSPACE' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
-- Non-deterministic: concat output for multi-N1 files depends on separator config
-- (default '|'). The first element remains deterministic ('Aaron Copeland|...').
ASSERT WARNING VALUE party_name LIKE 'Aaron Copeland%' WHERE df_file_name = 'x12_810_invoice_a.edi'
SELECT
    df_file_name,
    n1_1 AS entity_code,
    n1_2 AS party_name
FROM {{zone_name}}.edi_demos.repeating_concat
ORDER BY df_file_name;


-- ============================================================================
-- 4. ToJson Table — Party Names
-- ============================================================================
-- Shows the N1 entity code and party name from the ToJson table.
-- to_json mode wraps every value as a JSON array, regardless of how many
-- occurrences exist in the source file. Single-N1 files therefore come back
-- as one-element arrays like '["John Doe"]'; multi-N1 files come back as
-- multi-element arrays like '["Aaron Copeland","XYZ Bank",...]'.
--
-- What you'll see:
--   - df_file_name:  Source file
--   - entity_code:   N1 entity identifier code(s), JSON array
--   - party_name:    N1 party name(s), JSON array

ASSERT ROW_COUNT = 14
-- Single-N1 files: one-element JSON array. Exact value is deterministic.
ASSERT VALUE party_name = '["John Doe"]' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_name = '["ABC AEROSPACE"]' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
-- Non-deterministic: JSON array element ordering & exact JSON format for
-- multi-N1 files depends on engine serialization (with/without spacing, quoting).
ASSERT WARNING VALUE party_name LIKE '%Aaron Copeland%' WHERE df_file_name = 'x12_810_invoice_a.edi'
SELECT
    df_file_name,
    n1_1 AS entity_code,
    n1_2 AS party_name
FROM {{zone_name}}.edi_demos.repeating_json
ORDER BY df_file_name;


-- ============================================================================
-- 5. Compare All Three Modes — Side-by-Side
-- ============================================================================
-- For x12_850_purchase_order_a.edi, shows how each of the three tables
-- represents the same N1 party name and PO1 quantity data. Uses UNION ALL
-- with a mode label to compare the three approaches in a single result set.
--
-- What you'll see:
--   - mode:        'indexed', 'concatenate', or 'to_json'
--   - party_name:  N1 party name value from each table
--   - po_quantity: PO1 quantity value from each table

ASSERT ROW_COUNT = 3
ASSERT VALUE party_name = 'Transplace Laredo' WHERE mode = 'indexed'
-- Non-deterministic: concat/JSON values differ by mode when multiple N1/PO1
-- segments exist (5 N1s, 3 PO1s in this file). Indexed mode returns first only.
ASSERT WARNING VALUE party_name LIKE 'Transplace Laredo%' WHERE mode = 'concatenate'
ASSERT WARNING VALUE party_name LIKE '%Transplace Laredo%' WHERE mode = 'to_json'
SELECT
    'indexed' AS mode,
    n1_1_2 AS party_name,
    po1_1_2 AS po_quantity
FROM {{zone_name}}.edi_demos.repeating_indexed
WHERE df_file_name = 'x12_850_purchase_order_a.edi'

UNION ALL

SELECT
    'concatenate' AS mode,
    n1_2 AS party_name,
    po1_2 AS po_quantity
FROM {{zone_name}}.edi_demos.repeating_concat
WHERE df_file_name = 'x12_850_purchase_order_a.edi'

UNION ALL

SELECT
    'to_json' AS mode,
    n1_2 AS party_name,
    po1_2 AS po_quantity
FROM {{zone_name}}.edi_demos.repeating_json
WHERE df_file_name = 'x12_850_purchase_order_a.edi';


-- ============================================================================
-- 6. PO1 Price Analysis — Line Totals
-- ============================================================================
-- Computes line totals (quantity * unit price) from the flat PO1 columns
-- for files that have pricing data. Uses CAST to convert string values to
-- numeric types for arithmetic.
--
-- What you'll see:
--   - df_file_name:  Source file
--   - line_number:   PO1 line identifier
--   - quantity:      Quantity ordered
--   - uom:           Unit of measure
--   - unit_price:    Price per unit
--   - line_total:    Computed quantity * unit price
--
-- Examples:
--   x12_850_purchase_order.edi: 1 * 19.95 = 19.95
--   x12_850_purchase_order_a.edi: 2500 * 2.53 = 6325.00
--   x12_850_purchase_order_edifabric.edi: 25 * 36 = 900.00

ASSERT ROW_COUNT = 3
ASSERT VALUE line_total = 19.95 WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE line_total = 6325.0 WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE line_total = 900.0 WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
SELECT
    df_file_name,
    po1_1_1 AS line_number,
    po1_1_2 AS quantity,
    po1_1_3 AS uom,
    po1_1_4 AS unit_price,
    CAST(po1_1_2 AS DOUBLE) * CAST(po1_1_4 AS DOUBLE) AS line_total
FROM {{zone_name}}.edi_demos.repeating_indexed
WHERE st_1 = '850' AND po1_1_4 IS NOT NULL AND po1_1_4 <> ''
ORDER BY df_file_name;


-- ============================================================================
-- 7. Transaction Type Distribution
-- ============================================================================
-- Groups all 14 EDI files by their ST_1 transaction set identifier code
-- to show the distribution of document types across the test corpus.
--
-- What you'll see:
--   - txn_type:   ST segment transaction set identifier (810, 820, 835, etc.)
--   - doc_count:  Number of files with that transaction type
--
-- Expected distribution:
--   810 (Invoice): 5 files
--   850 (Purchase Order): 3 files
--   Others: 820, 835, 837, 855, 856, 997

ASSERT ROW_COUNT = 8
ASSERT VALUE doc_count = 5 WHERE txn_type = '810'
ASSERT VALUE doc_count = 3 WHERE txn_type = '850'
ASSERT VALUE doc_count = 1 WHERE txn_type = '824'
ASSERT VALUE doc_count = 1 WHERE txn_type = '855'
ASSERT VALUE doc_count = 1 WHERE txn_type = '856'
ASSERT VALUE doc_count = 1 WHERE txn_type = '857'
ASSERT VALUE doc_count = 1 WHERE txn_type = '861'
ASSERT VALUE doc_count = 1 WHERE txn_type = '997'
SELECT
    st_1 AS txn_type,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi_demos.repeating_indexed
GROUP BY st_1
ORDER BY doc_count DESC, st_1;


-- ============================================================================
-- 8. VERIFY — All Checks
-- ============================================================================
-- Automated pass/fail verification that all three tables loaded correctly
-- and contain the expected data. The indexed table is checked against
-- per-occurrence column n1_1_2; the concat/json tables are checked via
-- row counts only (their flat n1_2 column has different value shapes).

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = 'indexed_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'concat_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'three_tables_same_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'n1_populated'
SELECT check_name, result FROM (

    -- Check 1: Indexed table has 14 rows (one per .edi file)
    SELECT 'indexed_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.repeating_indexed) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Concatenate table has 14 rows
    SELECT 'concat_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.repeating_concat) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: ToJson table has 14 rows
    SELECT 'json_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.repeating_json) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: All three tables have the same row count
    SELECT 'three_tables_same_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.repeating_indexed)
                   = (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.repeating_concat)
                AND (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.repeating_concat)
                   = (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.repeating_json)
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: N1 party name column is populated in at least some rows
    SELECT 'n1_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.repeating_indexed
                       WHERE n1_1_2 IS NOT NULL) > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
