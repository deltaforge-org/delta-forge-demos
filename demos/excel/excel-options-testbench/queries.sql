-- ============================================================================
-- Excel Options Testbench — Verification Queries
-- ============================================================================
-- Each query verifies a specific Excel connector option. Data in each file is
-- designed so that incorrect parsing produces obviously wrong results.
-- ============================================================================


-- ============================================================================
-- 1. SHEET NAME — Correct sheet selected from multi-sheet workbook
-- ============================================================================
-- 01_multi_sheet.xlsx "Details" sheet has 5 rows: (1,Alpha,95)...(5,Echo,88).
-- If the wrong sheet is read (Summary), row count would be 3.

ASSERT ROW_COUNT = 5
ASSERT VALUE name = 'Alpha' WHERE id = 1
ASSERT VALUE score = 95 WHERE id = 1
SELECT id, name, score
FROM {{zone_name}}.excel_opts.opt_sheet_name
ORDER BY id;


-- ============================================================================
-- 2. SHEET NAME COLUMNS — Verify column names are id, name, score
-- ============================================================================
-- The Details sheet header defines these exact column names. If the wrong
-- sheet were read, columns would differ.

ASSERT ROW_COUNT = 3
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'excel_opts'
  AND table_name = 'opt_sheet_name'
ORDER BY ordinal_position;


-- ============================================================================
-- 3. NO HEADER — Auto-generated column names (column_0..column_3)
-- ============================================================================
-- 02_no_header.xlsx has no header row. With has_header=false, the reader
-- assigns column_0, column_1, column_2, column_3. If has_header were true,
-- the first data row would be consumed as header names.

ASSERT ROW_COUNT = 5
SELECT column_0, column_1, column_2, column_3
FROM {{zone_name}}.excel_opts.opt_no_header
ORDER BY column_0;


-- ============================================================================
-- 4. NO HEADER COLUMNS — Verify column names match column_N pattern
-- ============================================================================

ASSERT ROW_COUNT = 4
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'excel_opts'
  AND table_name = 'opt_no_header'
  AND column_name LIKE 'column_%'
ORDER BY ordinal_position;


-- ============================================================================
-- 5. SKIP ROWS — First 3 data rows skipped, 5 remaining rows returned
-- ============================================================================
-- 03_skip_rows.xlsx has 8 data rows. skip_rows=3 skips the first 3 (ids 1-3),
-- returning only 5 rows starting from id=4 (Delta).

ASSERT ROW_COUNT = 5
ASSERT VALUE project = 'Delta' WHERE id = 4
ASSERT VALUE hours = 45 WHERE id = 4
SELECT id, project, hours, status
FROM {{zone_name}}.excel_opts.opt_skip_rows
ORDER BY id;


-- ============================================================================
-- 6. MAX ROWS — Only first 5 of 20 rows read
-- ============================================================================
-- 04_max_rows.xlsx "Inventory" sheet has 20 data rows. max_rows=5 should
-- limit the result to the first 5. If max_rows is ignored, 20 rows appear.

ASSERT ROW_COUNT = 5
SELECT *
FROM {{zone_name}}.excel_opts.opt_max_rows;


-- ============================================================================
-- 7. MAX ROWS VALUES — Verify the first 5 rows are present
-- ============================================================================

ASSERT VALUE sku = 'SKU-001' WHERE product = 'Bolt'
SELECT sku, product, stock, price
FROM {{zone_name}}.excel_opts.opt_max_rows
ORDER BY sku;


-- ============================================================================
-- 8. FILE FILTER — Only target file included, decoy excluded
-- ============================================================================
-- 05_target.xlsx has value=CORRECT, 05_decoy.xlsx has value=WRONG.
-- file_filter='05_target*' should exclude the decoy entirely.

ASSERT ROW_COUNT = 3
ASSERT VALUE value = 'CORRECT' WHERE id = 1
SELECT id, value
FROM {{zone_name}}.excel_opts.opt_file_filter
ORDER BY id;


-- ============================================================================
-- 9. FILE FILTER EXCLUSION — No WRONG values present
-- ============================================================================
-- If the decoy file leaked through, rows with value='WRONG' would appear.

ASSERT ROW_COUNT = 0
SELECT id, value
FROM {{zone_name}}.excel_opts.opt_file_filter
WHERE value = 'WRONG';


-- ============================================================================
-- VERIFY: All Checks — PASS/FAIL summary
-- ============================================================================
-- Cross-cutting sanity check across all five options.

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = 'sheet_name_correct_rows'
ASSERT VALUE result = 'PASS' WHERE check_name = 'no_header_auto_columns'
ASSERT VALUE result = 'PASS' WHERE check_name = 'skip_rows_correct_rows'
ASSERT VALUE result = 'PASS' WHERE check_name = 'max_rows_limited'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_filter_only_correct'
SELECT check_name, result FROM (

    -- Check 1: sheet_name — Details sheet has exactly 5 rows
    SELECT 'sheet_name_correct_rows' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel_opts.opt_sheet_name) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: no_header — columns named column_0..column_3
    SELECT 'no_header_auto_columns' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'excel_opts'
                 AND table_name = 'opt_no_header'
                 AND column_name LIKE 'column_%'
           ) = 4 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: skip_rows — 5 data rows after skipping first 3
    SELECT 'skip_rows_correct_rows' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel_opts.opt_skip_rows) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: max_rows — only 5 of 20 rows returned
    SELECT 'max_rows_limited' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel_opts.opt_max_rows) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: file_filter — no WRONG values, only CORRECT
    SELECT 'file_filter_only_correct' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.excel_opts.opt_file_filter
               WHERE value = 'WRONG'
           ) = 0
           AND (
               SELECT COUNT(*) FROM {{zone_name}}.excel_opts.opt_file_filter
               WHERE value = 'CORRECT'
           ) = 3 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
