-- ============================================================================
-- CSV Advanced Options Testbench — Verification Queries
-- ============================================================================
-- Each query is designed so that CORRECT results prove the option is working.
-- If an option is NOT wired, the result will be obviously wrong.
-- ============================================================================


-- ============================================================================
-- 1. PIPE DELIMITER — delimiter = '|'
-- ============================================================================
-- If wired: 5 rows with 4 proper columns
-- If NOT wired: 1 column containing the whole pipe-delimited line

ASSERT VALUE row_count = 5
SELECT COUNT(*) AS row_count
FROM {{zone_name}}.csv_demos.opt_delimiter;

-- Verify column parsing works — this query fails if delimiter not wired
ASSERT ROW_COUNT = 5
ASSERT VALUE name = 'Alice' WHERE id = '1'
ASSERT VALUE amount = '250.00' WHERE id = '1'
ASSERT VALUE name = 'Bob' WHERE id = '2'
ASSERT VALUE amount = '175.50' WHERE id = '2'
ASSERT VALUE category = 'Furniture' WHERE id = '5'
ASSERT VALUE amount = '412.00' WHERE id = '5'
SELECT id, name, amount, category
FROM {{zone_name}}.csv_demos.opt_delimiter
ORDER BY id;


-- ============================================================================
-- 2. NULL VALUE — null_value = 'N/A'
-- ============================================================================
-- If wired: 2 rows have NULL score (ids 2 and 4)
-- If NOT wired: 0 rows have NULL score (N/A stays as literal string)

ASSERT VALUE null_count = 2
SELECT COUNT(*) FILTER (WHERE score IS NULL) AS null_count
FROM {{zone_name}}.csv_demos.opt_null_value;

-- Also verify status column has NULLs (rows 3 and 4 have N/A in status)
ASSERT VALUE null_status_count = 2
SELECT COUNT(*) FILTER (WHERE status IS NULL) AS null_status_count
FROM {{zone_name}}.csv_demos.opt_null_value;

ASSERT ROW_COUNT = 5
ASSERT VALUE score = '95' WHERE id = '1'
ASSERT VALUE score IS NULL WHERE id = '2'
SELECT id, name, score, status
FROM {{zone_name}}.csv_demos.opt_null_value
ORDER BY id;


-- ============================================================================
-- 3. COMMENT CHARACTER — comment_char = '#'
-- ============================================================================
-- If wired: 3 data rows (all # lines skipped)
-- If NOT wired: parser error or extra garbage rows

ASSERT VALUE row_count = 3
SELECT COUNT(*) AS row_count
FROM {{zone_name}}.csv_demos.opt_comment;

ASSERT ROW_COUNT = 3
ASSERT VALUE sensor = 'TMP-001' WHERE id = '1'
ASSERT VALUE temperature = '22.5' WHERE id = '1'
ASSERT VALUE humidity = '45.2' WHERE id = '1'
ASSERT VALUE sensor = 'TMP-002' WHERE id = '2'
ASSERT VALUE temperature = '23.1' WHERE id = '2'
ASSERT VALUE sensor = 'TMP-003' WHERE id = '3'
SELECT id, sensor, temperature, humidity
FROM {{zone_name}}.csv_demos.opt_comment
ORDER BY id;


-- ============================================================================
-- 4. SKIP STARTING ROWS — skip_starting_rows = 3
-- ============================================================================
-- If wired: 5 data rows with proper column names (id, product, warehouse...)
-- If NOT wired: column names are wrong ("Report: Quarterly..." etc.)

ASSERT VALUE row_count = 5
SELECT COUNT(*) AS row_count
FROM {{zone_name}}.csv_demos.opt_skip_rows;

-- This query would fail entirely if skip_rows not wired (no "product" column)
ASSERT ROW_COUNT = 5
ASSERT VALUE product = 'Widget A' WHERE id = '1'
ASSERT VALUE warehouse = 'West' WHERE id = '1'
ASSERT VALUE quantity = '150' WHERE id = '1'
ASSERT VALUE warehouse = 'East' WHERE id = '2'
ASSERT VALUE product = 'Widget B' WHERE id = '2'
ASSERT VALUE product = 'Tool Z' WHERE id = '5'
ASSERT VALUE unit_cost = '45.50' WHERE id = '5'
SELECT id, product, warehouse, quantity, unit_cost
FROM {{zone_name}}.csv_demos.opt_skip_rows
ORDER BY id;


-- ============================================================================
-- 5. MAX ROWS — max_rows = 5
-- ============================================================================
-- If wired: exactly 5 rows (ids 1-5)
-- If NOT wired: all 10 rows

ASSERT VALUE row_count = 5
SELECT COUNT(*) AS row_count
FROM {{zone_name}}.csv_demos.opt_max_rows;

-- Verify we got the first 5 rows (values 10+20+30+40+50 = 150)
ASSERT VALUE total_value = 150
SELECT SUM(CAST(value AS INT)) AS total_value
FROM {{zone_name}}.csv_demos.opt_max_rows;


-- ============================================================================
-- 6. TRIM WHITESPACE — trim_whitespace = 'true'
-- ============================================================================
-- If wired: name='Alice' (length 5), city='New York' (length 8)
-- If NOT wired: name='  Alice  ' (length 9), city='  New York  ' (length 12)

ASSERT VALUE name_length = 5
SELECT LENGTH(name) AS name_length
FROM {{zone_name}}.csv_demos.opt_trim
WHERE CAST(id AS INT) = 1;

-- Verify city is also trimmed: 'New York' = 8 chars (not 12 with spaces)
ASSERT VALUE city_length = 8
SELECT LENGTH(city) AS city_length
FROM {{zone_name}}.csv_demos.opt_trim
WHERE CAST(id AS INT) = 1;

-- Verify exact match works (would fail without trim)
ASSERT ROW_COUNT = 1
ASSERT VALUE city = 'New York'
SELECT id, name, city, score
FROM {{zone_name}}.csv_demos.opt_trim
WHERE name = 'Alice';


-- ============================================================================
-- 7. SEMICOLON + QUOTED FIELDS — delimiter=';' quote='"'
-- ============================================================================
-- If wired: 4 rows, descriptions with embedded semicolons parse correctly
-- If NOT wired: semicolons split columns incorrectly

ASSERT VALUE row_count = 4
SELECT COUNT(*) AS row_count
FROM {{zone_name}}.csv_demos.opt_quoted;

-- The semicolons inside description should NOT split the column
ASSERT ROW_COUNT = 4
ASSERT VALUE name = 'Widget A' WHERE id = '1'
ASSERT VALUE price = '29.99' WHERE id = '1'
ASSERT VALUE price = '49.99' WHERE id = '2'
ASSERT VALUE name = 'Gadget B' WHERE id = '2'
ASSERT VALUE price = '34.50' WHERE id = '4'
SELECT id, name, description, price
FROM {{zone_name}}.csv_demos.opt_quoted
ORDER BY id;


-- ============================================================================
-- 8. COMBINED OPTIONS — delimiter + comment + null + trim together
-- ============================================================================
-- Tests all options working simultaneously.
-- 5 data rows (comments skipped), 2 null scores, names trimmed

ASSERT VALUE total_rows = 5
SELECT COUNT(*) AS total_rows
FROM {{zone_name}}.csv_demos.opt_combined;

ASSERT VALUE null_scores = 2
SELECT COUNT(*) FILTER (WHERE score IS NULL) AS null_scores
FROM {{zone_name}}.csv_demos.opt_combined;

-- Verify trim + null + comment all work
ASSERT ROW_COUNT = 5
ASSERT VALUE name = 'Alice' WHERE id = '1'
ASSERT VALUE score = '95' WHERE id = '1'
ASSERT VALUE score IS NULL WHERE id = '2'
ASSERT VALUE department = 'Marketing' WHERE id = '2'
ASSERT VALUE name = 'Charlie' WHERE id = '3'
ASSERT VALUE score = '87' WHERE id = '3'
ASSERT VALUE department IS NULL WHERE id = '3'
ASSERT VALUE department = 'Engineering' WHERE id = '1'
ASSERT VALUE score = '100' WHERE id = '5'
ASSERT VALUE name = 'Eve' WHERE id = '5'
SELECT id, name, LENGTH(name) AS name_len, score, department
FROM {{zone_name}}.csv_demos.opt_combined
ORDER BY CAST(id AS INT);


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One row per option: every PASS proves the option is correctly wired.
-- trim_whitespace verified by exact name match — untrimmed '  Alice  '
-- would not equal 'Alice', producing FAIL.

ASSERT ROW_COUNT = 8
SELECT 'delimiter' AS option, CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result FROM {{zone_name}}.csv_demos.opt_delimiter
UNION ALL
SELECT 'null_value', CASE WHEN COUNT(*) FILTER (WHERE score IS NULL) = 2 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.csv_demos.opt_null_value
UNION ALL
SELECT 'comment_char', CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.csv_demos.opt_comment
UNION ALL
SELECT 'skip_starting_rows', CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.csv_demos.opt_skip_rows
UNION ALL
SELECT 'max_rows', CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.csv_demos.opt_max_rows
UNION ALL
SELECT 'trim_whitespace', CASE WHEN COUNT(*) FILTER (WHERE name = 'Alice') = 1 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.csv_demos.opt_trim
UNION ALL
SELECT 'semicolon_quoted', CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.csv_demos.opt_quoted
UNION ALL
SELECT 'combined_options', CASE WHEN COUNT(*) = 5 AND COUNT(*) FILTER (WHERE score IS NULL) = 2 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.csv_demos.opt_combined
ORDER BY option;
