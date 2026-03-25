-- ============================================================================
-- Excel Multi-Sheet Reporting — Verification Queries
-- ============================================================================
-- Each query demonstrates multi-sheet Excel capabilities: reading different
-- sheets from the same workbooks, cross-sheet JOINs, and regional analysis.
-- ============================================================================


-- ============================================================================
-- 1. ALL SALES — 33 total rows across 2 regional files
-- ============================================================================

ASSERT ROW_COUNT = 33
SELECT *
FROM {{zone_name}}.excel.all_sales;


-- ============================================================================
-- 2. SALES BY REGION — Revenue per region using df_file_name
-- ============================================================================
-- East: 17 rows, $14,554.10 | West: 16 rows, $12,854.29

ASSERT ROW_COUNT = 2
ASSERT VALUE orders = 17 WHERE region LIKE '%east%'
ASSERT VALUE orders = 16 WHERE region LIKE '%west%'
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_amount BETWEEN 14553.10 AND 14555.10 WHERE region LIKE '%east%'
ASSERT WARNING VALUE total_amount BETWEEN 12853.29 AND 12855.29 WHERE region LIKE '%west%'
SELECT df_file_name AS region,
       COUNT(*) AS orders,
       ROUND(SUM(CAST(total_amount AS DOUBLE)), 2) AS total_amount
FROM {{zone_name}}.excel.all_sales
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 3. ALL RETURNS — 7 total returns across 2 regional files
-- ============================================================================

ASSERT ROW_COUNT = 7
SELECT *
FROM {{zone_name}}.excel.all_returns;


-- ============================================================================
-- 4. RETURNS BY REGION — Refund totals per region
-- ============================================================================
-- East: 4 returns, $2,999.75 | West: 3 returns, $3,539.83

ASSERT ROW_COUNT = 2
ASSERT VALUE return_count = 4 WHERE region LIKE '%east%'
ASSERT VALUE return_count = 3 WHERE region LIKE '%west%'
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_refunds BETWEEN 2998.75 AND 3000.75 WHERE region LIKE '%east%'
ASSERT WARNING VALUE total_refunds BETWEEN 3538.83 AND 3540.83 WHERE region LIKE '%west%'
SELECT df_file_name AS region,
       COUNT(*) AS return_count,
       ROUND(SUM(CAST(refund_amount AS DOUBLE)), 2) AS total_refunds
FROM {{zone_name}}.excel.all_returns
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 5. CROSS-SHEET JOIN — Sales LEFT JOIN Returns to find returned orders
-- ============================================================================
-- Joins the Sales and Returns tables on order_id. This is the key multi-sheet
-- capability: relating data across different sheets in the same workbooks.

ASSERT ROW_COUNT = 33
ASSERT VALUE returned_orders = 7
ASSERT VALUE clean_orders = 26
SELECT COUNT(*) AS total_orders,
       COUNT(r.return_id) AS returned_orders,
       COUNT(*) - COUNT(r.return_id) AS clean_orders
FROM {{zone_name}}.excel.all_sales s
LEFT JOIN {{zone_name}}.excel.all_returns r
    ON s.order_id = r.order_id;


-- ============================================================================
-- 6. NET REVENUE — Sales minus refunds per region
-- ============================================================================
-- East: $14,554.10 - $2,999.75 = $11,554.35
-- West: $12,854.29 - $3,539.83 = $9,314.46

ASSERT ROW_COUNT = 2
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE gross_sales BETWEEN 14553.10 AND 14555.10 WHERE region LIKE '%east%'
ASSERT WARNING VALUE total_refunds BETWEEN 2998.75 AND 3000.75 WHERE region LIKE '%east%'
ASSERT WARNING VALUE net_revenue BETWEEN 11553.35 AND 11555.35 WHERE region LIKE '%east%'
ASSERT WARNING VALUE gross_sales BETWEEN 12853.29 AND 12855.29 WHERE region LIKE '%west%'
ASSERT WARNING VALUE total_refunds BETWEEN 3538.83 AND 3540.83 WHERE region LIKE '%west%'
ASSERT WARNING VALUE net_revenue BETWEEN 9313.46 AND 9315.46 WHERE region LIKE '%west%'
SELECT s.df_file_name AS region,
       ROUND(SUM(CAST(s.total_amount AS DOUBLE)), 2) AS gross_sales,
       ROUND(COALESCE(SUM(CAST(r.refund_amount AS DOUBLE)), 0), 2) AS total_refunds,
       ROUND(SUM(CAST(s.total_amount AS DOUBLE)) - COALESCE(SUM(CAST(r.refund_amount AS DOUBLE)), 0), 2) AS net_revenue
FROM {{zone_name}}.excel.all_sales s
LEFT JOIN {{zone_name}}.excel.all_returns r
    ON s.order_id = r.order_id
GROUP BY s.df_file_name
ORDER BY s.df_file_name;


-- ============================================================================
-- 7. ALL STAFF — 7 total staff members across 2 regions
-- ============================================================================

ASSERT ROW_COUNT = 7
SELECT *
FROM {{zone_name}}.excel.all_staff;


-- ============================================================================
-- 8. STAFF ROSTER — Distinct roles and headcount
-- ============================================================================

ASSERT ROW_COUNT >= 3
SELECT role,
       COUNT(*) AS headcount
FROM {{zone_name}}.excel.all_staff
GROUP BY role
ORDER BY headcount DESC;


-- ============================================================================
-- 9. RETURN RATE — Returns as percentage of sales per region
-- ============================================================================
-- East: 4/17 = 23.5% | West: 3/16 = 18.8%

ASSERT ROW_COUNT = 2
ASSERT VALUE sales_count = 17 WHERE region LIKE '%east%'
ASSERT VALUE return_count = 4 WHERE region LIKE '%east%'
ASSERT VALUE sales_count = 16 WHERE region LIKE '%west%'
ASSERT VALUE return_count = 3 WHERE region LIKE '%west%'
-- Non-deterministic: floating-point division may vary slightly across platforms
ASSERT WARNING VALUE return_rate_pct BETWEEN 22.5 AND 24.5 WHERE region LIKE '%east%'
ASSERT WARNING VALUE return_rate_pct BETWEEN 17.8 AND 19.8 WHERE region LIKE '%west%'
SELECT s.df_file_name AS region,
       s.sales_count,
       COALESCE(r.return_count, 0) AS return_count,
       ROUND(CAST(COALESCE(r.return_count, 0) AS DOUBLE) / CAST(s.sales_count AS DOUBLE) * 100, 1) AS return_rate_pct
FROM (
    SELECT df_file_name, COUNT(*) AS sales_count
    FROM {{zone_name}}.excel.all_sales
    GROUP BY df_file_name
) s
LEFT JOIN (
    SELECT df_file_name, COUNT(*) AS return_count
    FROM {{zone_name}}.excel.all_returns
    GROUP BY df_file_name
) r ON s.df_file_name = r.df_file_name
ORDER BY s.df_file_name;


-- ============================================================================
-- 10. VERIFY: All Checks — Cross-cutting sanity checks
-- ============================================================================

ASSERT ROW_COUNT = 8
ASSERT VALUE result = 'PASS' WHERE check_name = 'sales_count_33'
ASSERT VALUE result = 'PASS' WHERE check_name = 'returns_count_7'
ASSERT VALUE result = 'PASS' WHERE check_name = 'staff_count_7'
ASSERT VALUE result = 'PASS' WHERE check_name = 'two_source_files'
ASSERT VALUE result = 'PASS' WHERE check_name = 'east_sales_17'
ASSERT VALUE result = 'PASS' WHERE check_name = 'west_sales_16'
ASSERT VALUE result = 'PASS' WHERE check_name = 'cross_sheet_join_works'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_metadata_populated'
SELECT check_name, result FROM (

    -- Check 1: Total sales rows = 33
    SELECT 'sales_count_33' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_sales) = 33
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Total returns rows = 7
    SELECT 'returns_count_7' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_returns) = 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Total staff rows = 7
    SELECT 'staff_count_7' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_staff) = 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Exactly 2 source files in sales table
    SELECT 'two_source_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.excel.all_sales) = 2
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: East region has 17 sales rows
    SELECT 'east_sales_17' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_sales WHERE df_file_name LIKE '%east%') = 17
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: West region has 16 sales rows
    SELECT 'west_sales_16' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_sales WHERE df_file_name LIKE '%west%') = 16
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Cross-sheet JOIN produces results (returns reference valid order_ids)
    SELECT 'cross_sheet_join_works' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_sales s
                      INNER JOIN {{zone_name}}.excel.all_returns r ON s.order_id = r.order_id) = 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: File metadata populated on all sales rows
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel.all_sales WHERE df_file_name IS NOT NULL) = 33
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
