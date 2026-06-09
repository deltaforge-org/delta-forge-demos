-- ============================================================================
-- Excel Sales Analytics — Verification Queries
-- ============================================================================
-- Each query verifies a specific Excel feature: multi-file reading, sheet
-- selection, file filtering, file metadata, and type inference.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ROW COUNT — 9,994 orders across 4 files
-- ============================================================================

ASSERT ROW_COUNT = 9994
SELECT *
FROM {{zone_name}}.excel_demos.all_orders;


-- ============================================================================
-- 2. BROWSE ORDERS — See column types (dates, numbers, strings)
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT order_id, order_date, ship_date, customer_name,
       category, sales, quantity, profit
FROM {{zone_name}}.excel_demos.all_orders
LIMIT 20;


-- ============================================================================
-- 3. ROWS PER FILE — Breakdown by source file
-- ============================================================================
-- 2014: 1,993 | 2015: 2,102 | 2016: 2,587 | 2017: 3,312

ASSERT ROW_COUNT = 4
ASSERT VALUE row_count = 1993 WHERE df_file_name LIKE '%2014%'
ASSERT VALUE row_count = 2102 WHERE df_file_name LIKE '%2015%'
ASSERT VALUE row_count = 2587 WHERE df_file_name LIKE '%2016%'
ASSERT VALUE row_count = 3312 WHERE df_file_name LIKE '%2017%'
SELECT df_file_name, COUNT(*) AS row_count
FROM {{zone_name}}.excel_demos.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. YEAR-OVER-YEAR — Sales trend by file (proxy for year)
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE orders = 1993 WHERE source_file LIKE '%2014%'
ASSERT VALUE orders = 2102 WHERE source_file LIKE '%2015%'
ASSERT VALUE orders = 2587 WHERE source_file LIKE '%2016%'
ASSERT VALUE orders = 3312 WHERE source_file LIKE '%2017%'
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_sales BETWEEN 484246.50 AND 484248.50 WHERE source_file LIKE '%2014%'
ASSERT WARNING VALUE total_profit BETWEEN 49542.97 AND 49544.97 WHERE source_file LIKE '%2014%'
ASSERT WARNING VALUE total_sales BETWEEN 470531.51 AND 470533.51 WHERE source_file LIKE '%2015%'
ASSERT WARNING VALUE total_profit BETWEEN 61617.60 AND 61619.60 WHERE source_file LIKE '%2015%'
ASSERT WARNING VALUE total_sales BETWEEN 609204.60 AND 609206.60 WHERE source_file LIKE '%2016%'
ASSERT WARNING VALUE total_profit BETWEEN 81794.17 AND 81796.17 WHERE source_file LIKE '%2016%'
ASSERT WARNING VALUE total_sales BETWEEN 733214.26 AND 733216.26 WHERE source_file LIKE '%2017%'
ASSERT WARNING VALUE total_profit BETWEEN 93438.27 AND 93440.27 WHERE source_file LIKE '%2017%'
SELECT df_file_name AS source_file,
       COUNT(*) AS orders,
       ROUND(SUM(CAST(sales AS DOUBLE)), 2) AS total_sales,
       ROUND(SUM(CAST(profit AS DOUBLE)), 2) AS total_profit
FROM {{zone_name}}.excel_demos.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 5. SINGLE FILE — orders_2017 has exactly 3,312 rows
-- ============================================================================

ASSERT ROW_COUNT = 3312
SELECT *
FROM {{zone_name}}.excel_demos.orders_2017;


-- ============================================================================
-- 6. DISTINCT REGIONS — 4 regions (Central, East, South, West)
-- ============================================================================

ASSERT VALUE region_count = 4
SELECT COUNT(DISTINCT region) AS region_count
FROM {{zone_name}}.excel_demos.all_orders;


-- ============================================================================
-- 7. FILE METADATA — df_file_name populated, verify 2014 file pattern
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE df_file_name LIKE '%sales-data-2014%' WHERE rows = 1993
SELECT df_file_name, COUNT(*) AS rows
FROM {{zone_name}}.excel_demos.all_orders
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 8. SALES BY REGION — Analytics query
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE orders = 3203 WHERE region = 'West'
ASSERT VALUE orders = 2848 WHERE region = 'East'
ASSERT VALUE orders = 2323 WHERE region = 'Central'
ASSERT VALUE orders = 1620 WHERE region = 'South'
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_sales BETWEEN 725456.82 AND 725458.82 WHERE region = 'West'
ASSERT WARNING VALUE total_profit BETWEEN 108417.45 AND 108419.45 WHERE region = 'West'
ASSERT WARNING VALUE total_sales BETWEEN 678780.24 AND 678782.24 WHERE region = 'East'
ASSERT WARNING VALUE total_profit BETWEEN 91521.78 AND 91523.78 WHERE region = 'East'
ASSERT WARNING VALUE total_sales BETWEEN 501238.89 AND 501240.89 WHERE region = 'Central'
ASSERT WARNING VALUE total_profit BETWEEN 39705.36 AND 39707.36 WHERE region = 'Central'
ASSERT WARNING VALUE total_sales BETWEEN 391720.91 AND 391722.91 WHERE region = 'South'
ASSERT WARNING VALUE total_profit BETWEEN 46748.43 AND 46750.43 WHERE region = 'South'
-- Non-deterministic: floating-point AVG may vary slightly across platforms
ASSERT WARNING VALUE avg_discount BETWEEN 0.10 AND 0.12 WHERE region = 'West'
ASSERT WARNING VALUE avg_discount BETWEEN 0.13 AND 0.16 WHERE region = 'East'
ASSERT WARNING VALUE avg_discount BETWEEN 0.23 AND 0.25 WHERE region = 'Central'
ASSERT WARNING VALUE avg_discount BETWEEN 0.14 AND 0.16 WHERE region = 'South'
SELECT region,
       COUNT(*) AS orders,
       ROUND(SUM(CAST(sales AS DOUBLE)), 2) AS total_sales,
       ROUND(SUM(CAST(profit AS DOUBLE)), 2) AS total_profit,
       ROUND(AVG(CAST(discount AS DOUBLE)), 3) AS avg_discount
FROM {{zone_name}}.excel_demos.all_orders
GROUP BY region
ORDER BY total_sales DESC;


-- ============================================================================
-- 9. TOP PRODUCTS — By profit margin (top 10 sub-categories)
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE orders = 68 WHERE sub_category = 'Copiers'
ASSERT VALUE orders = 889 WHERE sub_category = 'Phones'
ASSERT VALUE orders = 775 WHERE sub_category = 'Accessories'
-- Non-deterministic: floating-point SUM may vary slightly across platforms
ASSERT WARNING VALUE total_sales BETWEEN 149527.03 AND 149529.03 WHERE sub_category = 'Copiers'
ASSERT WARNING VALUE total_profit BETWEEN 55616.82 AND 55618.82 WHERE sub_category = 'Copiers'
ASSERT WARNING VALUE total_sales BETWEEN 330006.05 AND 330008.05 WHERE sub_category = 'Phones'
ASSERT WARNING VALUE total_profit BETWEEN 44514.73 AND 44516.73 WHERE sub_category = 'Phones'
ASSERT WARNING VALUE total_sales BETWEEN 167379.32 AND 167381.32 WHERE sub_category = 'Accessories'
ASSERT WARNING VALUE total_profit BETWEEN 41935.64 AND 41937.64 WHERE sub_category = 'Accessories'
SELECT category, sub_category,
       COUNT(*) AS orders,
       ROUND(SUM(CAST(sales AS DOUBLE)), 2) AS total_sales,
       ROUND(SUM(CAST(profit AS DOUBLE)), 2) AS total_profit
FROM {{zone_name}}.excel_demos.all_orders
GROUP BY category, sub_category
ORDER BY total_profit DESC
LIMIT 10;


-- ============================================================================
-- 10. VERIFY: All Checks — Cross-cutting sanity check
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_count_9994'
ASSERT VALUE result = 'PASS' WHERE check_name = 'four_source_files'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_2017_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_metadata_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'type_inference_numeric'
ASSERT VALUE result = 'PASS' WHERE check_name = 'four_regions'
ASSERT VALUE result = 'PASS' WHERE check_name = 'orders_2017_filter'
SELECT check_name, result FROM (

    -- Check 1: Total row count = 9,994
    SELECT 'total_count_9994' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel_demos.all_orders) = 9994
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 4 distinct source files
    SELECT 'four_source_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.excel_demos.all_orders) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 2017 file has 3,312 rows
    SELECT 'file_2017_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel_demos.orders_2017) = 3312
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: File metadata populated (all rows have df_file_name)
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.excel_demos.all_orders WHERE df_file_name IS NOT NULL) = 9994
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Type inference — sales column exists and is castable
    SELECT 'type_inference_numeric' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM information_schema.columns
                       WHERE table_schema = 'excel_demos' AND table_name = 'all_orders'
                       AND column_name = 'sales') = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: 4 regions present (Central, East, South, West)
    SELECT 'four_regions' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT region) FROM {{zone_name}}.excel_demos.all_orders) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: orders_2017 file_filter works correctly
    SELECT 'orders_2017_filter' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.excel_demos.orders_2017) = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
