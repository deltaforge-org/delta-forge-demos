-- ============================================================================
-- Iceberg V2 — Retail Multi-Dimensional Aggregation — Queries
-- ============================================================================
-- Demonstrates GROUPING SETS, ROLLUP, FILTER clause, DISTINCT aggregates,
-- and HAVING on a native Iceberg V2 table with 120 retail transactions.
-- All queries are read-only.
-- ============================================================================


-- ============================================================================
-- Query 1: Full Table Scan — Baseline
-- ============================================================================
-- 120 transactions across 4 stores, 3 regions, 5 categories.

ASSERT ROW_COUNT = 120
SELECT * FROM {{zone_name}}.iceberg_demos.retail_sales
ORDER BY sale_id;


-- ============================================================================
-- Query 2: Grand Totals — Revenue Summary
-- ============================================================================
-- Gross revenue = quantity * unit_price. Net = gross * (1 - discount/100).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 120
ASSERT VALUE gross_revenue = 25506.46
ASSERT VALUE net_revenue = 23220.27
ASSERT VALUE total_units = 529
ASSERT VALUE return_count = 9
SELECT
    COUNT(*) AS total_rows,
    ROUND(SUM(quantity * unit_price), 2) AS gross_revenue,
    ROUND(SUM(quantity * unit_price * (1 - discount_pct / 100)), 2) AS net_revenue,
    SUM(quantity) AS total_units,
    SUM(is_return) AS return_count
FROM {{zone_name}}.iceberg_demos.retail_sales;


-- ============================================================================
-- Query 3: Per-Region Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 39 WHERE region = 'Central'
ASSERT VALUE cnt = 50 WHERE region = 'East'
ASSERT VALUE cnt = 31 WHERE region = 'West'
ASSERT VALUE gross = 8617.99 WHERE region = 'Central'
ASSERT VALUE gross = 9098.57 WHERE region = 'East'
ASSERT VALUE gross = 7789.9 WHERE region = 'West'
SELECT
    region,
    COUNT(*) AS cnt,
    ROUND(SUM(quantity * unit_price), 2) AS gross,
    ROUND(SUM(quantity * unit_price * (1 - discount_pct / 100)), 2) AS net
FROM {{zone_name}}.iceberg_demos.retail_sales
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 4: Per-Category Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE cnt = 28 WHERE category = 'Clothing'
ASSERT VALUE cnt = 20 WHERE category = 'Electronics'
ASSERT VALUE gross = 11302.93 WHERE category = 'Electronics'
ASSERT VALUE gross = 926.14 WHERE category = 'Grocery'
SELECT
    category,
    COUNT(*) AS cnt,
    ROUND(SUM(quantity * unit_price), 2) AS gross,
    SUM(quantity) AS units
FROM {{zone_name}}.iceberg_demos.retail_sales
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 5: GROUPING SETS — Region x Category Matrix
-- ============================================================================
-- Produces subtotals for every (region, category) combination, plus
-- per-region totals, per-category totals, and the grand total.
-- 3 regions x 5 categories = 15, plus 3 region + 5 category + 1 grand = 24.

ASSERT ROW_COUNT = 24
ASSERT VALUE gross = 25506.46 WHERE region IS NULL AND category IS NULL
ASSERT VALUE gross = 8617.99 WHERE region = 'Central' AND category IS NULL
ASSERT VALUE gross = 11302.93 WHERE region IS NULL AND category = 'Electronics'
ASSERT VALUE gross = 3586.92 WHERE region = 'Central' AND category = 'Electronics'
SELECT
    region,
    category,
    COUNT(*) AS cnt,
    ROUND(SUM(quantity * unit_price), 2) AS gross
FROM {{zone_name}}.iceberg_demos.retail_sales
GROUP BY GROUPING SETS ((region, category), (region), (category), ())
ORDER BY region NULLS LAST, category NULLS LAST;


-- ============================================================================
-- Query 6: ROLLUP — Region -> Store Hierarchy
-- ============================================================================
-- ROLLUP produces subtotals at each hierarchy level:
-- (region, store), (region), () = 4 stores + 3 regions + 1 grand = 8 rows.

ASSERT ROW_COUNT = 8
ASSERT VALUE gross = 25506.46 WHERE region IS NULL AND store_name IS NULL
ASSERT VALUE gross = 8617.99 WHERE region = 'Central' AND store_name IS NULL
ASSERT VALUE gross = 5416.54 WHERE store_name = 'Downtown Flagship'
ASSERT VALUE gross = 7789.9 WHERE store_name = 'Westside Mall'
SELECT
    region,
    store_name,
    COUNT(*) AS cnt,
    ROUND(SUM(quantity * unit_price), 2) AS gross
FROM {{zone_name}}.iceberg_demos.retail_sales
GROUP BY ROLLUP (region, store_name)
ORDER BY region NULLS LAST, store_name NULLS LAST;


-- ============================================================================
-- Query 7: FILTER Clause — Conditional Aggregates in One Pass
-- ============================================================================
-- FILTER applies a WHERE to individual aggregate functions, avoiding
-- multiple scans or CASE expressions.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_sales = 120
ASSERT VALUE returns = 9
ASSERT VALUE discounted_sales = 54
ASSERT VALUE non_return_gross = 24566.47
SELECT
    COUNT(*) AS total_sales,
    COUNT(*) FILTER (WHERE is_return = 1) AS returns,
    COUNT(*) FILTER (WHERE discount_pct > 0) AS discounted_sales,
    ROUND(SUM(quantity * unit_price) FILTER (WHERE is_return = 0), 2) AS non_return_gross
FROM {{zone_name}}.iceberg_demos.retail_sales;


-- ============================================================================
-- Query 8: DISTINCT Aggregates — Per-Region Diversity
-- ============================================================================
-- COUNT(DISTINCT ...) reveals how many unique products and salespeople
-- operate in each region.

ASSERT ROW_COUNT = 3
ASSERT VALUE distinct_categories = 5 WHERE region = 'Central'
ASSERT VALUE distinct_categories = 5 WHERE region = 'East'
ASSERT VALUE distinct_categories = 5 WHERE region = 'West'
SELECT
    region,
    COUNT(DISTINCT category) AS distinct_categories,
    COUNT(DISTINCT product_name) AS distinct_products,
    COUNT(DISTINCT salesperson) AS distinct_salespeople
FROM {{zone_name}}.iceberg_demos.retail_sales
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 9: HAVING — Categories Exceeding $3000 Gross
-- ============================================================================
-- Only categories with total gross revenue above $3000 qualify.

ASSERT ROW_COUNT = 3
ASSERT VALUE gross = 11302.93 WHERE category = 'Electronics'
ASSERT VALUE gross = 8245.29 WHERE category = 'Clothing'
ASSERT VALUE gross = 3239.05 WHERE category = 'Sports'
SELECT
    category,
    COUNT(*) AS cnt,
    ROUND(SUM(quantity * unit_price), 2) AS gross
FROM {{zone_name}}.iceberg_demos.retail_sales
GROUP BY category
HAVING SUM(quantity * unit_price) > 3000
ORDER BY gross DESC;


-- ============================================================================
-- Query 10: Per-Store Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE cnt = 23 WHERE store_name = 'Downtown Flagship'
ASSERT VALUE cnt = 39 WHERE store_name = 'Lakefront Center'
ASSERT VALUE cnt = 27 WHERE store_name = 'Midtown Express'
ASSERT VALUE cnt = 31 WHERE store_name = 'Westside Mall'
SELECT
    store_name,
    COUNT(*) AS cnt,
    ROUND(SUM(quantity * unit_price), 2) AS gross
FROM {{zone_name}}.iceberg_demos.retail_sales
GROUP BY store_name
ORDER BY store_name;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check combining all key invariants.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 120
ASSERT VALUE gross_revenue = 25506.46
ASSERT VALUE net_revenue = 23220.27
ASSERT VALUE total_units = 529
ASSERT VALUE store_count = 4
ASSERT VALUE region_count = 3
ASSERT VALUE category_count = 5
ASSERT VALUE return_count = 9
SELECT
    COUNT(*) AS total_rows,
    ROUND(SUM(quantity * unit_price), 2) AS gross_revenue,
    ROUND(SUM(quantity * unit_price * (1 - discount_pct / 100)), 2) AS net_revenue,
    SUM(quantity) AS total_units,
    COUNT(DISTINCT store_name) AS store_count,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT category) AS category_count,
    SUM(is_return) AS return_count
FROM {{zone_name}}.iceberg_demos.retail_sales;
