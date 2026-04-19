-- ============================================================================
-- Demo: Regional Sales Performance — Window Analytics with UniForm
-- ============================================================================
-- Tests ROW_NUMBER, RANK, LAG, LEAD, and running totals on a UniForm
-- Iceberg table. Proves that window function results are identical when
-- read through both the Delta and Iceberg metadata chains.

-- ============================================================================
-- Query 1: Baseline — All 40 Sales Present
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.iceberg_demos.sales ORDER BY sale_id;

-- ============================================================================
-- Query 2: ROW_NUMBER — Top Sale per Rep
-- ============================================================================
-- Uses ROW_NUMBER() PARTITION BY rep_name ORDER BY sale_amount DESC
-- to find each rep's highest-value sale.

ASSERT ROW_COUNT = 7
ASSERT VALUE sale_amount = 8900.00 WHERE rep_name = 'Emma Clark'
ASSERT VALUE sale_amount = 9200.00 WHERE rep_name = 'Olivia Kim'
ASSERT VALUE sale_amount = 9400.00 WHERE rep_name = 'Sophia Grant'
ASSERT VALUE sale_amount = 7800.00 WHERE rep_name = 'Liam Foster'
ASSERT VALUE sale_amount = 5500.00 WHERE rep_name = 'James Lee'
ASSERT VALUE sale_amount = 8200.00 WHERE rep_name = 'Ava Moore'
ASSERT VALUE sale_amount = 5200.00 WHERE rep_name = 'Noah Hayes'
SELECT rep_name, sale_id, product_category, sale_amount
FROM (
    SELECT
        rep_name,
        sale_id,
        product_category,
        sale_amount,
        ROW_NUMBER() OVER (PARTITION BY rep_name ORDER BY sale_amount DESC) AS rn
    FROM {{zone_name}}.iceberg_demos.sales
) ranked
WHERE rn = 1
ORDER BY sale_amount DESC;

-- ============================================================================
-- Query 3: RANK — Reps by Total Revenue
-- ============================================================================
-- Ranks reps by their total revenue across all sales.

ASSERT ROW_COUNT = 7
ASSERT VALUE total_revenue = 33100.00 WHERE revenue_rank = 1
ASSERT VALUE rep_name = 'Olivia Kim' WHERE revenue_rank = 1
ASSERT VALUE total_revenue = 32700.00 WHERE revenue_rank = 2
ASSERT VALUE rep_name = 'Sophia Grant' WHERE revenue_rank = 2
ASSERT VALUE total_revenue = 20100.00 WHERE revenue_rank = 7
SELECT
    RANK() OVER (ORDER BY SUM(sale_amount) DESC) AS revenue_rank,
    rep_name,
    SUM(sale_amount) AS total_revenue,
    COUNT(*) AS sale_count
FROM {{zone_name}}.iceberg_demos.sales
GROUP BY rep_name
ORDER BY revenue_rank;

-- ============================================================================
-- Query 4: Running Total per Region
-- ============================================================================
-- Cumulative revenue per region ordered by sale_date, sale_id.

ASSERT ROW_COUNT = 4
ASSERT VALUE region_total = 61700.00 WHERE region = 'Northeast'
ASSERT VALUE region_total = 56800.00 WHERE region = 'West'
ASSERT VALUE region_total = 46700.00 WHERE region = 'Southeast'
ASSERT VALUE region_total = 20100.00 WHERE region = 'Midwest'
SELECT region, region_total, sale_count
FROM (
    SELECT
        region,
        sale_amount,
        SUM(sale_amount) OVER (PARTITION BY region ORDER BY sale_date, sale_id) AS running_total,
        SUM(sale_amount) OVER (PARTITION BY region) AS region_total,
        COUNT(*) OVER (PARTITION BY region) AS sale_count,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date DESC, sale_id DESC) AS rn
    FROM {{zone_name}}.iceberg_demos.sales
) t
WHERE rn = 1
ORDER BY region_total DESC;

-- ============================================================================
-- Query 5: LAG / LEAD — Emma Clark's Sale-to-Sale Momentum
-- ============================================================================
-- Shows each of Emma Clark's sales with previous and next sale amounts.

ASSERT ROW_COUNT = 6
ASSERT VALUE prev_sale IS NULL WHERE sale_id = 1
ASSERT VALUE next_sale = 8900.00 WHERE sale_id = 6
ASSERT VALUE sale_change = 6800.00 WHERE sale_id = 13
ASSERT VALUE sale_change = -5200.00 WHERE sale_id = 20
ASSERT VALUE next_sale IS NULL WHERE sale_id = 34
SELECT
    sale_id,
    sale_amount,
    LAG(sale_amount) OVER (ORDER BY sale_date, sale_id) AS prev_sale,
    LEAD(sale_amount) OVER (ORDER BY sale_date, sale_id) AS next_sale,
    sale_amount - LAG(sale_amount) OVER (ORDER BY sale_date, sale_id) AS sale_change
FROM {{zone_name}}.iceberg_demos.sales
WHERE rep_name = 'Emma Clark'
ORDER BY sale_date, sale_id;

-- ============================================================================
-- Query 6: Category Revenue with Percentage of Total
-- ============================================================================
-- Uses SUM() OVER () (unbounded window) to compute each category's share.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_revenue = 98200.00 WHERE product_category = 'Electronics'
ASSERT VALUE total_revenue = 62300.00 WHERE product_category = 'Furniture'
ASSERT VALUE total_revenue = 24800.00 WHERE product_category = 'Clothing'
SELECT
    product_category,
    SUM(sale_amount) AS total_revenue,
    COUNT(*) AS sale_count,
    ROUND(100.0 * SUM(sale_amount) / SUM(SUM(sale_amount)) OVER (), 2) AS pct_of_total
FROM {{zone_name}}.iceberg_demos.sales
GROUP BY product_category
ORDER BY total_revenue DESC;

-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.sales_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sales_iceberg
USING ICEBERG
LOCATION '{{data_subdir}}/sales';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sales_iceberg TO USER {{current_user}};

-- ============================================================================
-- Iceberg Verify 1: Row Count
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.iceberg_demos.sales_iceberg ORDER BY sale_id;

-- ============================================================================
-- Iceberg Verify 2: Top Sale per Rep via Iceberg
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE sale_amount = 9400.00 WHERE rep_name = 'Sophia Grant'
ASSERT VALUE sale_amount = 9200.00 WHERE rep_name = 'Olivia Kim'
SELECT rep_name, sale_id, sale_amount
FROM (
    SELECT
        rep_name,
        sale_id,
        sale_amount,
        ROW_NUMBER() OVER (PARTITION BY rep_name ORDER BY sale_amount DESC) AS rn
    FROM {{zone_name}}.iceberg_demos.sales_iceberg
) ranked
WHERE rn = 1
ORDER BY sale_amount DESC;

-- ============================================================================
-- Iceberg Verify 3: Region Totals via Iceberg
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 61700.00 WHERE region = 'Northeast'
ASSERT VALUE total_revenue = 20100.00 WHERE region = 'Midwest'
SELECT
    region,
    SUM(sale_amount) AS total_revenue,
    COUNT(*) AS sale_count
FROM {{zone_name}}.iceberg_demos.sales_iceberg
GROUP BY region
ORDER BY total_revenue DESC;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_sales = 40
ASSERT VALUE total_revenue = 185300.00
ASSERT VALUE rep_count = 7
ASSERT VALUE region_count = 4
ASSERT VALUE category_count = 3
ASSERT VALUE avg_sale = 4632.50
ASSERT VALUE max_sale = 9400.00
ASSERT VALUE min_sale = 1200.00
SELECT
    COUNT(*) AS total_sales,
    SUM(sale_amount) AS total_revenue,
    COUNT(DISTINCT rep_name) AS rep_count,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT product_category) AS category_count,
    ROUND(AVG(sale_amount), 2) AS avg_sale,
    MAX(sale_amount) AS max_sale,
    MIN(sale_amount) AS min_sale
FROM {{zone_name}}.iceberg_demos.sales;
