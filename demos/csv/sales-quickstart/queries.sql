-- ============================================================================
-- Sales Schema Evolution Demo — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge unifies CSV files with evolving schemas.
-- Missing columns from older files surface as NULL when queried together.
--
-- Evolution timeline:
--   Q1 2024  base schema   id, product_name, quantity, unit_price, sale_date, region
--   Q2 2024  + sales_rep
--   Q3 2024  + discount_pct
--   Q4 2024  - region (retired), + territory
--   Q1 2025  - discount_pct (retired), + channel
-- ============================================================================


-- ============================================================================
-- 1. All Sales — Unified View
-- ============================================================================
-- Shows all 15 records across 5 quarterly files.
-- Missing columns from older files appear as NULL.
--
-- Records 1-3 have NULL sales_rep, territory, channel, discount_pct
-- Records 4-6 have NULL territory, channel, discount_pct
-- Records 10-12 have NULL region, channel
-- Records 13-15 have NULL region, discount_pct

ASSERT ROW_COUNT = 15
ASSERT VALUE product_name = 'Widget A' WHERE id = '1'
ASSERT VALUE region = 'North' WHERE id = '1'
ASSERT VALUE product_name = 'Widget B' WHERE id = '15'
ASSERT VALUE sales_rep = 'Alice' WHERE id = '15'
SELECT *
FROM {{zone_name}}.csv_demos.sales
ORDER BY id;


-- ============================================================================
-- 2. Revenue by Product
-- ============================================================================
-- Aggregates quantity * unit_price across all files.
--
-- Expected results (6 products):
--   Widget A  | 58 units | 1,805.42
--   Widget B  | 23 units | 1,204.77
--   Widget C  |  9 units |   809.91
--   Gadget X  | 35 units |   542.50
--   Gadget Z  | 39 units |   514.75
--   Gadget Y  | 21 units |   462.00
-- Total across all products: 5,339.35

ASSERT ROW_COUNT = 6
ASSERT VALUE total_revenue = 1805.42 WHERE product_name = 'Widget A'
ASSERT VALUE total_revenue = 1204.77 WHERE product_name = 'Widget B'
ASSERT VALUE total_revenue = 809.91 WHERE product_name = 'Widget C'
ASSERT VALUE total_revenue = 542.50 WHERE product_name = 'Gadget X'
ASSERT VALUE total_revenue = 514.75 WHERE product_name = 'Gadget Z'
ASSERT VALUE total_revenue = 462.00 WHERE product_name = 'Gadget Y'
ASSERT VALUE total_quantity = 58 WHERE product_name = 'Widget A'
SELECT
    product_name,
    SUM(CAST(quantity AS INT)) AS total_quantity,
    ROUND(SUM(CAST(quantity AS INT) * CAST(unit_price AS DOUBLE)), 2) AS total_revenue
FROM {{zone_name}}.csv_demos.sales
GROUP BY product_name
ORDER BY total_revenue DESC;


-- ============================================================================
-- 3. Sales Rep Performance
-- ============================================================================
-- Only Q2 2024+ have a sales_rep column; Q1 2024 rows show NULL.
--
-- Expected results:
--   Alice   | 5 sales | 1,973.47
--   Charlie | 3 sales | 1,498.10
--   Bob     | 4 sales | 1,007.93
--   NULL    | 3 sales |   859.85  (Q1 2024 — no sales_rep column)

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 1973.47 WHERE sales_rep = 'Alice'
ASSERT VALUE total_revenue = 1498.10 WHERE sales_rep = 'Charlie'
ASSERT VALUE total_revenue = 1007.93 WHERE sales_rep = 'Bob'
ASSERT VALUE total_revenue = 859.85 WHERE sales_rep IS NULL
ASSERT VALUE sale_count = 5 WHERE sales_rep = 'Alice'
ASSERT VALUE sale_count = 3 WHERE sales_rep = 'Charlie'
ASSERT VALUE sale_count = 4 WHERE sales_rep = 'Bob'
ASSERT VALUE sale_count = 3 WHERE sales_rep IS NULL
SELECT
    sales_rep,
    COUNT(*) AS sale_count,
    ROUND(SUM(CAST(quantity AS INT) * CAST(unit_price AS DOUBLE)), 2) AS total_revenue
FROM {{zone_name}}.csv_demos.sales
GROUP BY sales_rep
ORDER BY total_revenue DESC;


-- ============================================================================
-- 4. Quarterly Revenue Trends
-- ============================================================================
-- Tracks revenue per quarter across the schema-evolution timeline.
--
-- Expected results:
--   2024-1 | 3 sales |   859.85
--   2024-2 | 3 sales |   773.89
--   2024-3 | 3 sales |   901.18
--   2024-4 | 3 sales | 1,277.76
--   2025-1 | 3 sales | 1,526.67

ASSERT ROW_COUNT = 5
ASSERT WARNING VALUE total_revenue = 859.85 WHERE period = '2024-Q1'
ASSERT WARNING VALUE total_revenue = 773.89 WHERE period = '2024-Q2'
ASSERT WARNING VALUE total_revenue = 901.18 WHERE period = '2024-Q3'
ASSERT WARNING VALUE total_revenue = 1277.76 WHERE period = '2024-Q4'
ASSERT WARNING VALUE total_revenue = 1526.67 WHERE period = '2025-Q1'
ASSERT WARNING VALUE sale_count = 3 WHERE period = '2024-Q1'
SELECT
    CAST(EXTRACT(YEAR FROM sale_date) AS INT) || '-Q' || CAST(EXTRACT(QUARTER FROM sale_date) AS INT) AS period,
    COUNT(*) AS sale_count,
    ROUND(SUM(CAST(quantity AS INT) * CAST(unit_price AS DOUBLE)), 2) AS total_revenue
FROM {{zone_name}}.csv_demos.sales
GROUP BY period
ORDER BY period;


-- ============================================================================
-- 5. Region vs Territory — Schema Evolution in Action
-- ============================================================================
-- region existed in Q1-Q3 2024, then was retired in Q4 2024.
-- territory was introduced in Q4 2024, replacing region.
-- This query shows the transition: early records have region but NULL
-- territory; later records have territory but NULL region.
--
-- Expected results:
--   1  | region: North     | territory: NULL
--   2  | region: South     | territory: NULL
--   3  | region: East      | territory: NULL
--   4  | region: West      | territory: NULL
--   10 | region: NULL      | territory: Northeast
--   11 | region: NULL      | territory: Southeast

ASSERT ROW_COUNT = 15
ASSERT VALUE region = 'North' WHERE id = '1'
ASSERT VALUE region = 'South' WHERE id = '2'
ASSERT VALUE region = 'East' WHERE id = '3'
ASSERT VALUE region = 'West' WHERE id = '4'
ASSERT VALUE territory IS NULL WHERE id = '1'
ASSERT VALUE territory = 'Northeast' WHERE id = '10'
ASSERT VALUE territory = 'Southeast' WHERE id = '11'
ASSERT VALUE region IS NULL WHERE id = '10'
SELECT
    id,
    sale_date,
    region,
    territory
FROM {{zone_name}}.csv_demos.sales
ORDER BY id;


-- ============================================================================
-- 6. File Metadata — Which File Each Record Came From
-- ============================================================================
-- DeltaForge injects file metadata columns (df_file_name, df_row_number)
-- so you can trace each row back to its source file.
--
-- Expected: 3 per file
--   sales_2024_q1.csv → ids 1-3
--   sales_2024_q2.csv → ids 4-6
--   sales_2024_q3.csv → ids 7-9
--   sales_2024_q4.csv → ids 10-12
--   sales_2025_q1.csv → ids 13-15

ASSERT ROW_COUNT = 15
ASSERT VALUE df_file_name LIKE '%sales_2024_q1%' WHERE id = '1'
ASSERT VALUE df_row_number = 1 WHERE id = '1'
ASSERT VALUE df_file_name LIKE '%sales_2024_q2%' WHERE id = '4'
ASSERT VALUE df_row_number = 1 WHERE id = '4'
ASSERT VALUE df_file_name LIKE '%sales_2025_q1%' WHERE id = '13'
ASSERT VALUE df_row_number = 1 WHERE id = '13'
SELECT
    id,
    product_name,
    df_file_name,
    df_row_number
FROM {{zone_name}}.csv_demos.sales
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, grand revenue total, and key
-- schema-evolution invariants (NULL columns from retired/added fields).

ASSERT ROW_COUNT = 1
ASSERT VALUE grand_total_revenue = 5339.35
ASSERT VALUE q1_sales_rep IS NULL
ASSERT VALUE q1_territory IS NULL
ASSERT VALUE q4_region IS NULL
ASSERT VALUE q1_2025_channel IS NOT NULL
SELECT
    COUNT(*) AS row_count,
    ROUND(SUM(CAST(quantity AS INT) * CAST(unit_price AS DOUBLE)), 2) AS grand_total_revenue,
    MAX(CASE WHEN id = '1' THEN sales_rep END) AS q1_sales_rep,
    MAX(CASE WHEN id = '1' THEN territory END) AS q1_territory,
    MAX(CASE WHEN id = '10' THEN region END) AS q4_region,
    MAX(CASE WHEN id = '13' THEN channel END) AS q1_2025_channel
FROM {{zone_name}}.csv_demos.sales;
