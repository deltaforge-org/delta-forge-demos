-- ============================================================================
-- Iceberg Cross-Format Join — Retail Store Analytics — Queries
-- ============================================================================
-- Demonstrates cross-format interoperability:
--   1. Delta table (sales) with UniForm V2 Iceberg metadata
--   2. CSV external table (stores)
--   3. Iceberg external table (same physical data as Delta sales)
--
-- Queries progress from single-table baselines to cross-format JOINs,
-- then register an Iceberg external table over the same sales data and
-- re-run the JOINs through the Iceberg metadata chain.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — CSV Store Locations (10 Stores)
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE store_name = 'Downtown Flagship' WHERE store_id = 'S001'
ASSERT VALUE region = 'Northeast' WHERE store_id = 'S001'
SELECT *
FROM {{zone_name}}.retail_demo.stores
ORDER BY store_id;


-- ============================================================================
-- Query 2: Baseline — Delta Sales (40 Transactions)
-- ============================================================================

ASSERT ROW_COUNT = 40
ASSERT VALUE product_name = 'Running Pro X' WHERE txn_id = 1
ASSERT VALUE quantity = 3 WHERE txn_id = 1
SELECT *
FROM {{zone_name}}.retail_demo.sales
ORDER BY txn_id;


-- ============================================================================
-- Query 3: Cross-Format JOIN — Revenue by Store with Location
-- ============================================================================
-- Joins Delta sales with CSV store locations.

ASSERT ROW_COUNT = 10
ASSERT VALUE store_name = 'Downtown Flagship' WHERE store_id = 'S001'
ASSERT VALUE revenue = 1579.9 WHERE store_id = 'S001'
ASSERT VALUE store_name = 'Michigan Avenue' WHERE store_id = 'S003'
ASSERT VALUE revenue = 1999.85 WHERE store_id = 'S003'
ASSERT VALUE store_name = 'Sunset Boulevard' WHERE store_id = 'S004'
ASSERT VALUE revenue = 2129.82 WHERE store_id = 'S004'
SELECT
    s.store_id,
    st.store_name,
    st.city,
    st.region,
    COUNT(*) AS txn_count,
    SUM(s.quantity) AS total_qty,
    ROUND(SUM(s.quantity * s.unit_price), 2) AS revenue
FROM {{zone_name}}.retail_demo.sales s
JOIN {{zone_name}}.retail_demo.stores st ON s.store_id = st.store_id
GROUP BY s.store_id, st.store_name, st.city, st.region
ORDER BY s.store_id;


-- ============================================================================
-- Query 4: Cross-Format JOIN — Revenue by Region
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE revenue = 1999.85 WHERE region = 'Midwest'
ASSERT VALUE revenue = 3419.76 WHERE region = 'Northeast'
ASSERT VALUE revenue = 1659.86 WHERE region = 'South'
ASSERT VALUE revenue = 2239.78 WHERE region = 'Southeast'
ASSERT VALUE revenue = 5509.48 WHERE region = 'West'
SELECT
    st.region,
    COUNT(*) AS txn_count,
    ROUND(SUM(s.quantity * s.unit_price), 2) AS revenue
FROM {{zone_name}}.retail_demo.sales s
JOIN {{zone_name}}.retail_demo.stores st ON s.store_id = st.store_id
GROUP BY st.region
ORDER BY st.region;


-- ============================================================================
-- Query 5: Cross-Format JOIN — Top Products by Region
-- ============================================================================

ASSERT ROW_COUNT = 5
SELECT
    st.region,
    s.product_name,
    SUM(s.quantity) AS total_qty,
    ROUND(SUM(s.quantity * s.unit_price), 2) AS revenue
FROM {{zone_name}}.retail_demo.sales s
JOIN {{zone_name}}.retail_demo.stores st ON s.store_id = st.store_id
GROUP BY st.region, s.product_name
ORDER BY st.region, revenue DESC
LIMIT 5;


-- ============================================================================
-- Query 6: Grand Totals — Delta Path
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_txns = 40
ASSERT VALUE total_qty = 127
ASSERT VALUE total_revenue = 14828.73
SELECT
    COUNT(*) AS total_txns,
    SUM(quantity) AS total_qty,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.retail_demo.sales;


-- ============================================================================
-- VERIFY: All Checks — Delta + CSV Join
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_txns = 40
ASSERT VALUE total_revenue = 14828.73
ASSERT VALUE store_count = 10
ASSERT VALUE region_count = 5
SELECT
    COUNT(*) AS total_txns,
    ROUND(SUM(s.quantity * s.unit_price), 2) AS total_revenue,
    COUNT(DISTINCT st.store_id) AS store_count,
    COUNT(DISTINCT st.region) AS region_count
FROM {{zone_name}}.retail_demo.sales s
JOIN {{zone_name}}.retail_demo.stores st ON s.store_id = st.store_id;


-- ============================================================================
-- ICEBERG READ-BACK + CROSS-FORMAT JOIN VERIFICATION
-- ============================================================================
-- Register an Iceberg external table over the same physical Delta data.
-- This reads through the V2 metadata chain. Then JOIN with the CSV table
-- to prove 3-way format interop: Iceberg (read) + CSV (read) + Delta (write).

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.retail_demo.sales_iceberg
USING ICEBERG
LOCATION '{{data_path}}/sales';

GRANT ADMIN ON TABLE {{zone_name}}.retail_demo.sales_iceberg TO USER {{current_user}};


-- ============================================================================
-- Iceberg Verify 1: Row Count — 40 Sales via Iceberg Reader
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.retail_demo.sales_iceberg ORDER BY txn_id;


-- ============================================================================
-- Iceberg Verify 2: Spot-Check Sale via Iceberg
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE store_id = 'S001' WHERE txn_id = 1
ASSERT VALUE product_name = 'Running Pro X' WHERE txn_id = 1
ASSERT VALUE quantity = 3 WHERE txn_id = 1
ASSERT VALUE unit_price = 129.99 WHERE txn_id = 1
SELECT *
FROM {{zone_name}}.retail_demo.sales_iceberg
WHERE txn_id = 1;


-- ============================================================================
-- Iceberg Verify 3: Iceberg + CSV Join — Revenue by Store
-- ============================================================================
-- The same JOIN as Query 3, but reading sales through Iceberg metadata.

ASSERT ROW_COUNT = 10
ASSERT VALUE store_name = 'Downtown Flagship' WHERE store_id = 'S001'
ASSERT VALUE revenue = 1579.9 WHERE store_id = 'S001'
ASSERT VALUE store_name = 'Sunset Boulevard' WHERE store_id = 'S004'
ASSERT VALUE revenue = 2129.82 WHERE store_id = 'S004'
SELECT
    si.store_id,
    st.store_name,
    st.city,
    st.region,
    COUNT(*) AS txn_count,
    SUM(si.quantity) AS total_qty,
    ROUND(SUM(si.quantity * si.unit_price), 2) AS revenue
FROM {{zone_name}}.retail_demo.sales_iceberg si
JOIN {{zone_name}}.retail_demo.stores st ON si.store_id = st.store_id
GROUP BY si.store_id, st.store_name, st.city, st.region
ORDER BY si.store_id;


-- ============================================================================
-- Iceberg Verify 4: Iceberg + CSV Join — Revenue by Region
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE revenue = 1999.85 WHERE region = 'Midwest'
ASSERT VALUE revenue = 3419.76 WHERE region = 'Northeast'
ASSERT VALUE revenue = 5509.48 WHERE region = 'West'
SELECT
    st.region,
    COUNT(*) AS txn_count,
    ROUND(SUM(si.quantity * si.unit_price), 2) AS revenue
FROM {{zone_name}}.retail_demo.sales_iceberg si
JOIN {{zone_name}}.retail_demo.stores st ON si.store_id = st.store_id
GROUP BY st.region
ORDER BY st.region;


-- ============================================================================
-- Iceberg Verify 5: Grand Totals via Iceberg Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_txns = 40
ASSERT VALUE total_qty = 127
ASSERT VALUE total_revenue = 14828.73
SELECT
    COUNT(*) AS total_txns,
    SUM(quantity) AS total_qty,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.retail_demo.sales_iceberg;
