-- ============================================================================
-- Iceberg UniForm Basics — Queries
-- ============================================================================
-- HOW UNIFORM WORKS
-- -----------------
-- Delta Forge reads and writes data through the Delta protocol as usual.
-- All queries below go through the Delta transaction log — nothing changes
-- about how you query the table.
--
-- The difference: when `delta.universalFormat.enabledFormats = 'iceberg'`
-- is set, a post-commit hook ALSO generates Iceberg metadata alongside
-- the Delta log after every commit. This means:
--
--   _delta_log/      ← Delta reads/writes (what these queries use)
--   metadata/        ← Iceberg metadata (generated automatically)
--     v1.metadata.json, v2.metadata.json, ...
--     snap-*.avro  (manifest lists)
--     *-m0.avro    (manifests)
--     version-hint.text
--
-- The Iceberg metadata is a SHADOW — it mirrors the Delta state but is
-- never read by Delta Forge itself. Its purpose is interoperability:
-- any Iceberg engine (Spark, Trino, DuckDB, Snowflake) can read the
-- same table using the metadata/ directory.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify the generated Iceberg metadata with:
--   python3 verify_iceberg_metadata.py <table_data_path>/product_catalog
--
-- This script reads ONLY through the Iceberg metadata chain (not Delta)
-- and confirms the data is accessible to external Iceberg engines.
-- ============================================================================
-- ============================================================================
-- EXPLORE: Verify UniForm Properties
-- ============================================================================
-- Confirm that Iceberg UniForm is enabled via SHOW TBLPROPERTIES.
-- The key properties are:
--   delta.universalFormat.enabledFormats = 'iceberg'
--   delta.columnMapping.mode = 'id'

ASSERT WARNING ROW_COUNT >= 2
SHOW TBLPROPERTIES {{zone_name}}.iceberg_demos.product_catalog;
-- ============================================================================
-- Query 1: Baseline — Full Table Scan
-- ============================================================================
-- All 15 products should be present. This data was committed with UniForm
-- enabled, so Iceberg metadata (manifest + manifest list + metadata.json)
-- was generated alongside the Delta log.

ASSERT ROW_COUNT = 15
SELECT * FROM {{zone_name}}.iceberg_demos.product_catalog
ORDER BY id;
-- ============================================================================
-- Query 2: Category Breakdown
-- ============================================================================
-- Verifies data integrity across all 3 categories.

ASSERT ROW_COUNT = 3
ASSERT VALUE product_count = 5 WHERE category = 'Electronics'
ASSERT VALUE product_count = 5 WHERE category = 'Furniture'
ASSERT VALUE product_count = 5 WHERE category = 'Audio'
SELECT
    category,
    COUNT(*) AS product_count,
    ROUND(SUM(price), 2) AS total_value,
    SUM(stock) AS total_stock
FROM {{zone_name}}.iceberg_demos.product_catalog
GROUP BY category
ORDER BY category;
-- ============================================================================
-- Query 3: Total Revenue Potential (price × stock)
-- ============================================================================
-- Grand total across all products. Each commit with UniForm enabled
-- produces a new Iceberg snapshot — this query validates the first snapshot.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_revenue_potential = 219884.85
SELECT
    ROUND(SUM(price * stock), 2) AS total_revenue_potential
FROM {{zone_name}}.iceberg_demos.product_catalog;
-- ============================================================================
-- Query 4: Category Revenue Breakdown
-- ============================================================================
-- Per-category revenue potential proves correct arithmetic across segments.

ASSERT ROW_COUNT = 3
ASSERT VALUE revenue = 106794.25 WHERE category = 'Electronics'
ASSERT VALUE revenue = 55095.60 WHERE category = 'Furniture'
ASSERT VALUE revenue = 57995.00 WHERE category = 'Audio'
SELECT
    category,
    ROUND(SUM(price * stock), 2) AS revenue
FROM {{zone_name}}.iceberg_demos.product_catalog
GROUP BY category
ORDER BY category;
-- ============================================================================
-- LEARN: DML With UniForm — INSERT New Products
-- ============================================================================
-- Every DML commit generates a new Iceberg snapshot. This INSERT creates
-- Delta version 2 and Iceberg snapshot 2.

INSERT INTO {{zone_name}}.iceberg_demos.product_catalog VALUES
    (16, 'Webcam HD',        'Electronics', 69.99,   85,  4.3),
    (17, 'Cable Management', 'Furniture',   24.99,   300, 3.8),
    (18, 'DAC Amplifier',    'Audio',       149.99,  45,  4.7);
-- ============================================================================
-- Query 5: Post-Insert Row Count
-- ============================================================================
-- Confirms all 18 products (15 original + 3 new) are present.

ASSERT ROW_COUNT = 18
SELECT * FROM {{zone_name}}.iceberg_demos.product_catalog
ORDER BY id;
-- ============================================================================
-- Query 6: Updated Category Totals After Insert
-- ============================================================================
-- Each category gained one product.

ASSERT ROW_COUNT = 3
ASSERT VALUE product_count = 6 WHERE category = 'Electronics'
ASSERT VALUE product_count = 6 WHERE category = 'Furniture'
ASSERT VALUE product_count = 6 WHERE category = 'Audio'
SELECT
    category,
    COUNT(*) AS product_count
FROM {{zone_name}}.iceberg_demos.product_catalog
GROUP BY category
ORDER BY category;
-- ============================================================================
-- LEARN: DML With UniForm — UPDATE Prices
-- ============================================================================
-- Apply a 10% price increase to Electronics. Generates Delta version 3
-- and Iceberg snapshot 3.

UPDATE {{zone_name}}.iceberg_demos.product_catalog
SET price = ROUND(price * 1.10, 2)
WHERE category = 'Electronics';
-- ============================================================================
-- Query 7: Verify Price Update
-- ============================================================================
-- Electronics prices should be 10% higher than original.

ASSERT ROW_COUNT = 6
ASSERT VALUE new_price = 1429.99 WHERE name = 'Laptop Pro'
ASSERT VALUE new_price = 32.99 WHERE name = 'Wireless Mouse'
ASSERT VALUE new_price = 76.99 WHERE name = 'Webcam HD'
SELECT
    name,
    ROUND(price, 2) AS new_price
FROM {{zone_name}}.iceberg_demos.product_catalog
WHERE category = 'Electronics'
ORDER BY id;
-- ============================================================================
-- Query 8: Time Travel — Compare Pre- and Post-Update
-- ============================================================================
-- VERSION AS OF 2 reads the state before the price update (after INSERT).
-- Each Delta version maps to an Iceberg snapshot, so time travel works
-- identically in both formats.

ASSERT ROW_COUNT = 6
ASSERT VALUE old_price = 1299.99 WHERE name = 'Laptop Pro'
ASSERT VALUE current_price = 1429.99 WHERE name = 'Laptop Pro'
SELECT
    c.name,
    ROUND(old.price, 2) AS old_price,
    ROUND(c.price, 2) AS current_price,
    ROUND(c.price - old.price, 2) AS price_diff
FROM {{zone_name}}.iceberg_demos.product_catalog c
JOIN {{zone_name}}.iceberg_demos.product_catalog VERSION AS OF 2 old
    ON c.id = old.id
WHERE c.category = 'Electronics'
ORDER BY c.id;
-- ============================================================================
-- LEARN: Inspect Table History
-- ============================================================================
-- DESCRIBE HISTORY shows all Delta versions. Each version with UniForm
-- enabled has a corresponding Iceberg snapshot in metadata/v{N}.metadata.json.

ASSERT WARNING ROW_COUNT >= 3
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.product_catalog;
-- ============================================================================
-- LEARN: Inspect Table Detail
-- ============================================================================
-- DESCRIBE DETAIL reveals the physical layout, including the metadata/
-- directory where Iceberg artifacts live.

ASSERT WARNING ROW_COUNT >= 1
DESCRIBE DETAIL {{zone_name}}.iceberg_demos.product_catalog;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, grand totals, and key invariants.
-- A user who runs only this query can verify UniForm is working correctly.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_products = 18
ASSERT VALUE total_stock = 1945
ASSERT VALUE electronics_revenue = 124018.40
ASSERT VALUE avg_rating = 4.39
SELECT
    COUNT(*) AS total_products,
    SUM(stock) AS total_stock,
    ROUND(SUM(CASE WHEN category = 'Electronics' THEN price * stock ELSE 0 END), 2) AS electronics_revenue,
    ROUND(AVG(rating), 2) AS avg_rating
FROM {{zone_name}}.iceberg_demos.product_catalog;
-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata is readable by an Iceberg engine — not just Delta.
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.product_catalog_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.product_catalog_iceberg
USING ICEBERG
LOCATION '{{data_path}}/product_catalog';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.product_catalog_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Verify 1: Row Count
-- ============================================================================
-- The Iceberg table should see all 18 products (15 original + 3 inserted).

ASSERT ROW_COUNT = 18
SELECT * FROM {{zone_name}}.iceberg_demos.product_catalog_iceberg ORDER BY id;
-- ============================================================================
-- Iceberg Verify 2: Category Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE product_count = 6 WHERE category = 'Electronics'
ASSERT VALUE product_count = 6 WHERE category = 'Furniture'
ASSERT VALUE product_count = 6 WHERE category = 'Audio'
SELECT
    category,
    COUNT(*) AS product_count
FROM {{zone_name}}.iceberg_demos.product_catalog_iceberg
GROUP BY category
ORDER BY category;
-- ============================================================================
-- Iceberg Verify 3: Grand Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_products = 18
ASSERT VALUE total_stock = 1945
ASSERT VALUE avg_rating = 4.39
SELECT
    COUNT(*) AS total_products,
    SUM(stock) AS total_stock,
    ROUND(AVG(rating), 2) AS avg_rating
FROM {{zone_name}}.iceberg_demos.product_catalog_iceberg;
