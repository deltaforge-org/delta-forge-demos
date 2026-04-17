-- ============================================================================
-- Iceberg UniForm Complex Types — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH COMPLEX TYPES
-- -------------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- Delta complex types map to Iceberg types as follows:
--   - STRUCT → Iceberg struct (named fields with types)
--   - ARRAY  → Iceberg list (element type)
--   - MAP    → Iceberg map (key type, value type)
--
-- The column mapping mode 'id' ensures field IDs remain stable across
-- schema evolution, which is critical for Iceberg compatibility.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running, verify the schema in the Iceberg metadata:
--   python3 verify_iceberg_metadata.py <table_data_path>/product_catalog_nested -v
-- ============================================================================
-- ============================================================================
-- EXPLORE: Baseline — All 18 Products
-- ============================================================================

ASSERT ROW_COUNT = 18
SELECT * FROM {{zone_name}}.iceberg_demos.product_catalog_nested ORDER BY product_id;
-- ============================================================================
-- Query 1: Per-Category Summary
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE product_count = 6 WHERE category = 'Electronics'
ASSERT VALUE product_count = 6 WHERE category = 'Home'
ASSERT VALUE product_count = 6 WHERE category = 'Outdoor'
ASSERT VALUE total_price = 514.42 WHERE category = 'Electronics'
ASSERT VALUE total_price = 180.23 WHERE category = 'Home'
ASSERT VALUE total_price = 361.43 WHERE category = 'Outdoor'
SELECT
    category,
    COUNT(*) AS product_count,
    ROUND(SUM(price), 2) AS total_price,
    ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
GROUP BY category
ORDER BY category;
-- ============================================================================
-- Query 2: Stock Status
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_products = 18
ASSERT VALUE in_stock_count = 15
ASSERT VALUE out_of_stock_count = 3
SELECT
    COUNT(*) AS total_products,
    COUNT(*) FILTER (WHERE in_stock = true) AS in_stock_count,
    COUNT(*) FILTER (WHERE in_stock = false) AS out_of_stock_count
FROM {{zone_name}}.iceberg_demos.product_catalog_nested;
-- ============================================================================
-- Query 3: Struct Fields — Large Products (length > 50)
-- ============================================================================
-- Access struct fields using dot notation: dimensions.length

ASSERT ROW_COUNT = 5
SELECT
    product_id,
    product_name,
    category,
    dimensions.length AS length,
    dimensions.width AS width,
    dimensions.height AS height,
    dimensions.unit AS unit
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
WHERE dimensions.length > 50
ORDER BY dimensions.length DESC;
-- ============================================================================
-- Query 4: Struct Fields — Tall Products (height > 20)
-- ============================================================================

ASSERT ROW_COUNT = 6
SELECT
    product_id,
    product_name,
    category,
    dimensions.height AS height
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
WHERE dimensions.height > 20
ORDER BY dimensions.height DESC;
-- ============================================================================
-- Query 5: Array — Tag Count Per Product
-- ============================================================================
-- All seed products have exactly 3 tags each.

ASSERT ROW_COUNT = 18
SELECT
    product_id,
    product_name,
    ARRAY_LENGTH(tags) AS tag_count
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
ORDER BY product_id;
-- ============================================================================
-- Query 6: Array — Products With 'portable' Tag
-- ============================================================================

ASSERT ROW_COUNT = 4
SELECT
    product_id,
    product_name,
    category,
    tags
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
WHERE ARRAY_CONTAINS(tags, 'portable')
ORDER BY product_id;
-- ============================================================================
-- Query 7: Array — Products With 'eco-friendly' Tag
-- ============================================================================

ASSERT ROW_COUNT = 2
SELECT
    product_id,
    product_name,
    category,
    tags
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
WHERE ARRAY_CONTAINS(tags, 'eco-friendly')
ORDER BY product_id;
-- ============================================================================
-- Query 8: Map — French Product Names
-- ============================================================================

ASSERT ROW_COUNT = 18
ASSERT VALUE french_name = 'Souris Sans Fil' WHERE product_id = 1
ASSERT VALUE french_name = 'Clavier Mecanique' WHERE product_id = 2
ASSERT VALUE french_name = 'Tente de Camping' WHERE product_id = 13
ASSERT VALUE french_name = 'Hamac' WHERE product_id = 18
SELECT
    product_id,
    product_name,
    localized_names['fr'] AS french_name
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
ORDER BY product_id;
-- ============================================================================
-- Query 9: Map — German Product Names
-- ============================================================================

ASSERT ROW_COUNT = 18
ASSERT VALUE german_name = 'Kabellose Maus' WHERE product_id = 1
ASSERT VALUE german_name = 'Mechanische Tastatur' WHERE product_id = 2
ASSERT VALUE german_name = 'Campingzelt' WHERE product_id = 13
ASSERT VALUE german_name = 'Hangematte' WHERE product_id = 18
SELECT
    product_id,
    product_name,
    localized_names['de'] AS german_name
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
ORDER BY product_id;
-- ============================================================================
-- LEARN: INSERT — New Products With Complex Nested Values (Version 2)
-- ============================================================================
-- Add one product per category, each with full struct, array, and map values.

INSERT INTO {{zone_name}}.iceberg_demos.product_catalog_nested VALUES
    (19, 'Noise-Cancel Headphones', 'Electronics', 159.99,
        STRUCT(18.0, 17.0, 8.0, 'cm'),
        ARRAY('audio', 'noise-cancelling', 'wireless'),
        MAP('en', 'Noise-Cancel Headphones', 'fr', 'Casque Anti-Bruit', 'de', 'Gerauschunterdrueckende Kopfhoerer'),
        true),
    (20, 'Yoga Mat',                'Home',        35.00,
        STRUCT(183.0, 61.0, 0.6, 'cm'),
        ARRAY('fitness', 'yoga', 'non-slip'),
        MAP('en', 'Yoga Mat', 'fr', 'Tapis de Yoga', 'de', 'Yogamatte'),
        true),
    (21, 'Portable Grill',          'Outdoor',     88.50,
        STRUCT(45.0, 30.0, 35.0, 'cm'),
        ARRAY('cooking', 'portable', 'camping'),
        MAP('en', 'Portable Grill', 'fr', 'Grill Portable', 'de', 'Tragbarer Grill'),
        true);
-- ============================================================================
-- Query 10: Per-Category Counts After Insert
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE product_count = 7 WHERE category = 'Electronics'
ASSERT VALUE product_count = 7 WHERE category = 'Home'
ASSERT VALUE product_count = 7 WHERE category = 'Outdoor'
SELECT
    category,
    COUNT(*) AS product_count,
    ROUND(SUM(price), 2) AS total_price
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
GROUP BY category
ORDER BY category;
-- ============================================================================
-- LEARN: UPDATE — Adjust Outdoor Product Heights (Version 3)
-- ============================================================================
-- Add 2.0 to the height of all Outdoor products to account for updated
-- packaging dimensions. This modifies struct field values in-place.

UPDATE {{zone_name}}.iceberg_demos.product_catalog_nested
SET dimensions = STRUCT(dimensions.length, dimensions.width, dimensions.height + 2.0, dimensions.unit)
WHERE category = 'Outdoor';
-- ============================================================================
-- Query 11: Updated Outdoor Dimensions
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE height = 122.0 WHERE product_id = 13
ASSERT VALUE height = 27.0 WHERE product_id = 14
ASSERT VALUE height = 28.0 WHERE product_id = 15
ASSERT VALUE height = 82.0 WHERE product_id = 16
ASSERT VALUE height = 20.0 WHERE product_id = 17
ASSERT VALUE height = 5.0 WHERE product_id = 18
ASSERT VALUE height = 37.0 WHERE product_id = 21
SELECT
    product_id,
    product_name,
    dimensions.height AS height
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
WHERE category = 'Outdoor'
ORDER BY product_id;
-- ============================================================================
-- Query 12: Average Dimensions By Category
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_length = 24.07 WHERE category = 'Electronics'
ASSERT VALUE avg_length = 60.43 WHERE category = 'Home'
ASSERT VALUE avg_length = 98.21 WHERE category = 'Outdoor'
ASSERT VALUE avg_height = 6.73 WHERE category = 'Electronics'
ASSERT VALUE avg_height = 12.09 WHERE category = 'Home'
ASSERT VALUE avg_height = 45.86 WHERE category = 'Outdoor'
SELECT
    category,
    ROUND(AVG(dimensions.length), 2) AS avg_length,
    ROUND(AVG(dimensions.width), 2) AS avg_width,
    ROUND(AVG(dimensions.height), 2) AS avg_height
FROM {{zone_name}}.iceberg_demos.product_catalog_nested
GROUP BY category
ORDER BY category;
-- ============================================================================
-- Query 13: Time Travel — Original vs Current Price Totals
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_total = 1056.08
ASSERT VALUE v1_count = 18
ASSERT VALUE current_total = 1339.57
ASSERT VALUE current_count = 21
SELECT
    ROUND((SELECT SUM(price) FROM {{zone_name}}.iceberg_demos.product_catalog_nested VERSION AS OF 1), 2) AS v1_total,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.product_catalog_nested VERSION AS OF 1) AS v1_count,
    ROUND(SUM(price), 2) AS current_total,
    COUNT(*) AS current_count
FROM {{zone_name}}.iceberg_demos.product_catalog_nested;
-- ============================================================================
-- Query 14: Version History
-- ============================================================================
-- 3 versions: seed, insert new products, update outdoor heights.

ASSERT WARNING ROW_COUNT >= 3
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.product_catalog_nested;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check covering the full lifecycle of mutations.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_products = 21
ASSERT VALUE category_count = 3
ASSERT VALUE total_price = 1339.57
ASSERT VALUE avg_price = 63.79
ASSERT VALUE in_stock_count = 18
SELECT
    COUNT(*) AS total_products,
    COUNT(DISTINCT category) AS category_count,
    ROUND(SUM(price), 2) AS total_price,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(*) FILTER (WHERE in_stock = true) AS in_stock_count
FROM {{zone_name}}.iceberg_demos.product_catalog_nested;
-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents complex types (struct, array, map)
-- after INSERT with nested values and UPDATE of struct fields.
--
-- The Iceberg schema should show:
--   - dimensions: struct<length: double, width: double, height: double, unit: string>
--   - tags: list<string>
--   - localized_names: map<string, string>
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg
USING ICEBERG
LOCATION '{{data_path}}/product_catalog_nested';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Verify 1: Row Count — 21 Products After All Mutations
-- ============================================================================

ASSERT ROW_COUNT = 21
SELECT * FROM {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg ORDER BY product_id;
-- ============================================================================
-- Iceberg Verify 2: Per-Category Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE product_count = 7 WHERE category = 'Electronics'
ASSERT VALUE product_count = 7 WHERE category = 'Home'
ASSERT VALUE product_count = 7 WHERE category = 'Outdoor'
ASSERT VALUE total_price = 674.41 WHERE category = 'Electronics'
ASSERT VALUE total_price = 215.23 WHERE category = 'Home'
ASSERT VALUE total_price = 449.93 WHERE category = 'Outdoor'
SELECT
    category,
    COUNT(*) AS product_count,
    ROUND(SUM(price), 2) AS total_price
FROM {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg
GROUP BY category
ORDER BY category;
-- ============================================================================
-- Iceberg Verify 3: Struct Fields — Updated Outdoor Heights Via Iceberg
-- ============================================================================
-- The UPDATE added 2.0 to all Outdoor product heights. Verify Iceberg sees
-- the updated struct field values, proving nested type fidelity through UniForm.

ASSERT ROW_COUNT = 7
ASSERT VALUE height = 122.0 WHERE product_id = 13
ASSERT VALUE height = 5.0 WHERE product_id = 18
ASSERT VALUE height = 37.0 WHERE product_id = 21
SELECT
    product_id,
    product_name,
    dimensions.height AS height
FROM {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg
WHERE category = 'Outdoor'
ORDER BY product_id;
-- ============================================================================
-- Iceberg Verify 4: Map Access — French Names Via Iceberg
-- ============================================================================
-- Verify MAP values survive the Delta→Iceberg round-trip.

ASSERT ROW_COUNT = 21
ASSERT VALUE french_name = 'Souris Sans Fil' WHERE product_id = 1
ASSERT VALUE french_name = 'Casque Anti-Bruit' WHERE product_id = 19
ASSERT VALUE french_name = 'Tapis de Yoga' WHERE product_id = 20
ASSERT VALUE french_name = 'Grill Portable' WHERE product_id = 21
SELECT
    product_id,
    product_name,
    localized_names['fr'] AS french_name
FROM {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg
ORDER BY product_id;
-- ============================================================================
-- Iceberg Verify 5: Array — Portable Products Via Iceberg
-- ============================================================================
-- Verify ARRAY filtering works through the Iceberg metadata path.

ASSERT ROW_COUNT = 5
SELECT
    product_id,
    product_name,
    category,
    tags
FROM {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg
WHERE ARRAY_CONTAINS(tags, 'portable')
ORDER BY product_id;
-- ============================================================================
-- Iceberg Verify 6: Grand Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_products = 21
ASSERT VALUE total_price = 1339.57
ASSERT VALUE avg_price = 63.79
ASSERT VALUE category_count = 3
ASSERT VALUE in_stock_count = 18
SELECT
    COUNT(*) AS total_products,
    ROUND(SUM(price), 2) AS total_price,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(DISTINCT category) AS category_count,
    COUNT(*) FILTER (WHERE in_stock = true) AS in_stock_count
FROM {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg;
