-- ============================================================================
-- String Statistics — Truncation & Bloom Filter Bridge — Educational Queries
-- ============================================================================
-- WHAT: Delta stores min/max statistics for string columns, but only the
--       first 32 characters. Strings longer than 32 chars get truncated stats.
-- WHY:  Short, distinct strings (like SKU codes) get full statistics and
--       efficient data skipping. Long strings with common prefixes (like URLs)
--       share the same truncated min/max, making stats useless for filtering.
-- HOW:  The 32-char truncation is a Delta protocol design choice to balance
--       storage overhead vs. filtering effectiveness. For long-string point
--       lookups, Bloom filters provide an alternative probabilistic index.
-- ============================================================================


-- ============================================================================
-- Query 1: String Length Analysis Per Column
-- ============================================================================
-- Shows how different columns fall relative to the 32-char truncation boundary.
-- SKUs and names are well within limits; descriptions and URLs exceed them.

ASSERT VALUE min_len = 8 WHERE col_name = 'sku'
ASSERT VALUE max_len = 8 WHERE col_name = 'sku'
ASSERT VALUE over_32 = 0 WHERE col_name = 'sku'
ASSERT VALUE over_32 = 0 WHERE col_name = 'name'
ASSERT VALUE over_32 = 20 WHERE col_name = 'description'
ASSERT VALUE over_32 = 20 WHERE col_name = 'url'
ASSERT ROW_COUNT = 4
SELECT 'sku' AS col_name,
       MIN(LENGTH(sku)) AS min_len,
       MAX(LENGTH(sku)) AS max_len,
       COUNT(*) FILTER (WHERE LENGTH(sku) > 32) AS over_32
FROM {{zone_name}}.string_demos.product_catalog
UNION ALL
SELECT 'name',
       MIN(LENGTH(name)),
       MAX(LENGTH(name)),
       COUNT(*) FILTER (WHERE LENGTH(name) > 32)
FROM {{zone_name}}.string_demos.product_catalog
UNION ALL
SELECT 'description',
       MIN(LENGTH(description)),
       MAX(LENGTH(description)),
       COUNT(*) FILTER (WHERE LENGTH(description) > 32)
FROM {{zone_name}}.string_demos.product_catalog
UNION ALL
SELECT 'url',
       MIN(LENGTH(url)),
       MAX(LENGTH(url)),
       COUNT(*) FILTER (WHERE LENGTH(url) > 32)
FROM {{zone_name}}.string_demos.product_catalog
ORDER BY col_name;


-- ============================================================================
-- Query 2: SKU Filtering — Full Statistics (All Under 32 Chars)
-- ============================================================================
-- All SKU codes are exactly 8 characters. The engine stores the complete
-- min ('SKU-A001') and max ('SKU-G002') in file statistics. A filter like
-- WHERE sku = 'SKU-A001' can use these stats to determine whether a file
-- could contain the target value before reading any Parquet data.

ASSERT VALUE sku_count = 3
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS sku_count
FROM {{zone_name}}.string_demos.product_catalog
WHERE sku LIKE 'SKU-A%';


-- ============================================================================
-- Query 3: URL Prefix Problem — Truncation Defeats Skipping
-- ============================================================================
-- All 20 URLs share the same 35-character prefix:
--   'https://store.example.com/products/'
--
-- Since Delta only stores the first 32 characters, the truncated stats are:
--   min = 'https://store.example.com/produc'
--   max = 'https://store.example.com/produc'
--
-- Every URL truncates to the SAME 32-char prefix. The engine cannot
-- distinguish between URLs using min/max stats alone, so it must read
-- every file — no skipping is possible.

ASSERT VALUE distinct_url_prefixes = 1
ASSERT VALUE common_prefix_length = 35
ASSERT ROW_COUNT = 1
SELECT COUNT(DISTINCT SUBSTRING(url, 1, 32)) AS distinct_url_prefixes,
       LENGTH('https://store.example.com/products/') AS common_prefix_length
FROM {{zone_name}}.string_demos.product_catalog;


-- ============================================================================
-- Query 4: Category Breakdown — Short Strings With Good Stats
-- ============================================================================
-- Category names are 4-12 characters — well within the 32-char limit.
-- The engine can use category min/max stats effectively for filtering.

ASSERT VALUE product_count = 6 WHERE category = 'accessories'
ASSERT VALUE product_count = 3 WHERE category = 'audio'
ASSERT VALUE product_count = 6 WHERE category = 'furniture'
ASSERT VALUE product_count = 5 WHERE category = 'peripherals'
ASSERT ROW_COUNT = 4
SELECT category,
       COUNT(*) AS product_count,
       ROUND(AVG(price), 2) AS avg_price,
       MIN(price) AS min_price,
       MAX(price) AS max_price
FROM {{zone_name}}.string_demos.product_catalog
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 5: Exact URL Lookup — Where Bloom Filters Would Help
-- ============================================================================
-- An exact URL lookup cannot benefit from min/max string stats because all
-- URLs share the same truncated prefix. The engine must scan every file.
-- A Bloom filter index on the url column would enable probabilistic file
-- skipping: check if a file MIGHT contain the target URL before reading.

ASSERT VALUE found_id = 10
ASSERT VALUE found_sku = 'SKU-D001'
ASSERT ROW_COUNT = 1
SELECT id AS found_id, sku AS found_sku, name
FROM {{zone_name}}.string_demos.product_catalog
WHERE url = 'https://store.example.com/products/peripherals/docking-station-thunderbolt4-d001';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 20
ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.string_demos.product_catalog;

-- Verify all SKUs are exactly 8 characters
ASSERT VALUE all_8_chars = 20
SELECT COUNT(*) AS all_8_chars FROM {{zone_name}}.string_demos.product_catalog WHERE LENGTH(sku) = 8;

-- Verify all URLs exceed 32 characters
ASSERT VALUE all_over_32 = 20
SELECT COUNT(*) AS all_over_32 FROM {{zone_name}}.string_demos.product_catalog WHERE LENGTH(url) > 32;

-- Verify all descriptions exceed 32 characters
ASSERT VALUE desc_over_32 = 20
SELECT COUNT(*) AS desc_over_32 FROM {{zone_name}}.string_demos.product_catalog WHERE LENGTH(description) > 32;

-- Verify total catalog value
ASSERT VALUE total_value = 2315.8
SELECT ROUND(SUM(price), 2) AS total_value FROM {{zone_name}}.string_demos.product_catalog;

-- Verify peripherals count
ASSERT VALUE periph_count = 5
SELECT COUNT(*) AS periph_count FROM {{zone_name}}.string_demos.product_catalog WHERE category = 'peripherals';
