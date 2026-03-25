-- ============================================================================
-- Delta Unicode Roundtrip — Educational Queries
-- ============================================================================
-- WHAT: Every DML operation (INSERT, UPDATE, DELETE) on a Delta table must
--       preserve multi-byte UTF-8 characters exactly — no silent corruption,
--       no mojibake, no byte truncation at character boundaries.
-- WHY:  GlobalBazaar is an international marketplace where sellers list products
--       in their native script. A corrupted product name means lost sales and
--       broken search. The storage layer must be invisible to Unicode.
-- HOW:  Delta's copy-on-write mechanism rewrites entire Parquet row groups on
--       UPDATE/DELETE. This demo asserts exact Unicode values after every
--       mutation to prove the rewrite preserves multi-byte sequences.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Multi-script product names side by side
-- ============================================================================
-- Eight products spanning Japanese, Chinese, Cyrillic, Greek, Hebrew, Persian,
-- Ukrainian, and Turkish. All stored in the same VARCHAR column.

ASSERT ROW_COUNT = 8
SELECT id, product_name, product_name_local, country, region
FROM {{zone_name}}.delta_demos.global_bazaar
WHERE id IN (1, 3, 13, 14, 22, 25, 18, 23)
ORDER BY region, id;


-- ============================================================================
-- EXPLORE: Region distribution — all 3 regions populated
-- ============================================================================
-- Each region has 10 products. The partition key is ASCII but the data within
-- each partition contains full Unicode strings across multiple scripts.

ASSERT ROW_COUNT = 3
SELECT region, COUNT(*) AS products,
       COUNT(DISTINCT country) AS countries
FROM {{zone_name}}.delta_demos.global_bazaar
GROUP BY region
ORDER BY region;


-- ============================================================================
-- LEARN: Write/read roundtrip — verify exact Unicode after INSERT
-- ============================================================================
-- After INSERT, read back specific rows and assert the exact Unicode string.
-- This proves Parquet's UTF-8 encoding preserved every byte through the
-- write/read cycle. We test CJK (3 bytes/char), Cyrillic (2 bytes/char),
-- and Hebrew (2 bytes/char).

-- Japanese kanji + katakana preserved
ASSERT VALUE product_name_local = '抹茶セレモニーセット'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 1;

-- Cyrillic preserved (Russian)
ASSERT VALUE product_name_local = 'Матрёшка'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 13;

-- Hebrew preserved (right-to-left)
ASSERT VALUE product_name_local = 'מלח ים המוות'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 22;

ASSERT ROW_COUNT = 10
SELECT id, product_name, product_name_local, category, country
FROM {{zone_name}}.delta_demos.global_bazaar
WHERE region = 'Asia'
ORDER BY id;


-- ============================================================================
-- MUTATE: UPDATE Unicode columns directly — rename products in local script
-- ============================================================================
-- This is the key test no other demo covers. We UPDATE the product_name_local
-- column itself — not just a numeric column in the same row group. Delta must
-- rewrite the Parquet file with the new multi-byte value without corrupting
-- neighboring Unicode strings in the same row group.

-- Rename Chinese calligraphy set (id=6): '书法毛笔套装' → '毛笔书法工具'
ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.global_bazaar
SET product_name_local = '毛笔书法工具'
WHERE id = 6;

-- Expand Russian doll name (id=13): 'Матрёшка' → 'Русская Матрёшка'
ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.global_bazaar
SET product_name_local = 'Русская Матрёшка'
WHERE id = 13;


-- ============================================================================
-- LEARN: Verify updated Unicode values survived copy-on-write
-- ============================================================================
-- Delta rewrote the Parquet files containing ids 6 and 13. The new Unicode
-- strings must be byte-perfect, and all OTHER rows in the same row groups
-- must also be preserved exactly.

-- Verify Chinese update
ASSERT VALUE product_name_local = '毛笔书法工具'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 6;

-- Verify Cyrillic expansion
ASSERT VALUE product_name_local = 'Русская Матрёшка'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 13;


-- ============================================================================
-- MUTATE: DELETE with CJK predicate — Unicode in WHERE clause
-- ============================================================================
-- Delete products whose local name ends with '蒸笼' (steamer). Only id=3
-- ('点心蒸笼') matches. This tests that the engine correctly evaluates LIKE
-- patterns containing multi-byte characters, not just ASCII wildcards.

ASSERT ROW_COUNT = 1
DELETE FROM {{zone_name}}.delta_demos.global_bazaar
WHERE product_name_local LIKE '%蒸笼';


-- ============================================================================
-- MUTATE: 15% price increase for Europe region
-- ============================================================================
-- Delta rewrites ALL Europe partition Parquet files. Every diacritic character
-- (ü, é, î, ě, ň, ý, æ, ö, ó, å) in the same row groups must survive the
-- copy-on-write rewrite unchanged.

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.global_bazaar
SET price = ROUND(price * 1.15, 2)
WHERE region = 'Europe';


-- ============================================================================
-- LEARN: Verify diacritics and Cyrillic after price UPDATE
-- ============================================================================
-- The price UPDATE rewrote every Europe row group. Assert that Latin diacritics
-- (French), Czech háčky/čárky, and Cyrillic (Ukrainian) survived intact.

-- French diacritics: è, î, ê
ASSERT VALUE product_name_local = 'Crème fraîche starter'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 12;

-- Czech háčky and čárky: ž, ň, ý
ASSERT VALUE product_name_local = 'Plzeňský kvasinkový kmen'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 15;

-- Ukrainian Cyrillic
ASSERT VALUE product_name_local = 'Український мед'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 18;

-- Price updated correctly (id=14: 11.00 * 1.15 = 12.65)
ASSERT VALUE price = 12.65
SELECT price FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 14;


-- ============================================================================
-- MUTATE: DELETE MENA products with price < 15
-- ============================================================================
-- Removes 3 MENA products (ids 24, 27, 29) with Arabic local names.
-- The remaining Arabic, Hebrew, and Turkish strings must survive.

ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.delta_demos.global_bazaar
WHERE region = 'MENA' AND price < 15;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count: 30 - 1 (CJK delete) - 3 (MENA delete) = 26
ASSERT ROW_COUNT = 26
SELECT * FROM {{zone_name}}.delta_demos.global_bazaar;

-- Verify Asia has 9 products (1 deleted)
ASSERT VALUE asia_count = 9
SELECT COUNT(*) AS asia_count FROM {{zone_name}}.delta_demos.global_bazaar WHERE region = 'Asia';

-- Verify Europe has 10 products (none deleted)
ASSERT VALUE europe_count = 10
SELECT COUNT(*) AS europe_count FROM {{zone_name}}.delta_demos.global_bazaar WHERE region = 'Europe';

-- Verify MENA has 7 products (3 deleted)
ASSERT VALUE mena_count = 7
SELECT COUNT(*) AS mena_count FROM {{zone_name}}.delta_demos.global_bazaar WHERE region = 'MENA';

-- Verify 3 distinct regions still exist
ASSERT VALUE region_count = 3
SELECT COUNT(DISTINCT region) AS region_count FROM {{zone_name}}.delta_demos.global_bazaar;

-- Verify Japanese kanji preserved through all mutations
ASSERT VALUE product_name_local = '抹茶セレモニーセット'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 1;

-- Verify Persian Arabic-script preserved after MENA deletes
ASSERT VALUE product_name_local = 'زعفران نخ ایرانی'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 25;

-- Verify updated Cyrillic value persists
ASSERT VALUE product_name_local = 'Русская Матрёшка'
SELECT product_name_local FROM {{zone_name}}.delta_demos.global_bazaar WHERE id = 13;
