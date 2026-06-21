-- ============================================================================
-- Demo: EU Point-of-Sale Onboarding: Queries
-- ============================================================================
-- The landing folder holds one semicolon-delimited CSV of 40 receipts:
--   pos_receipts_2026_q1.csv  has header row, ';' field separator, '.' decimals
-- DISCOVER sniffs the dialect from the bytes (delimiter ';', header present)
-- and registers ONE external table with the correct OPTIONS. A naive comma
-- reader would see a single column; DISCOVER does not. Every assertion value
-- below was precomputed from the data file, not the engine.
-- ============================================================================

-- ============================================================================
-- Query 1: DISCOVER in PRINT mode: review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the landed file,
-- sniffs the CSV dialect (it picks ';' as the field separator, not ',', and
-- recognises the header row), and emits the matching OPTIONS. FILE_METADATA
-- adds the df_file_name / df_row_number provenance columns to the generated
-- table. Inspect the printed DDL: the sniffed delimiter is ';'.

DISCOVER {{zone_name}}.discover_demos.pos_receipts
    PATH 'discover-csv-pos-semicolon'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode: auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). The
-- evidence column records the sniffed dialect: ';' delimiter with a header
-- row. This single statement replaces a hand-written CREATE EXTERNAL TABLE
-- with an explicit DELIMITER ';' option.

DISCOVER {{zone_name}}.discover_demos.pos_receipts
    PATH 'discover-csv-pos-semicolon'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: The receipts feed: all 40 rows decoded into 8 columns
-- ============================================================================
-- Because the ';' dialect was sniffed correctly, every receipt splits into its
-- eight fields. A comma reader would have produced one column and zero usable
-- rows. df_file_name confirms the single source file.

ASSERT ROW_COUNT = 40
SELECT
    receipt_id,
    store_id,
    city,
    product_name,
    quantity,
    unit_price_eur,
    total_eur,
    sale_date,
    df_file_name
FROM {{zone_name}}.discover_demos.pos_receipts;

-- ============================================================================
-- Query 4: Receipts per city: exact GROUP BY counts
-- ============================================================================
-- Proves the city column split out cleanly under the sniffed ';' dialect.
-- Counts are exact and precomputed from the data.

ASSERT ROW_COUNT = 4
ASSERT VALUE receipt_count = 14 WHERE city = 'Berlin'
ASSERT VALUE receipt_count = 11 WHERE city = 'Munich'
ASSERT VALUE receipt_count = 9 WHERE city = 'Hamburg'
ASSERT VALUE receipt_count = 6 WHERE city = 'Cologne'
SELECT
    city,
    COUNT(*) AS receipt_count
FROM {{zone_name}}.discover_demos.pos_receipts
GROUP BY city
ORDER BY receipt_count DESC, city;

-- ============================================================================
-- Query 5: Receipts per store: exact GROUP BY counts
-- ============================================================================
-- store_id is a distinct field from city; both decoded independently. Confirms
-- the field boundaries are exactly where the ';' separators are.

ASSERT ROW_COUNT = 4
ASSERT VALUE receipt_count = 14 WHERE store_id = 'S01'
ASSERT VALUE receipt_count = 11 WHERE store_id = 'S02'
ASSERT VALUE receipt_count = 9 WHERE store_id = 'S03'
ASSERT VALUE receipt_count = 6 WHERE store_id = 'S04'
SELECT
    store_id,
    COUNT(*) AS receipt_count
FROM {{zone_name}}.discover_demos.pos_receipts
GROUP BY store_id
ORDER BY receipt_count DESC, store_id;

-- ============================================================================
-- Query 6: Units sold: integer aggregate over the quantity column
-- ============================================================================
-- quantity was inferred as an integer (not swallowed into a text blob), so it
-- sums cleanly. The total and the per-store breakdown are exact.

ASSERT ROW_COUNT = 4
ASSERT VALUE units_sold = 33 WHERE store_id = 'S01'
ASSERT VALUE units_sold = 28 WHERE store_id = 'S02'
ASSERT VALUE units_sold = 22 WHERE store_id = 'S03'
ASSERT VALUE units_sold = 17 WHERE store_id = 'S04'
SELECT
    store_id,
    SUM(CAST(quantity AS BIGINT)) AS units_sold
FROM {{zone_name}}.discover_demos.pos_receipts
GROUP BY store_id
ORDER BY units_sold DESC, store_id;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total receipt volume, total
-- units sold, the distinct city and store cardinalities, and the single
-- source file. If the dialect had been mis-sniffed as comma, none of these
-- would hold (the table would have collapsed to one column).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_receipts = 40
ASSERT VALUE total_units = 100
ASSERT VALUE distinct_cities = 4
ASSERT VALUE distinct_stores = 4
ASSERT VALUE distinct_products = 5
ASSERT VALUE distinct_source_files = 1
SELECT
    COUNT(*)                        AS total_receipts,
    SUM(CAST(quantity AS BIGINT))                   AS total_units,
    COUNT(DISTINCT city)            AS distinct_cities,
    COUNT(DISTINCT store_id)        AS distinct_stores,
    COUNT(DISTINCT product_name)    AS distinct_products,
    COUNT(DISTINCT df_file_name)    AS distinct_source_files
FROM {{zone_name}}.discover_demos.pos_receipts;
