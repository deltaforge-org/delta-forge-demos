-- ============================================================================
-- Demo: Book Catalog Auto-Onboarding - Queries
-- ============================================================================
-- The landing folder holds one XML file:
--   book_catalog.xml - 25 books as repeating <book> elements under <catalog>
-- DISCOVER classifies the bytes as XML, picks the repeating <book> element as
-- the row anchor, synthesizes the leaf-element columns (catalog_book_isbn, catalog_book_title, catalog_book_author,
-- catalog_book_genre, catalog_book_price, catalog_book_year, catalog_book_in_stock), and registers ONE external table. Every
-- assertion value below was precomputed from the data file, not the engine.
-- XML leaf values are all strings, so the asserts use string equality and
-- integer COUNT only (never a raw float catalog_book_price comparison).
-- ============================================================================

-- ============================================================================
-- Query 1: DISCOVER in PRINT mode - review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the landed file,
-- classifies the text shape as XML, picks the repeating <book> element as the
-- row anchor, and synthesizes the xml_flatten_config. FILE_METADATA adds the
-- df_file_name / df_row_number provenance columns to the generated table.

DISCOVER {{zone_name}}.discover_demos.book_catalog
    PATH 'discover-xml-book-catalog'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode - auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). The
-- evidence column records the XML text-shape classification and the chosen
-- row anchor. This single statement replaces a hand-written CREATE EXTERNAL
-- TABLE + xml_flatten_config block.

DISCOVER {{zone_name}}.discover_demos.book_catalog
    PATH 'discover-xml-book-catalog'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: The full catalog - all 25 books
-- ============================================================================
-- One table over the single XML file. The discovered schema is the set of
-- leaf elements under the repeating <book> row anchor.

ASSERT ROW_COUNT = 25
SELECT
    catalog_book_isbn,
    catalog_book_title,
    catalog_book_author,
    catalog_book_genre,
    df_file_name
FROM {{zone_name}}.discover_demos.book_catalog;

-- ============================================================================
-- Query 4: Catalog mix - books per catalog_book_genre
-- ============================================================================
-- Proves every <book> element decoded correctly. Counts are exact and
-- precomputed from the data file.

ASSERT ROW_COUNT = 5
ASSERT VALUE book_count = 7 WHERE catalog_book_genre = 'Fiction'
ASSERT VALUE book_count = 6 WHERE catalog_book_genre = 'Mystery'
ASSERT VALUE book_count = 5 WHERE catalog_book_genre = 'Science Fiction'
ASSERT VALUE book_count = 4 WHERE catalog_book_genre = 'Biography'
ASSERT VALUE book_count = 3 WHERE catalog_book_genre = 'History'
SELECT
    catalog_book_genre,
    COUNT(*) AS book_count
FROM {{zone_name}}.discover_demos.book_catalog
GROUP BY catalog_book_genre
ORDER BY book_count DESC, catalog_book_genre;

-- ============================================================================
-- Query 5: Known book lookup - catalog_book_author by ISBN
-- ============================================================================
-- A point lookup on a specific ISBN proves the leaf elements landed in the
-- right columns. ISBN, catalog_book_title, catalog_book_author, and catalog_book_genre are all asserted as exact
-- strings (XML values are Utf8).

ASSERT ROW_COUNT = 1
ASSERT VALUE catalog_book_author = 'Arthur Conway' WHERE catalog_book_isbn = '978-1-08056-104-8'
ASSERT VALUE catalog_book_title = 'Mystery Volume 1' WHERE catalog_book_isbn = '978-1-08056-104-8'
ASSERT VALUE catalog_book_genre = 'Mystery' WHERE catalog_book_isbn = '978-1-08056-104-8'
SELECT
    catalog_book_isbn,
    catalog_book_title,
    catalog_book_author,
    catalog_book_genre
FROM {{zone_name}}.discover_demos.book_catalog
WHERE catalog_book_isbn = '978-1-08056-104-8';

-- ============================================================================
-- Query 6: Stock split - books in and out of stock
-- ============================================================================
-- catalog_book_in_stock is a string field ('true' / 'false'). Asserting COUNT per string
-- value keeps this deterministic and type-safe across the inferred Utf8 type.

ASSERT ROW_COUNT = 2
ASSERT VALUE book_count = 17 WHERE catalog_book_in_stock = 'true'
ASSERT VALUE book_count = 8 WHERE catalog_book_in_stock = 'false'
SELECT
    catalog_book_in_stock,
    COUNT(*) AS book_count
FROM {{zone_name}}.discover_demos.book_catalog
GROUP BY catalog_book_in_stock
ORDER BY book_count DESC;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total catalog volume, the
-- number of distinct genres and authors, and the single-file provenance.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_books = 25
ASSERT VALUE distinct_genres = 5
ASSERT VALUE distinct_authors = 5
ASSERT VALUE distinct_source_files = 1
SELECT
    COUNT(*)                        AS total_books,
    COUNT(DISTINCT catalog_book_genre)           AS distinct_genres,
    COUNT(DISTINCT catalog_book_author)          AS distinct_authors,
    COUNT(DISTINCT df_file_name)    AS distinct_source_files
FROM {{zone_name}}.discover_demos.book_catalog;
