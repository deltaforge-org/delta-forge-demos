-- ============================================================================
-- XML Books Schema Evolution — Verification Queries
-- ============================================================================
-- Each query verifies that schema evolution works correctly:
--   - All 15 books from 5 files are read
--   - New columns added in later files appear as NULL in earlier files
--   - Dropped columns appear as NULL in files that lack them
--   - Attributes (@id, @format) are extracted as catalog_book_attr_id, catalog_book_attr_format
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ROW COUNT — All 5 files should produce 15 rows
-- ============================================================================
-- If the table reads all files: 15 rows (3 per file)
-- If only some files are read: fewer rows

ASSERT ROW_COUNT = 15
SELECT *
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 2. BROWSE ALL DATA — See the full union schema
-- ============================================================================

ASSERT VALUE catalog_book_author = 'Gambardella, Matthew' WHERE catalog_book_attr_id = 'bk101'
ASSERT VALUE catalog_book_title = 'XML Developer''s Guide' WHERE catalog_book_attr_id = 'bk101'
ASSERT VALUE catalog_book_genre = 'Computer' WHERE catalog_book_attr_id = 'bk101'
ASSERT VALUE catalog_book_price = '44.95' WHERE catalog_book_attr_id = 'bk101'
ASSERT VALUE catalog_book_isbn IS NULL WHERE catalog_book_attr_id = 'bk101'
ASSERT VALUE catalog_book_author = 'Galos, Mike' WHERE catalog_book_attr_id = 'bk114'
ASSERT VALUE catalog_book_title = 'Visual Studio 7: A Comprehensive Guide' WHERE catalog_book_attr_id = 'bk114'
ASSERT VALUE catalog_book_attr_format = 'paperback' WHERE catalog_book_attr_id = 'bk114'
SELECT *
FROM {{zone_name}}.xml_demos.books_evolved
ORDER BY catalog_book_attr_id;


-- ============================================================================
-- 3. ISBN COLUMN (added in file 2) — 3 NULLs from file 1
-- ============================================================================
-- File 1 (bk101-bk103) lacks isbn → NULL
-- Files 2-5 (bk104-bk115) have isbn → NOT NULL

ASSERT VALUE isbn_null_count = 3
SELECT COUNT(*) FILTER (WHERE catalog_book_isbn IS NULL) AS isbn_null_count
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 4. LANGUAGE COLUMN (added in file 2) — 3 NULLs from file 1
-- ============================================================================

ASSERT VALUE language_null_count = 3
SELECT COUNT(*) FILTER (WHERE catalog_book_language IS NULL) AS language_null_count
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 5. PUBLISHER COLUMN (added in file 3) — 6 NULLs from files 1-2
-- ============================================================================

ASSERT VALUE publisher_null_count = 6
SELECT COUNT(*) FILTER (WHERE catalog_book_publisher IS NULL) AS publisher_null_count
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 6. RATING COLUMN (added in file 3) — 6 NULLs from files 1-2
-- ============================================================================

ASSERT VALUE rating_null_count = 6
SELECT COUNT(*) FILTER (WHERE catalog_book_rating IS NULL) AS rating_null_count
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 7. DESCRIPTION COLUMN (dropped in file 4) — 6 NULLs from files 4-5
-- ============================================================================
-- Files 1-3 (bk101-bk109) have description
-- Files 4-5 (bk110-bk115) dropped description → NULL

ASSERT VALUE description_null_count = 6
SELECT COUNT(*) FILTER (WHERE catalog_book_description IS NULL) AS description_null_count
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 8. EDITION COLUMN (added in file 4) — 9 NULLs from files 1-3
-- ============================================================================

ASSERT VALUE edition_null_count = 9
SELECT COUNT(*) FILTER (WHERE catalog_book_edition IS NULL) AS edition_null_count
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 9. PAGES COLUMN (added in file 4) — 9 NULLs from files 1-3
-- ============================================================================

ASSERT VALUE pages_null_count = 9
SELECT COUNT(*) FILTER (WHERE catalog_book_pages IS NULL) AS pages_null_count
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 10. SERIES COLUMN (added in file 5) — 12 NULLs from files 1-4
-- ============================================================================

ASSERT VALUE series_null_count = 12
SELECT COUNT(*) FILTER (WHERE catalog_book_series IS NULL) AS series_null_count
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 11. FORMAT ATTRIBUTE (added in file 5) — 12 NULLs from files 1-4
-- ============================================================================
-- The @format attribute on <book> is extracted as column catalog_book_attr_format.

ASSERT VALUE attr_format_null_count = 12
SELECT COUNT(*) FILTER (WHERE catalog_book_attr_format IS NULL) AS attr_format_null_count
FROM {{zone_name}}.xml_demos.books_evolved;


-- ============================================================================
-- 12. VALUE SPOT-CHECK — Verify specific books have correct data
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE catalog_book_author = 'O''Brien, Tim' WHERE catalog_book_attr_id = 'bk108'
ASSERT VALUE catalog_book_title = 'The .NET Programming Bible' WHERE catalog_book_attr_id = 'bk108'
ASSERT VALUE catalog_book_price = '36.95' WHERE catalog_book_attr_id = 'bk108'
ASSERT VALUE catalog_book_rating = '4.8' WHERE catalog_book_attr_id = 'bk108'
ASSERT VALUE catalog_book_author = 'Corets, Eva' WHERE catalog_book_attr_id = 'bk113'
ASSERT VALUE catalog_book_title = 'Dragon''s Keep' WHERE catalog_book_attr_id = 'bk113'
ASSERT VALUE catalog_book_price = '6.95' WHERE catalog_book_attr_id = 'bk113'
ASSERT VALUE catalog_book_attr_format = 'hardcover' WHERE catalog_book_attr_id = 'bk113'
ASSERT VALUE catalog_book_series = 'Maeve Saga' WHERE catalog_book_attr_id = 'bk113'
SELECT catalog_book_attr_id, catalog_book_author, catalog_book_title, catalog_book_genre, catalog_book_price,
       catalog_book_rating, catalog_book_attr_format, catalog_book_series
FROM {{zone_name}}.xml_demos.books_evolved
WHERE catalog_book_attr_id IN ('bk101', 'bk108', 'bk113')
ORDER BY catalog_book_attr_id;


-- ============================================================================
-- 13. GENRE DISTRIBUTION — All 15 books should have a genre
-- ============================================================================
-- Expected: Fantasy=5, Computer=4, Science Fiction=2, Romance=2, Horror=2

ASSERT ROW_COUNT = 5
ASSERT VALUE book_count = 5 WHERE catalog_book_genre = 'Fantasy'
ASSERT VALUE book_count = 4 WHERE catalog_book_genre = 'Computer'
ASSERT VALUE book_count = 2 WHERE catalog_book_genre = 'Science Fiction'
ASSERT VALUE book_count = 2 WHERE catalog_book_genre = 'Romance'
ASSERT VALUE book_count = 2 WHERE catalog_book_genre = 'Horror'
SELECT catalog_book_genre, COUNT(*) AS book_count
FROM {{zone_name}}.xml_demos.books_evolved
GROUP BY catalog_book_genre
ORDER BY book_count DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: all 15 books present, and every schema evolution
-- invariant (NULL counts by wave) confirmed in a single query.

ASSERT ROW_COUNT = 10
SELECT 'total_rows' AS check_name, CASE WHEN COUNT(*) = 15 THEN 'PASS' ELSE 'FAIL' END AS result FROM {{zone_name}}.xml_demos.books_evolved
UNION ALL
SELECT 'isbn_nulls', CASE WHEN COUNT(*) FILTER (WHERE catalog_book_isbn IS NULL) = 3 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml_demos.books_evolved
UNION ALL
SELECT 'language_nulls', CASE WHEN COUNT(*) FILTER (WHERE catalog_book_language IS NULL) = 3 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml_demos.books_evolved
UNION ALL
SELECT 'publisher_nulls', CASE WHEN COUNT(*) FILTER (WHERE catalog_book_publisher IS NULL) = 6 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml_demos.books_evolved
UNION ALL
SELECT 'rating_nulls', CASE WHEN COUNT(*) FILTER (WHERE catalog_book_rating IS NULL) = 6 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml_demos.books_evolved
UNION ALL
SELECT 'description_nulls', CASE WHEN COUNT(*) FILTER (WHERE catalog_book_description IS NULL) = 6 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml_demos.books_evolved
UNION ALL
SELECT 'edition_nulls', CASE WHEN COUNT(*) FILTER (WHERE catalog_book_edition IS NULL) = 9 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml_demos.books_evolved
UNION ALL
SELECT 'pages_nulls', CASE WHEN COUNT(*) FILTER (WHERE catalog_book_pages IS NULL) = 9 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml_demos.books_evolved
UNION ALL
SELECT 'series_nulls', CASE WHEN COUNT(*) FILTER (WHERE catalog_book_series IS NULL) = 12 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml_demos.books_evolved
UNION ALL
SELECT 'attr_format_nulls', CASE WHEN COUNT(*) FILTER (WHERE catalog_book_attr_format IS NULL) = 12 THEN 'PASS' ELSE 'FAIL' END FROM {{zone_name}}.xml_demos.books_evolved
ORDER BY check_name;
