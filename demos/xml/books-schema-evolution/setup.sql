-- ============================================================================
-- XML Books Schema Evolution — Setup Script
-- ============================================================================
-- Creates one external table that reads 5 XML files spanning 2000–2004.
-- Each file adds or removes elements, demonstrating schema evolution.
--
-- The xml_flatten_config pre-selects the union of all paths across all files.
-- The engine uses this config to generate the column definitions
-- (ConfigBasedStrategy), so no file I/O is needed at schema-discovery time.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.xml_demos
    COMMENT 'XML-backed external tables';

-- ============================================================================
-- TABLE: books_evolved — All 5 catalog files (schema evolution)
-- ============================================================================
-- The xml_flatten_config specifies:
--   row_xpath        — //book  (each <book> element becomes a row)
--   include_paths    — union of all leaf elements + attributes across 5 files
--   include_attributes — true (extract @id and @format as columns)
--   separator        — _ (nested paths join with underscore)
--
-- Column naming convention (from delta-forge-schema naming):
--   /catalog/book/@id    →  catalog_book_attr_id
--   /catalog/book/author →  catalog_book_author
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.xml_demos.books_evolved
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    xml_flatten_config = '{
        "row_xpath": "//book",
        "include_paths": [
            "/catalog/book/@id",
            "/catalog/book/@format",
            "/catalog/book/author",
            "/catalog/book/title",
            "/catalog/book/genre",
            "/catalog/book/price",
            "/catalog/book/publish_date",
            "/catalog/book/description",
            "/catalog/book/isbn",
            "/catalog/book/language",
            "/catalog/book/publisher",
            "/catalog/book/rating",
            "/catalog/book/edition",
            "/catalog/book/pages",
            "/catalog/book/series"
        ],
        "column_mappings": {
            "/catalog/book/@id": "catalog_book_attr_id",
            "/catalog/book/@format": "catalog_book_attr_format",
            "/catalog/book/author": "catalog_book_author",
            "/catalog/book/title": "catalog_book_title",
            "/catalog/book/genre": "catalog_book_genre",
            "/catalog/book/price": "catalog_book_price",
            "/catalog/book/publish_date": "catalog_book_publish_date",
            "/catalog/book/description": "catalog_book_description",
            "/catalog/book/isbn": "catalog_book_isbn",
            "/catalog/book/language": "catalog_book_language",
            "/catalog/book/publisher": "catalog_book_publisher",
            "/catalog/book/rating": "catalog_book_rating",
            "/catalog/book/edition": "catalog_book_edition",
            "/catalog/book/pages": "catalog_book_pages",
            "/catalog/book/series": "catalog_book_series"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "strip_namespace_prefixes": true
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
