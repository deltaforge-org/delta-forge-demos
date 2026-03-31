-- ============================================================================
-- Political Books — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- ============================================================================

-- STEP 1: Drop graph definition (also cascade-deletes table mappings)
DROP GRAPH IF EXISTS {{zone_name}}.political_books.political_books;

-- STEP 2: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.political_books.vertices WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.political_books.edges WITH FILES;

-- STEP 3: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.polbooks_edges WITH FILES;

-- STEP 4: Drop schemas and zone
DROP SCHEMA IF EXISTS {{zone_name}}.political_books;
DROP SCHEMA IF EXISTS {{zone_name}}.raw;
DROP ZONE IF EXISTS {{zone_name}};
