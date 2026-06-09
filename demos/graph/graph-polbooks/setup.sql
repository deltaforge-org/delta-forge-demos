-- ============================================================================
-- Political Books — Setup Script
-- ============================================================================
-- Loads the Political Books co-purchasing dataset (Krebs / Newman) into
-- Delta tables and creates a named graph for algorithm verification.
--
-- Data source: V. Krebs, unpublished; compiled by M. E. J. Newman
-- Format: pipe-delimited CSV with header (src|dst|weight|edge_type)
--
-- Vertices: 105 political books (IDs 0–104)
-- Edges: 882 rows (441 undirected edges stored bidirectionally, weight=1.0)
--
-- Graph:
--   {{zone_name}}.political_books.political_books — All books as vertices, co-purchases as edges
-- ============================================================================
-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.polbooks_raw
    COMMENT 'Political Books — external CSV staging tables';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.political_books
    COMMENT 'Political Books — Delta tables, co-purchasing graph with ground-truth communities, and algorithm queries';
-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.polbooks_raw.polbooks_edges
USING CSV LOCATION 'graph-polbooks/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.political_books.edges
LOCATION 'graph-polbooks/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.polbooks_raw.polbooks_edges;

-- === Vertex Table (from CSV with book titles and political leanings) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.polbooks_raw.polbooks_vertices
USING CSV LOCATION 'graph-polbooks/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.political_books.vertices
LOCATION 'graph-polbooks/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS leaning
FROM {{zone_name}}.polbooks_raw.polbooks_vertices;

-- ############################################################################
-- STEP 3b: Physical Layout — Z-ORDER for fast data skipping
-- ############################################################################
-- The data was loaded in vertex_id order, which has reasonable locality for
-- `vertex_id` but scatters the frequent filter column (leaning) — the main
-- community structure — across files.  Z-ORDER rewrites files so rows with
-- similar values on the ordering keys co-locate, giving Parquet min/max
-- statistics much tighter ranges per file.  This benefits three hot paths:
--
--   1. CSR build from the edges table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `vertex_id` co-location lets the Parquet
--      reader skip almost every row group for targeted book lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE v.leaning = 'liberal'` skip entire files instead of reading
--      the whole vertex table.
--
-- One-time cost at setup; every subsequent query benefits.

OPTIMIZE {{zone_name}}.political_books.vertices
    ZORDER BY (vertex_id, leaning);

OPTIMIZE {{zone_name}}.political_books.edges
    ZORDER BY (src, dst);

-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling book vertices with co-purchasing edges.
-- Cypher queries reference this by name: USE {{zone_name}}.political_books.political_books MATCH ...

CREATE GRAPH IF NOT EXISTS {{zone_name}}.political_books.political_books
    VERTEX TABLE {{zone_name}}.political_books.vertices ID COLUMN vertex_id NODE NAME COLUMN name NODE TYPE COLUMN leaning
    EDGE TABLE {{zone_name}}.political_books.edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN edge_type
    DIRECTED;

-- ############################################################################
-- STEP 5: Warm the CSR cache
-- ############################################################################
-- CREATE GRAPHCSR pre-builds the Compressed Sparse Row topology and writes
-- it to disk as a .dcsr file. The first Cypher query then loads in ~200 ms
-- instead of rebuilding from Delta tables. Safe to re-run after bulk edge
-- loads to refresh the cache.

CREATE GRAPHCSR {{zone_name}}.political_books.political_books;
