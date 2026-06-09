-- ============================================================================
-- Manual CSR Cache Management — Setup Script
-- ============================================================================
-- Loads the Zachary Karate Club dataset and creates a graph with automatic
-- CSR disk caching DISABLED (NO AUTO CACHE CSR). This forces every Cypher
-- query to rebuild the graph from Delta tables until the operator manually
-- runs CREATE GRAPHCSR.
--
-- Vertices: 34 club members (IDs 0-33)
-- Edges: 156 rows (78 undirected edges stored bidirectionally, weight=1.0)
-- ============================================================================

-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.karate_manual_raw
    COMMENT 'Manual CSR Karate — external CSV staging tables (pipe-delimited)';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.karate_manual
    COMMENT 'Karate Club — Delta tables, graph with NO AUTO CACHE CSR, and manual CSR management queries';

-- ############################################################################
-- STEP 2: External Tables — Raw CSV Readers (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.karate_manual_raw.karate_edges
USING CSV LOCATION 'graph-manual-csr/edges.csv'
OPTIONS (header = 'true', delimiter = '|');


CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.karate_manual_raw.karate_vertices
USING CSV LOCATION 'graph-manual-csr/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');


-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.karate_manual.edges
LOCATION 'graph-manual-csr/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.karate_manual_raw.karate_edges;


-- === Vertex Table ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.karate_manual.vertices
LOCATION 'graph-manual-csr/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS role
FROM {{zone_name}}.karate_manual_raw.karate_vertices;


-- ############################################################################
-- STEP 3b: Physical Layout — Z-ORDER for fast data skipping
-- ############################################################################
-- The data was loaded in vertex_id order, which has reasonable locality for
-- `vertex_id` but scatters the frequent filter column (role) across files.
-- Z-ORDER rewrites files so rows with similar values on the ordering keys
-- co-locate, giving Parquet min/max statistics much tighter ranges per file.
-- This benefits three hot paths:
--
--   1. Manual CSR rebuilds — this graph has NO AUTO CACHE CSR, so every
--      explicit `CREATE GRAPHCSR` reads the edge table; `(src, dst)` ordering
--      keeps that I/O sequential.
--   2. Reverse-index lookups — `vertex_id` co-location lets the Parquet
--      reader skip almost every row group for targeted member lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE v.role = 'Mr. Hi'` skip entire files instead of reading the
--      whole vertex table.
--
-- One-time cost at setup; every subsequent manual CSR rebuild benefits.

OPTIMIZE {{zone_name}}.karate_manual.vertices
    ZORDER BY (vertex_id, role);

OPTIMIZE {{zone_name}}.karate_manual.edges
    ZORDER BY (src, dst);

-- ############################################################################
-- STEP 4: Graph Definition — NO AUTO CACHE CSR
-- ############################################################################
-- Creates a named graph with automatic CSR disk caching DISABLED.
-- Without this flag, the engine would auto-write a .dcsr file after every
-- graph rebuild. With NO AUTO CACHE CSR, the operator must explicitly run
-- CREATE GRAPHCSR to populate or refresh the disk cache.

CREATE GRAPH IF NOT EXISTS {{zone_name}}.karate_manual.karate_manual
    VERTEX TABLE {{zone_name}}.karate_manual.vertices ID COLUMN vertex_id
        NODE TYPE COLUMN role NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.karate_manual.edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN edge_type
    NO AUTO CACHE CSR
    DIRECTED;
