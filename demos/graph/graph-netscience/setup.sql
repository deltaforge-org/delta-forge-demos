-- ============================================================================
-- NetScience Coauthorship Network — Setup Script
-- ============================================================================
-- Loads the NetScience coauthorship dataset (Newman, 2006) into Delta tables
-- and creates a named graph for algorithm verification.
--
-- Data source: M. E. J. Newman, network data repository
-- Format: pipe-delimited CSV with header (src|dst|weight|edge_type)
--
-- Vertices: 1,461 authors (IDs 0–1588, non-sequential with 128 gaps)
-- Edges: 5,484 rows (2,742 undirected edges stored bidirectionally,
--        non-uniform weights representing coauthorship strength)
--
-- Graph:
--   {{zone_name}}.netscience_collab.netscience_collab — Authors as vertices, coauthorships as edges
-- ============================================================================
-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.netscience_raw
    COMMENT 'NetScience — external CSV staging table';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.netscience_collab
    COMMENT 'NetScience coauthorship — Delta tables, weighted collaboration graph, and algorithm queries';
-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.netscience_raw.netscience_edges
USING CSV LOCATION 'graph-netscience/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.netscience_collab.edges
LOCATION 'graph-netscience/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.netscience_raw.netscience_edges;

-- === Vertex Table (from CSV with researcher names and roles) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.netscience_raw.netscience_vertices
USING CSV LOCATION 'graph-netscience/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.netscience_collab.vertices
LOCATION 'graph-netscience/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS role
FROM {{zone_name}}.netscience_raw.netscience_vertices;

-- ############################################################################
-- STEP 3b: Physical Layout — Z-ORDER for fast data skipping
-- ############################################################################
-- The data was loaded in vertex_id order, which has reasonable locality for
-- `vertex_id` but scatters the frequent filter column (role) across files.
-- Z-ORDER rewrites files so rows with similar values on the ordering keys
-- co-locate, giving Parquet min/max statistics much tighter ranges per file.
-- This benefits three hot paths:
--
--   1. CSR build from the edges table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `vertex_id` co-location lets the Parquet
--      reader skip almost every row group for targeted author lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE v.role = 'senior'` skip entire files instead of reading the
--      whole author table.
--
-- One-time cost at setup; every subsequent query benefits.

OPTIMIZE {{zone_name}}.netscience_collab.vertices
    ZORDER BY (vertex_id, role);

OPTIMIZE {{zone_name}}.netscience_collab.edges
    ZORDER BY (src, dst);

-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling author vertices with coauthorship edges.
-- Cypher queries reference this by name: USE {{zone_name}}.netscience_collab.netscience_collab MATCH ...

CREATE GRAPH IF NOT EXISTS {{zone_name}}.netscience_collab.netscience_collab
    VERTEX TABLE {{zone_name}}.netscience_collab.vertices ID COLUMN vertex_id NODE NAME COLUMN name NODE TYPE COLUMN role
    EDGE TABLE {{zone_name}}.netscience_collab.edges SOURCE COLUMN src TARGET COLUMN dst
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

CREATE GRAPHCSR {{zone_name}}.netscience_collab.netscience_collab;
