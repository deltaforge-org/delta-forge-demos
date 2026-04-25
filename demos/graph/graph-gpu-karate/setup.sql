-- ============================================================================
-- GPU Karate Club — Setup Script
-- ============================================================================
-- Loads the classic Zachary Karate Club dataset (1977) and creates a graph
-- for GPU algorithm quality verification. Same data as the CPU karate demo
-- but queries.sql forces GPU execution via ON GPU THRESHOLD 1 to
-- validate GPU algorithm correctness against published reference values.
--
-- Data source: W. W. Zachary, Journal of Anthropological Research, 1977
--
-- Vertices: 34 club members (IDs 0-33)
-- Edges: 78 rows in canonical form (src < dst); graph is UNDIRECTED, so the
--        engine infers the reverse direction at CSR build time. Weight = 1.0.
--
-- Graph:
--   {{zone_name}}.gpu_karate.gpu_karate — All members, friendships as edges
-- ============================================================================

-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.gpu_karate_raw
    COMMENT 'GPU Karate Club — external CSV staging tables';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.gpu_karate
    COMMENT 'GPU Karate club — Delta tables and graph for GPU quality verification';

-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.gpu_karate_raw.karate_edges
USING CSV LOCATION 'edges.csv'
OPTIONS (header = 'true', delimiter = '|');


-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (from external CSV) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.gpu_karate.edges
LOCATION 'delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.gpu_karate_raw.karate_edges;


-- === Vertex Table (from CSV with member names and roles) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.gpu_karate_raw.karate_vertices
USING CSV LOCATION 'vertices.csv'
OPTIONS (header = 'true', delimiter = '|');


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.gpu_karate.vertices
LOCATION 'delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS role
FROM {{zone_name}}.gpu_karate_raw.karate_vertices;


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
--      reader skip almost every row group for targeted member lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE v.role = 'Mr. Hi'` skip entire files instead of reading the
--      whole vertex table.
--
-- One-time cost at setup; every subsequent GPU kernel launch benefits.

OPTIMIZE {{zone_name}}.gpu_karate.vertices
    ZORDER BY (vertex_id, role);

OPTIMIZE {{zone_name}}.gpu_karate.edges
    ZORDER BY (src, dst);

-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################

CREATE GRAPH IF NOT EXISTS {{zone_name}}.gpu_karate.gpu_karate
    VERTEX TABLE {{zone_name}}.gpu_karate.vertices ID COLUMN vertex_id NODE TYPE COLUMN role NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.gpu_karate.edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN edge_type
    UNDIRECTED;

-- ############################################################################
-- STEP 5: Warm the CSR cache
-- ############################################################################
-- CREATE GRAPHCSR pre-builds the Compressed Sparse Row topology and writes
-- it to disk as a .dcsr file. The first Cypher query then loads in ~200 ms
-- instead of rebuilding from Delta tables. Safe to re-run after bulk edge
-- loads to refresh the cache.

CREATE GRAPHCSR {{zone_name}}.gpu_karate.gpu_karate;
