-- ============================================================================
-- Zachary's Karate Club — Setup Script
-- ============================================================================
-- Loads the classic Zachary Karate Club dataset (1977) into Delta tables
-- and creates a named graph for algorithm verification.
--
-- Data source: W. W. Zachary, Journal of Anthropological Research, 1977
-- Format: pipe-delimited CSV with header (src|dst|weight|edge_type)
--
-- Vertices: 34 club members (IDs 0–33)
-- Edges: 78 rows in canonical form (src < dst); graph is UNDIRECTED, so the
--        engine infers the reverse direction at CSR build time. Weight = 1.0.
--
-- Graph:
--   {{zone_name}}.karate_club.karate_club — All members as vertices, friendships as edges
-- ============================================================================
-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.karate_club_raw
    COMMENT 'Karate Club — external CSV staging tables';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.karate_club
    COMMENT 'Karate club — Delta tables and graph definition for Zachary dataset';
-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.karate_club_raw.karate_edges
USING CSV LOCATION 'graph-karate-club/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.karate_club.edges
LOCATION 'graph-karate-club/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.karate_club_raw.karate_edges;

-- === Vertex Table (from CSV with member names and roles) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.karate_club_raw.karate_vertices
USING CSV LOCATION 'graph-karate-club/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.karate_club.vertices
LOCATION 'graph-karate-club/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS role
FROM {{zone_name}}.karate_club_raw.karate_vertices;

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
-- One-time cost at setup; every subsequent query benefits.

OPTIMIZE {{zone_name}}.karate_club.vertices
    ZORDER BY (vertex_id, role);

OPTIMIZE {{zone_name}}.karate_club.edges
    ZORDER BY (src, dst);

-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling member vertices with friendship edges.
-- Cypher queries reference this by name: USE {{zone_name}}.karate_club.karate_club MATCH ...

CREATE GRAPH IF NOT EXISTS {{zone_name}}.karate_club.karate_club
    VERTEX TABLE {{zone_name}}.karate_club.vertices ID COLUMN vertex_id NODE TYPE COLUMN role NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.karate_club.edges SOURCE COLUMN src TARGET COLUMN dst
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

CREATE GRAPHCSR {{zone_name}}.karate_club.karate_club;
