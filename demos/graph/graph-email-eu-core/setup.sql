-- ============================================================================
-- Email-Eu-core — Setup Script
-- ============================================================================
-- Loads the Email-Eu-core communication network (SNAP dataset) into Delta
-- tables and creates a named graph for algorithm verification.
--
-- Data source: Stanford SNAP (Leskovec et al.)
-- Format: pipe-delimited CSV with header (src|dst|weight|edge_type)
--
-- Vertices: 1,005 institution members (IDs 0–1004)
-- Edges: 25,571 directed email edges (NOT symmetric, has self-loops)
--
-- Graph:
--   {{zone_name}}.email_eu_core.email_eu_core — Members as vertices, emails as directed edges
-- ============================================================================
-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.email_eu_core_raw
    COMMENT 'Email-Eu-core — external CSV staging table';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.email_eu_core
    COMMENT 'Email-Eu-core — Delta tables and graph definition for SNAP email dataset';
-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.email_eu_core_raw.email_eu_edges
USING CSV LOCATION 'graph-email-eu-core/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.email_eu_core.edges
LOCATION 'graph-email-eu-core/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.email_eu_core_raw.email_eu_edges;

-- === Vertex Table (from CSV with member names and departments) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.email_eu_core_raw.email_eu_vertices
USING CSV LOCATION 'graph-email-eu-core/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.email_eu_core.vertices
LOCATION 'graph-email-eu-core/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS department
FROM {{zone_name}}.email_eu_core_raw.email_eu_vertices;

-- ############################################################################
-- STEP 3b: Physical Layout — Z-ORDER for fast data skipping
-- ############################################################################
-- The data was loaded in vertex_id order, which has reasonable locality for
-- `vertex_id` but scatters the frequent filter column (department) across
-- files.  Z-ORDER rewrites files so rows with similar values on the ordering
-- keys co-locate, giving Parquet min/max statistics much tighter ranges per
-- file.  This benefits three hot paths:
--
--   1. CSR build from the edges table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `vertex_id` co-location lets the Parquet
--      reader skip almost every row group for targeted member lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE v.department = '14'` skip entire files instead of reading the
--      whole vertex table.
--
-- One-time cost at setup; every subsequent query benefits.  These OPTIMIZE
-- statements also compact small files written by the CSV→Delta load.

OPTIMIZE {{zone_name}}.email_eu_core.vertices
    ZORDER BY (vertex_id, department);

OPTIMIZE {{zone_name}}.email_eu_core.edges
    ZORDER BY (src, dst);

-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling member vertices with email edges.
-- This is a DIRECTED graph — email from A to B does not imply B to A.
-- Cypher queries reference this by name: USE {{zone_name}}.email_eu_core.email_eu_core MATCH ...

CREATE GRAPH IF NOT EXISTS {{zone_name}}.email_eu_core.email_eu_core
    VERTEX TABLE {{zone_name}}.email_eu_core.vertices ID COLUMN vertex_id NODE NAME COLUMN name NODE TYPE COLUMN department
    EDGE TABLE {{zone_name}}.email_eu_core.edges SOURCE COLUMN src TARGET COLUMN dst
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

CREATE GRAPHCSR {{zone_name}}.email_eu_core.email_eu_core;
