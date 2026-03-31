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

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.raw
    COMMENT 'Email-Eu-core — external CSV staging table';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.email_eu_core
    COMMENT 'Email-Eu-core — Delta tables and graph definition for SNAP email dataset';
-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.email_eu_edges
USING CSV LOCATION '{{data_path}}/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.raw.email_eu_edges TO USER {{current_user}};
-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.email_eu_core.edges
LOCATION '{{data_path}}/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.raw.email_eu_edges;

GRANT ADMIN ON TABLE {{zone_name}}.email_eu_core.edges TO USER {{current_user}};
-- === Vertex Table (from CSV with member names and departments) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.email_eu_vertices
USING CSV LOCATION '{{data_path}}/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.raw.email_eu_vertices TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.email_eu_core.vertices
LOCATION '{{data_path}}/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS department
FROM {{zone_name}}.raw.email_eu_vertices;

GRANT ADMIN ON TABLE {{zone_name}}.email_eu_core.vertices TO USER {{current_user}};
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
