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
--   {{zone_name}}.netscience.netscience_collab — Authors as vertices, coauthorships as edges
-- ============================================================================


-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.raw
    COMMENT 'NetScience — external CSV staging table';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.netscience
    COMMENT 'NetScience — Delta tables and graph definition';


-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.netscience_edges
USING CSV LOCATION '{{data_path}}/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

DETECT SCHEMA FOR TABLE {{zone_name}}.raw.netscience_edges;
GRANT ADMIN ON TABLE {{zone_name}}.raw.netscience_edges TO USER {{current_user}};


-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.netscience.edges
LOCATION '{{data_path}}/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.raw.netscience_edges;

DETECT SCHEMA FOR TABLE {{zone_name}}.netscience.edges;
GRANT ADMIN ON TABLE {{zone_name}}.netscience.edges TO USER {{current_user}};


-- === Vertex Table (from CSV with researcher names and roles) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.netscience_vertices
USING CSV LOCATION '{{data_path}}/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');

DETECT SCHEMA FOR TABLE {{zone_name}}.raw.netscience_vertices;
GRANT ADMIN ON TABLE {{zone_name}}.raw.netscience_vertices TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.netscience.vertices
LOCATION '{{data_path}}/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS role
FROM {{zone_name}}.raw.netscience_vertices;

DETECT SCHEMA FOR TABLE {{zone_name}}.netscience.vertices;
GRANT ADMIN ON TABLE {{zone_name}}.netscience.vertices TO USER {{current_user}};


-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling author vertices with coauthorship edges.
-- Cypher queries reference this by name: USE {{zone_name}}.netscience.netscience_collab MATCH ...

CREATE GRAPH IF NOT EXISTS {{zone_name}}.netscience.netscience_collab
    VERTEX TABLE {{zone_name}}.netscience.vertices ID COLUMN vertex_id NODE NAME COLUMN name NODE TYPE COLUMN role
    EDGE TABLE {{zone_name}}.netscience.edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN edge_type
    DIRECTED;
