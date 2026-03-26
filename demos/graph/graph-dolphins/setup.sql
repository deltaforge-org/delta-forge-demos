-- ============================================================================
-- Dolphins Social Network — Setup Script
-- ============================================================================
-- Loads the Dolphins Social Network dataset (Lusseau et al., 2003) into
-- Delta tables and creates a named graph for algorithm verification.
--
-- Data source: Lusseau et al., Behavioral Ecology and Sociobiology, 2003
-- Format: pipe-delimited CSV with header (src|dst|weight|edge_type)
--
-- Vertices: 62 dolphins (IDs 0–61)
-- Edges: 318 rows (159 undirected edges stored bidirectionally, weight=1.0)
--
-- Graph:
--   {{zone_name}}.dolphins.dolphins_social — All dolphins as vertices, associations as edges
-- ============================================================================


-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.raw
    COMMENT 'Dolphins — external CSV staging table';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.dolphins
    COMMENT 'Dolphins — Delta tables and graph definition';


-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.dolphins_edges
USING CSV LOCATION '{{data_path}}/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.raw.dolphins_edges TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.raw.dolphins_edges;


-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.dolphins.edges
LOCATION '{{data_path}}/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.raw.dolphins_edges;

GRANT ADMIN ON TABLE {{zone_name}}.dolphins.edges TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.dolphins.edges;


-- === Vertex Table (from CSV with dolphin names) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.dolphins_vertices
USING CSV LOCATION '{{data_path}}/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.raw.dolphins_vertices TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.raw.dolphins_vertices;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.dolphins.vertices
LOCATION '{{data_path}}/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS category
FROM {{zone_name}}.raw.dolphins_vertices;

GRANT ADMIN ON TABLE {{zone_name}}.dolphins.vertices TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.dolphins.vertices;


-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling dolphin vertices with association edges.
-- Cypher queries reference this by name: USE {{zone_name}}.dolphins.dolphins_social MATCH ...

CREATE GRAPH IF NOT EXISTS {{zone_name}}.dolphins.dolphins_social
    VERTEX TABLE {{zone_name}}.dolphins.vertices ID COLUMN vertex_id NODE NAME COLUMN name NODE TYPE COLUMN category
    EDGE TABLE {{zone_name}}.dolphins.edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN edge_type
    DIRECTED;
