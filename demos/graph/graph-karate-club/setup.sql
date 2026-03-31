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
-- Edges: 156 rows (78 undirected edges stored bidirectionally, weight=1.0)
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
USING CSV LOCATION '{{data_path}}/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.karate_club_raw.karate_edges TO USER {{current_user}};
-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.karate_club.edges
LOCATION '{{data_path}}/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.karate_club_raw.karate_edges;

GRANT ADMIN ON TABLE {{zone_name}}.karate_club.edges TO USER {{current_user}};
-- === Vertex Table (from CSV with member names and roles) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.karate_club_raw.karate_vertices
USING CSV LOCATION '{{data_path}}/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.karate_club_raw.karate_vertices TO USER {{current_user}};

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.karate_club.vertices
LOCATION '{{data_path}}/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS role
FROM {{zone_name}}.karate_club_raw.karate_vertices;

GRANT ADMIN ON TABLE {{zone_name}}.karate_club.vertices TO USER {{current_user}};
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
    DIRECTED;
