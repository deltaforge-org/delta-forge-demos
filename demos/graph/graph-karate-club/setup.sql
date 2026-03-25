-- ============================================================================
-- Zachary's Karate Club — Setup Script
-- ============================================================================
-- Loads the classic Zachary Karate Club dataset (1977) into Delta tables
-- and creates a named graph for algorithm verification.
--
-- Data source: W. W. Zachary, Journal of Anthropological Research, 1977
-- Format: pipe-delimited CSV with header (src|dst|weight)
--
-- Vertices: 34 club members (IDs 0–33)
-- Edges: 156 rows (78 undirected edges stored bidirectionally, weight=1.0)
--
-- Graph:
--   {{zone_name}}.karate.karate_club — All members as vertices, friendships as edges
-- ============================================================================


-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.raw
    COMMENT 'Karate Club — external CSV staging table';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.karate
    COMMENT 'Karate Club — Delta tables and graph definition';


-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.karate_edges
USING CSV LOCATION '{{data_path}}/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.raw.karate_edges TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.raw.karate_edges;


-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.karate.edges
LOCATION '{{data_path}}/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight
FROM {{zone_name}}.raw.karate_edges;

GRANT ADMIN ON TABLE {{zone_name}}.karate.edges TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.karate.edges;


-- === Vertex Table (derived from edges) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.karate.vertices
LOCATION '{{data_path}}/delta/vertices'
AS SELECT DISTINCT
    vertex_id,
    'Member ' || CAST(vertex_id AS VARCHAR) AS name,
    CASE
        WHEN vertex_id = 0 THEN 'Instructor'
        WHEN vertex_id = 33 THEN 'President'
        ELSE 'Member'
    END AS role
FROM (
    SELECT src AS vertex_id FROM {{zone_name}}.karate.edges
    UNION
    SELECT dst AS vertex_id FROM {{zone_name}}.karate.edges
);

GRANT ADMIN ON TABLE {{zone_name}}.karate.vertices TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.karate.vertices;


-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling member vertices with friendship edges.
-- Cypher queries reference this by name: USE {{zone_name}}.karate.karate_club MATCH ...

CREATE GRAPH IF NOT EXISTS {{zone_name}}.karate.karate_club
    VERTEX TABLE {{zone_name}}.karate.vertices ID COLUMN vertex_id NODE TYPE COLUMN role NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.karate.edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    DIRECTED;
