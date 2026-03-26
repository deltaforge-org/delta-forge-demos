-- ============================================================================
-- Political Books — Setup Script
-- ============================================================================
-- Loads the Political Books co-purchasing dataset (Krebs / Newman) into
-- Delta tables and creates a named graph for algorithm verification.
--
-- Data source: V. Krebs, unpublished; compiled by M. E. J. Newman
-- Format: pipe-delimited CSV with header (src|dst|weight|edge_type)
--
-- Vertices: 105 political books (IDs 0–104)
-- Edges: 882 rows (441 undirected edges stored bidirectionally, weight=1.0)
--
-- Graph:
--   {{zone_name}}.polbooks.political_books — All books as vertices, co-purchases as edges
-- ============================================================================


-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.raw
    COMMENT 'Political Books — external CSV staging table';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.polbooks
    COMMENT 'Political Books — Delta tables and graph definition';


-- ############################################################################
-- STEP 2: External Table — Raw CSV Reader (pipe-delimited)
-- ############################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.polbooks_edges
USING CSV LOCATION '{{data_path}}/edges.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.raw.polbooks_edges TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.raw.polbooks_edges;


-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################

-- === Edge Table (CTAS from external) ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.polbooks.edges
LOCATION '{{data_path}}/delta/edges'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(weight AS DOUBLE) AS weight,
    CAST(edge_type AS VARCHAR) AS edge_type
FROM {{zone_name}}.raw.polbooks_edges;

GRANT ADMIN ON TABLE {{zone_name}}.polbooks.edges TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.polbooks.edges;


-- === Vertex Table (from CSV with book titles and political leanings) ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.polbooks_vertices
USING CSV LOCATION '{{data_path}}/vertices.csv'
OPTIONS (header = 'true', delimiter = '|');

GRANT ADMIN ON TABLE {{zone_name}}.raw.polbooks_vertices TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.raw.polbooks_vertices;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.polbooks.vertices
LOCATION '{{data_path}}/delta/vertices'
AS SELECT
    CAST(vertex_id AS BIGINT) AS vertex_id,
    CAST(name AS VARCHAR) AS name,
    CAST(category AS VARCHAR) AS leaning
FROM {{zone_name}}.raw.polbooks_vertices;

GRANT ADMIN ON TABLE {{zone_name}}.polbooks.vertices TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.polbooks.vertices;


-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling book vertices with co-purchasing edges.
-- Cypher queries reference this by name: USE {{zone_name}}.polbooks.political_books MATCH ...

CREATE GRAPH IF NOT EXISTS {{zone_name}}.polbooks.political_books
    VERTEX TABLE {{zone_name}}.polbooks.vertices ID COLUMN vertex_id NODE NAME COLUMN name NODE TYPE COLUMN leaning
    EDGE TABLE {{zone_name}}.polbooks.edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN edge_type
    DIRECTED;
