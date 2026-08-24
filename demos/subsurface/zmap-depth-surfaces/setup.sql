-- ============================================================================
-- Depth Surface Handover and Gross Rock Volume - Setup Script
-- ============================================================================
-- A geophysicist depth-converts a seismic interpretation and hands the result
-- to the mapping team as ZMAP+ grids. The team loads them to compute gross
-- rock volume, which is the number a prospect is sized on, and to compare one
-- depth-conversion iteration against the next.
--
--   11 March   TOP_HUGIN_V1                  the first pass
--   12 March   TOP_HUGIN_V2, BASE_HUGIN      the corrected top, and the base
--
-- Each grid is 45 rows by 60 columns on a 200 m spacing, so 2700 nodes of
-- which 1441 fall inside the mapped polygon and 1259 were never interpreted.
--
--   1. depth_grids     external, DISCOVER over the landing folder
--   2. all_nodes       external, the same first grid with its blanks kept
--   3. depth_surfaces  DELTA, the curated surfaces
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.mapping
    COMMENT 'ZMAP+ depth grids read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- One row per grid node, with the real-world coordinate already computed from
-- the header's extent and the node's position in the stream. The file itself
-- holds no coordinates at all: it holds a corner, an extent and a run of
-- numbers, and the reader does the arithmetic.
--
-- Nodes outside the mapped polygon carry a null sentinel of 1e30 and are
-- dropped by default, which is almost always what a query wants: read as a
-- number, one of them in an average destroys the answer.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.mapping.depth_grids;

DISCOVER {{zone_name}}.mapping.depth_grids
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The same grid with its blank nodes kept
-- ----------------------------------------------------------------------------
-- include_null_nodes turns the sentinel into a real NULL and keeps the row,
-- which is what a query about the SHAPE of the mapped area needs: the blanks
-- are where the interpretation stops, and that outline is data.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.mapping.all_nodes;

DISCOVER {{zone_name}}.mapping.all_nodes
    PATH '{{data_subdir}}/landing/2026-03-11_top_hugin_v1.zmap'
    WITH (include_null_nodes = 'true');


-- ----------------------------------------------------------------------------
-- STEP 4: The curated Delta surfaces
-- ----------------------------------------------------------------------------
-- The grid indices are renamed here. `row` and `column` are reserved words,
-- and a curated table nobody has to quote to query is worth the rename.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.mapping.depth_surfaces (
    surface       VARCHAR,
    iteration     INTEGER,
    delivered_on  VARCHAR,
    source_file   VARCHAR,
    grid_row      INTEGER,
    grid_column   INTEGER,
    x             DOUBLE,
    y             DOUBLE,
    depth_m       DOUBLE
) LOCATION '{{data_subdir}}/curated/depth_surfaces';
