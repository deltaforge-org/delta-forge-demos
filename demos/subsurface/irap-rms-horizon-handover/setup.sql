-- ============================================================================
-- RMS Horizon Handover: Irap Surfaces in Both Containers - Setup Script
-- ============================================================================
-- A geomodeller finishes a structural model in RMS and hands the horizons to
-- the subsurface team. RMS writes surfaces as Irap, and a handover routinely
-- carries the same surface twice: the binary .gri the project stores, and a
-- classic ASCII export made for a tool that only reads text.
--
--   8 April    TOP_HUGIN.gri, TOP_HUGIN.irap   one surface, two containers
--   9 April    BASE_HUGIN.gri                  the base that makes it a volume
--
-- Each surface is 60 columns by 45 rows on a 200 m spacing, rotated 24 degrees
-- counter-clockwise from east: 2700 nodes, of which 1508 fall inside the
-- mapped area and 1192 were never interpreted.
--
--   1. horizons     external, DISCOVER over the landing folder, both containers
--   2. all_nodes    external, the binary top with its blank nodes kept
--   3. surfaces     DELTA, the curated horizons
--
-- Every expected value in queries.sql was decoded from these files by
-- Equinor's xtgeo 4.25.1 before the engine saw them.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.geomodel
    COMMENT 'Irap horizons handed over from RMS, read in place and curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- One table over BOTH containers. The binary and the ASCII form of Irap are
-- told apart from the file's own first bytes, not from its name, so a delivery
-- that mixes them is one table rather than two.
--
-- One row per grid node, with the real-world coordinate already computed. The
-- file holds no coordinates: it holds an origin, a node spacing and a
-- rotation, and the reader does the trigonometry.
--
-- Nodes outside the mapped area carry an undefined sentinel and are dropped by
-- default. The sentinel is a THRESHOLD rather than an exact number, and it is
-- a different number in each container: 1e30 in the binary form, 9999900.0 in
-- the ASCII one. Compared against the wrong one, a blank node reads as a real
-- depth of nearly ten million metres.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.geomodel.horizons;

DISCOVER {{zone_name}}.geomodel.horizons
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The same top surface with its blank nodes kept
-- ----------------------------------------------------------------------------
-- include_undefined_nodes keeps the node and turns the sentinel into a real
-- NULL, which is what a question about the SHAPE of the mapped area needs: the
-- blanks are where the interpretation stops, and that outline is data.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.geomodel.all_nodes;

DISCOVER {{zone_name}}.geomodel.all_nodes
    PATH '{{data_subdir}}/landing/2026-04-08_top_hugin.gri'
    WITH (include_undefined_nodes = 'true');


-- ----------------------------------------------------------------------------
-- STEP 4: The curated Delta surfaces
-- ----------------------------------------------------------------------------
-- The grid indices are renamed here. `row` and `column` are reserved words,
-- and a curated table nobody has to quote to query is worth the rename.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.geomodel.surfaces (
    surface       VARCHAR,
    container     VARCHAR,
    delivered_on  VARCHAR,
    source_file   VARCHAR,
    grid_row      INTEGER,
    grid_column   INTEGER,
    x             DOUBLE,
    y             DOUBLE,
    depth_m       DOUBLE
) LOCATION '{{data_subdir}}/curated/surfaces';
