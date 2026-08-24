-- ============================================================================
-- Licence Block Acreage and Relinquishment - Setup Script
-- ============================================================================
-- A licensing authority publishes the blocks awarded in each round as
-- GeoJSON. An operator loads them to track the acreage it holds and, more to
-- the point, the relinquishment obligations attached to it: a licence hands
-- acreage back at the end of its term, and missing that date costs the block
-- rather than the obligation.
--
--   11 March   APA-2025, quadrants 15 and 16    7 blocks
--   12 March   APA-2026, quadrant 25            5 blocks
--
-- The blocks sit on the real Norwegian quadrant grid, one degree of latitude
-- by two of longitude divided into twelve, so each is a genuine graticule
-- rectangle and its area is the area that rectangle has at that latitude.
--
--   1. block_awards    external, DISCOVER over the landing folder
--   2. licence_blocks  DELTA, the acreage register
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.licensing
    COMMENT 'GeoJSON licence awards read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- GeoJSON is JSON, read through the JSON engine under a curated profile
-- rather than a parser of its own. Detection needs the type declaration and a
-- geometry rather than the extension, which these files share with every
-- other JSON file on disk.
--
-- The profile keeps `geometry` whole. That is the interesting part: a
-- polygon's coordinate array flattened would produce a column per vertex, and
-- a schema that changes shape with the number of corners in a block is not a
-- schema.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.licensing.block_awards;

DISCOVER {{zone_name}}.licensing.block_awards
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta acreage register
-- ----------------------------------------------------------------------------
-- The geometry rides along as the JSON value it already is, so a block keeps
-- its outline without the register having to model a polygon.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.licensing.licence_blocks (
    block            VARCHAR,
    quadrant         INTEGER,
    licence          VARCHAR,
    operator         VARCHAR,
    licence_round    VARCHAR,
    awarded_year     INTEGER,
    term_years       INTEGER,
    relinquish_by    INTEGER,
    work_commitment  VARCHAR,
    area_km2         DOUBLE,
    geometry         VARCHAR,
    delivered_on     VARCHAR,
    source_file      VARCHAR
) LOCATION '{{data_subdir}}/curated/licence_blocks';
