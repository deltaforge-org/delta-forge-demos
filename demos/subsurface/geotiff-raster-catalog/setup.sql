-- ============================================================================
-- Aerial Survey Raster Catalogue - Setup Script
-- ============================================================================
-- An environmental baseline survey delivers orthophoto and elevation tiles for
-- a licence area. Before anything is processed the GIS team has to know what
-- arrived: how much ground it covers, at what resolution, and whether every
-- tile is in the same coordinate system.
--
--   11 March   three orthophoto tiles
--   12 March   two more orthophotos and a BigTIFF elevation model
--
-- Answering those questions by opening the images means reading gigabytes.
-- Answering them from the tag directories means reading kilobytes, and that
-- is what this reader does: one row per image directory, describing the
-- raster rather than decoding it.
--
--   1. raster_tiles    external, DISCOVER over the landing folder
--   2. raster_catalog  DELTA, the survey catalogue
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.raster_survey
    COMMENT 'GeoTIFF tag directories read in place, catalogued into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- A TIFF is recognised from its byte-order marker and magic number, 42 for
-- classic and 43 for BigTIFF, and both are in this delivery.
--
-- One row comes out per image DIRECTORY, not per file. A TIFF is a chain of
-- directories and an overview pyramid is simply more of them in the same
-- file, so these six tiles produce eighteen rows: full resolution and two
-- overviews each. A reader that follows only the first directory sees a third
-- of what the file describes.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raster_survey.raster_tiles;

DISCOVER {{zone_name}}.raster_survey.raster_tiles
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta catalogue
-- ----------------------------------------------------------------------------
-- Ground extent is derived rather than stored: a raster's width in metres is
-- its pixel count times its pixel scale, and keeping that as a column is what
-- lets the catalogue answer a coverage question without arithmetic in every
-- query.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.raster_survey.raster_catalog (
    tile              VARCHAR,
    delivered_on      VARCHAR,
    source_file       VARCHAR,
    directory_index   INTEGER,
    is_full_resolution BOOLEAN,
    width             BIGINT,
    height            BIGINT,
    bands             INTEGER,
    bits_per_sample   INTEGER,
    pixel_scale_m     DOUBLE,
    epsg              INTEGER,
    origin_x          DOUBLE,
    origin_y          DOUBLE,
    ground_width_m    DOUBLE,
    ground_height_m   DOUBLE
) LOCATION '{{data_subdir}}/curated/raster_catalog';
