-- ============================================================================
-- Survey Navigation Database - Setup Script
-- ============================================================================
-- A survey contractor delivers 2D navigation as UKOOA P1/90, one file per
-- line. The client builds a shot-point database from it, because navigation
-- is what ties a seismic trace to a place on the earth: without it a survey
-- is a picture with no map reference.
--
-- The files are real. These are four of the ST0299 CMP navigation files from
-- Equinor's Volve release as the OSDU Forum redistributes them, shot in April
-- 2002 by Fugro from the Geo Searcher with a 4x40 cubic inch gun array. The
-- header records still say so.
--
--   11 March   ST0299-CMP-05002, ST0299-CMP-05003    95 positions each
--   12 March   ST0299-CMP-05004, ST0299-CMP-05005    95 positions each
--
-- P1/90 is fixed-width positional text: the column a character sits in is its
-- meaning, there is no delimiter and no header row to infer. Coordinates are
-- packed as degrees, minutes and seconds run together with a hemisphere
-- letter, so 582533.61N is 58.4260 degrees north and not five hundred and
-- eighty-two thousand of anything.
--
--   1. navigation_lines   external, DISCOVER over the landing folder
--   2. shot_point_index   DELTA, the curated shot-point database
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.survey_navigation
    COMMENT 'Volve ST0299 P1/90 navigation read in place, indexed into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- Navigation is identified from the shape of the records: a run of H header
-- records followed by fixed-width position records. There is no magic number
-- to find, because the file is text.
--
-- These particular files are all CMP records, which is what a common mid
-- point deliverable is. That is worth saying because a reader that treats the
-- C record identifier as a comment returns an empty table for the whole
-- class, and does it silently.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.survey_navigation.navigation_lines;

DISCOVER {{zone_name}}.survey_navigation.navigation_lines
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta shot-point index
-- ----------------------------------------------------------------------------
-- Both coordinate systems are kept. The geographic pair is what a map needs
-- and the grid pair is what ties to the seismic, and a P1/90 record carries
-- both because the survey was navigated in one and processed in the other.
-- Storing only one would mean recomputing the other with a projection nobody
-- wrote down.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.survey_navigation.shot_point_index (
    line          VARCHAR,
    delivered_on  VARCHAR,
    source_file   VARCHAR,
    record_type   VARCHAR,
    point_number  INTEGER,
    latitude      DOUBLE,
    longitude     DOUBLE,
    easting       DOUBLE,
    northing      DOUBLE
) LOCATION '{{data_subdir}}/curated/shot_point_index';
