-- ============================================================================
-- Drilling Survey Anti-Collision - Setup Script
-- ============================================================================
-- A directional driller hands over the definitive survey at the end of each
-- bit run, as WITSML. The well engineer needs those trajectories in the lake
-- for two reasons: to tie each well to the geological model, and to run
-- anti-collision checks against the wells already drilled from the same
-- platform template.
--
--   11 March   15/9-F-11 (40 stations), 15/9-F-11 A (32 stations)
--   12 March   15/9-F-12 (48 stations)
--
-- All three are drilled from one template, so they leave surface a few metres
-- apart and diverge with depth. That is the whole reason anti-collision
-- exists: the wells are closest where they leave the template, not where they
-- land.
--
--   1. survey_documents  external, DISCOVER over the landing folder
--   2. survey_stations   DELTA, one row per station with usable column names
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.drilling
    COMMENT 'WITSML definitive surveys read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- WITSML is XML, and DeltaForge reads it through the XML engine under a
-- curated profile rather than a parser of its own. That is the point of the
-- USING keyword here: the profile already knows that `//well` is the row and
-- that `//trajectoryStation` explodes, so nobody writes an XPath.
--
-- Detection is on the document's own vocabulary rather than the extension,
-- because every one of these dialects ships as a plain .xml file and the
-- extension says nothing. A document that merely MENTIONS witsml is not a
-- WITSML document; the namespace or a real element is what counts.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.drilling.survey_documents;

DISCOVER {{zone_name}}.drilling.survey_documents
    PATH '{{data_path}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta table
-- ----------------------------------------------------------------------------
-- The flattened column names are the document's full element path, so a
-- station's measured depth arrives as
-- `wells_well_trajectory_trajectory_station_md`. That is correct and
-- unambiguous, and nobody wants to write it twice.
--
-- Renaming them is most of what this curated layer is for. The other part is
-- types: the survey numbers arrive as the document wrote them, and an
-- anti-collision calculation needs doubles it can take a square root of.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.drilling.survey_stations (
    well             VARCHAR,
    well_uid         VARCHAR,
    field            VARCHAR,
    delivered_on     VARCHAR,
    source_file      VARCHAR,
    station_uid      VARCHAR,
    md_m             DOUBLE,
    tvd_m            DOUBLE,
    inclination_deg  DOUBLE,
    azimuth_deg      DOUBLE,
    north_m          DOUBLE,
    east_m           DOUBLE
) LOCATION '{{data_path}}/curated/survey_stations';
