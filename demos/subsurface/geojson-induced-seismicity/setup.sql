-- ============================================================================
-- Induced Seismicity Monitoring - Setup Script
-- ============================================================================
-- Produced water from oil and gas production is disposed of by injecting it
-- back underground, and in Oklahoma and the Permian Basin that injection is
-- linked to earthquakes. Operators and regulators watch the seismic record
-- around their disposal wells, because a magnitude threshold being crossed is
-- what triggers a rate reduction or a shut-in.
--
-- The monitoring team pulls the USGS catalogue for the play each month and
-- loads it into a register that accumulates rather than being replaced:
--
--   11 March   2026-01   34 events
--   11 March   2026-02   47 events
--   12 March   2026-03   64 events
--
-- The data is REAL. It is the USGS earthquake catalogue, queried for
-- magnitude 2.5 and above between 31.0 and 37.0 north and 104.5 and 94.4
-- west, which covers the Oklahoma seismic zone and the Permian Basin. USGS
-- data is a work of the United States government and is in the public domain.
-- See ATTRIBUTION.md in the parent folder for the exact query.
--
-- GeoJSON is JSON, read through the JSON engine under a curated profile
-- rather than a parser of its own. Two properties of that profile decide what
-- the table looks like:
--
--   * `$.features` is both the row path and an explode path, so one feature
--     becomes one row rather than the whole collection becoming one row.
--   * `$.geometry` is OPAQUE. A point's coordinate array stays a single value
--     instead of exploding into a column per ordinate, which for a polygon
--     would mean a column per vertex.
--
--   1. seismic_feed     external, DISCOVER over the landing folder
--   2. seismic_register DELTA, the accumulating catalogue
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.seismicity
    COMMENT 'USGS seismic catalogue read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- One DISCOVER over the folder rather than one per file: every monthly pull
-- has the same shape, so they belong in one external table, and the file name
-- travels with each row as the watermark the incremental load keys on.
--
-- Column names come from the feature, so a property lands as
-- `properties_<name>` and a camelCase property is split: USGS writes
-- `magType` and the column is `properties_mag_type`.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.seismicity.seismic_feed;

DISCOVER {{zone_name}}.seismicity.seismic_feed
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated register
-- ----------------------------------------------------------------------------
-- event_id is the USGS event identifier and is what makes a re-pull safe: the
-- same event carries the same id in every catalogue query that covers it.
-- The load still keys on source_file, because the question a monitoring
-- register answers is "which pulls have I already taken", and an event can be
-- revised between pulls without changing its id.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.seismicity.seismic_register (
    catalogue_month VARCHAR,
    delivered_on    VARCHAR,
    source_file     VARCHAR,
    event_id        VARCHAR,
    magnitude       DOUBLE,
    magnitude_type  VARCHAR,
    place           VARCHAR,
    event_time      BIGINT,
    significance    BIGINT,
    network         VARCHAR,
    event_type      VARCHAR,
    geometry        VARCHAR
) LOCATION '{{data_subdir}}/curated/seismic_register';
