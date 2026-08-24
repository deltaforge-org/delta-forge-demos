-- ============================================================================
-- Well Pad Lease Compliance - Setup Script
-- ============================================================================
-- An onshore operator has to prove that every well pad it has built sits
-- inside a tract it actually leases. The land department holds the lease
-- tracts as polygons and the drilling department holds the pads as points,
-- and until both are in the same place nobody can answer the question.
--
--   11 March   well_pads      8 points, NAD83 UTM zone 13N
--   12 March   lease_tracts   4 polygons, the same projection
--
-- A shapefile is three files that have to travel together. The .shp holds
-- geometry as big-endian record headers wrapped around little-endian
-- coordinates, which is the format's own inconsistency and the detail that
-- catches naive readers. The .dbf is a dBase III table with one attribute row
-- per shape, in file order, with no key joining them. The .prj states the
-- coordinate system. Lose the .dbf and you have geometry with no attributes;
-- lose the .prj and you have coordinates with no meaning.
--
--   1. well_pads      external, DISCOVER over the pad delivery
--   2. lease_tracts   external, DISCOVER over the tract delivery
--   3. pad_compliance DELTA, every pad with the tract it sits in, or none
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.surface_land
    COMMENT 'Shapefile pads and lease tracts read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register both deliveries with DISCOVER
-- ----------------------------------------------------------------------------
-- A shapefile is recognised from the .shp header's file code and shape type,
-- a two-field agreement no other format produces. The reader opens the
-- companion .dbf beside it, so the attribute columns arrive without being
-- asked for.
--
-- The two layers are registered separately because they are different
-- geometries with different attributes: points with a pad identifier, and
-- polygons with a lease. Nothing would be gained by forcing them into one
-- table and the geometry column would have to hold both.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.surface_land.well_pads;

DISCOVER {{zone_name}}.surface_land.well_pads
    PATH '{{data_subdir}}/landing/2026-03-11_well_pads.shp'
    WITH (FILE_METADATA = true);

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.surface_land.lease_tracts;

DISCOVER {{zone_name}}.surface_land.lease_tracts
    PATH '{{data_subdir}}/landing/2026-03-12_lease_tracts.shp'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta compliance register
-- ----------------------------------------------------------------------------
-- One row per pad, carrying the tract it sits in or NULL where it sits in
-- none. The NULL is the whole point of the table: a compliance register that
-- silently dropped the pads it could not place would report full compliance.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.surface_land.pad_compliance (
    pad_id        VARCHAR,
    operator      VARCHAR,
    spud_year     INTEGER,
    status        VARCHAR,
    easting       DOUBLE,
    northing      DOUBLE,
    tract_id      VARCHAR,
    lessor        VARCHAR,
    lease_expiry  INTEGER,
    compliant     BOOLEAN,
    delivered_on  VARCHAR,
    source_file   VARCHAR
) LOCATION '{{data_subdir}}/curated/pad_compliance';
