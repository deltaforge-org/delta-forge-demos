-- ============================================================================
-- Offshore Lease Footprint - Setup Script
-- ============================================================================
-- Every active oil and gas lease on the United States Outer Continental Shelf,
-- as the Bureau of Ocean Energy Management publishes it, loaded against the
-- official block grid the leases are measured on.
--
--   11 March   leases   1870 lease polygons
--   12 March   blocks   29,186 block polygons, the reference grid
--
-- The data is REAL and is in the public domain, being a work of the United
-- States government. See ATTRIBUTION.md in the parent folder.
--
-- A shapefile is not one file. It is a set that must be read together:
--
--   .shp   the geometry, as a record per shape
--   .dbf   the attributes, as a dBASE III table, one row per shape in order
--   .shx   the index from record number to byte offset in the .shp
--   .prj   the coordinate reference system, as WKT
--   .cpg   the code page the .dbf strings are in
--
-- The reader opens the .shp and pulls its siblings in beside it, which is why
-- DISCOVER is pointed at the .shp and the rest are found rather than named.
--
--   1. leases         external, DISCOVER over the lease geometry
--   2. blocks         external, DISCOVER over the block grid
--   3. lease_register DELTA, the curated register
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.surface_land
    COMMENT 'BOEM lease and block shapefiles read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register both layers with DISCOVER
-- ----------------------------------------------------------------------------
-- A shapefile is recognised from the .shp header's file code and shape type, a
-- two-field agreement no other format produces. Both of these are file code
-- 9994 and shape type 5, which is Polygon.
--
-- Three columns come from the geometry side (record_index, geometry_type and
-- geometry as OGC well-known binary) and the rest come from the .dbf, with
-- their names lowercased.
--
-- Those names are worth looking at, because they are the format speaking
-- rather than a naming choice. DBF truncates every field name to TEN
-- characters: LEASE_NUMBER becomes lease_numb, SALE_NUMBER becomes sale_numbe,
-- CURRENT_AREA becomes current_ar, and LEASE_EFF_DATE becomes lease_eff: the
-- cut landed mid-word and left DBF holding LEASE_EFF_, whose trailing
-- underscore is dropped when the name is normalised. This is what real
-- shapefile columns look like.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.surface_land.leases;

DISCOVER {{zone_name}}.surface_land.leases
    PATH '{{data_subdir}}/landing/leases.shp'
    WITH (FILE_METADATA = true);

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.surface_land.blocks;

DISCOVER {{zone_name}}.surface_land.blocks
    PATH '{{data_subdir}}/landing/blocks.shp'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated register
-- ----------------------------------------------------------------------------
-- The truncated DBF names are given readable ones here, which is the right
-- place to do it: the external table shows the file as it is, and the curated
-- table is where a naming decision belongs.
--
-- effective_date stays a VARCHAR because BOEM writes it as YYYYMMDD and the
-- range is genuinely ninety years, from 1936 to 2026. Parsing it into a date
-- is a decision with an error mode, and this register does not need it.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.surface_land.lease_register (
    delivered_on    VARCHAR,
    source_file     VARCHAR,
    lease_number    VARCHAR,
    mineral_type    VARCHAR,
    lease_status    VARCHAR,
    effective_date  VARCHAR,
    royalty_rate    DOUBLE,
    current_area    DOUBLE,
    geometry_type   VARCHAR
) LOCATION '{{data_subdir}}/curated/lease_register';
