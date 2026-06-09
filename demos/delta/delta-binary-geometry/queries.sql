-- ============================================================================
-- Delta Binary & Spatial Data Types — Educational Queries
-- ============================================================================
-- WHAT: Storing binary-like data (hashes, checksums) and spatial geometry
--       (WKT) in Delta tables using VARCHAR columns
-- WHY:  Real-world data includes document fingerprints, geospatial coordinates,
--       and content-addressable patterns that need reliable storage and querying
-- HOW:  Delta stores these as VARCHAR columns in Parquet files; SHA-256 hashes
--       become 64-character hex strings, and spatial data uses the OGC
--       Well-Known Text (WKT) standard for POINT, POLYGON, and LINESTRING
-- ============================================================================


-- ============================================================================
-- EXPLORE: Document Store — File Types and Sizes
-- ============================================================================
-- The document store holds file metadata with SHA-256 content hashes.
-- Content hashing enables deduplication: two files with the same hash
-- have identical content regardless of filename.

ASSERT ROW_COUNT = 7
ASSERT VALUE doc_count = 7 WHERE mime_type = 'application/pdf'
ASSERT VALUE total_size_mb = 14.88 WHERE mime_type = 'application/pdf'
SELECT mime_type,
       COUNT(*) AS doc_count,
       ROUND(AVG(size_bytes) / 1024.0, 1) AS avg_size_kb,
       ROUND(SUM(size_bytes) / 1048576.0, 2) AS total_size_mb
FROM {{zone_name}}.delta_demos.document_store
GROUP BY mime_type
ORDER BY total_size_mb DESC;


-- ============================================================================
-- LEARN: SHA-256 Content Hashes as Fingerprints
-- ============================================================================
-- Each document has a 64-character hexadecimal SHA-256 hash stored as VARCHAR.
-- In a production system, you would compute this hash from the file's binary
-- content. Two files with the same hash are byte-for-byte identical.
-- Let's verify all hashes are properly formatted (64 hex characters).

-- Verify all displayed hashes are exactly 64 characters (SHA-256 hex)
ASSERT ROW_COUNT = 10
ASSERT VALUE hash_length = 64 WHERE id = 1
SELECT id, name,
       content_hash,
       LENGTH(content_hash) AS hash_length
FROM {{zone_name}}.delta_demos.document_store
ORDER BY id
LIMIT 10;


-- ============================================================================
-- LEARN: WKT Geometry Types — POINT, POLYGON, LINESTRING
-- ============================================================================
-- Well-Known Text (WKT) is an OGC standard for representing geometry as text:
--   POINT(lon lat)           — a single location
--   POLYGON((lon1 lat1, ...)) — a closed area
--   LINESTRING(lon1 lat1, ...) — a path or route
-- Delta stores these as VARCHAR, enabling pattern matching and text parsing.

-- Verify 20 POINTs, 5 POLYGONs, and 5 LINESTRINGs
ASSERT VALUE location_count = 20 WHERE loc_type = 'POINT'
ASSERT VALUE location_count = 5 WHERE loc_type = 'POLYGON'
ASSERT VALUE location_count = 5 WHERE loc_type = 'LINESTRING'
ASSERT ROW_COUNT = 3
SELECT loc_type,
       COUNT(*) AS location_count,
       MIN(name) AS example_name,
       MIN(wkt) AS example_wkt
FROM {{zone_name}}.delta_demos.geo_locations
GROUP BY loc_type
ORDER BY loc_type;


-- ============================================================================
-- EXPLORE: Geographic Distribution by Region
-- ============================================================================
-- Locations span 6 global regions with a mix of landmarks (POINTs),
-- parks/areas (POLYGONs), and routes (LINESTRINGs).

-- Verify 6 global regions are represented; North America has 7 after UPDATE reclassifications
ASSERT ROW_COUNT = 6
ASSERT VALUE total_locations = 7 WHERE region = 'North America'
ASSERT VALUE points = 5 WHERE region = 'North America'
SELECT region,
       COUNT(*) AS total_locations,
       COUNT(*) FILTER (WHERE loc_type = 'POINT') AS points,
       COUNT(*) FILTER (WHERE loc_type = 'POLYGON') AS polygons,
       COUNT(*) FILTER (WHERE loc_type = 'LINESTRING') AS linestrings
FROM {{zone_name}}.delta_demos.geo_locations
GROUP BY region
ORDER BY total_locations DESC;


-- ============================================================================
-- LEARN: Querying Spatial Data with String Functions
-- ============================================================================
-- Without a dedicated spatial engine, you can still extract coordinates from
-- WKT strings and filter by bounding box. This finds all POINT locations
-- in the Northern Hemisphere (latitude > 0).

-- Verify 13 POINTs in the Northern Hemisphere
ASSERT ROW_COUNT = 13
SELECT name, region, latitude, longitude, wkt
FROM {{zone_name}}.delta_demos.geo_locations
WHERE loc_type = 'POINT' AND latitude > 0
ORDER BY latitude DESC;


-- ============================================================================
-- EXPLORE: Document Cleanup — Deprecated Files Removed
-- ============================================================================
-- Two deprecated documents (deprecated_schema_v1.json and old_migration_notes.csv)
-- were DELETEd from the document store. In the Delta log, this appears as
-- a copy-on-write operation: the data file containing those rows is rewritten
-- without them, and the old file is marked as removed.

ASSERT ROW_COUNT = 5
ASSERT VALUE name = 'architecture_diagram.png' WHERE id = 23
SELECT id, name, mime_type, created_at
FROM {{zone_name}}.delta_demos.document_store
ORDER BY created_at DESC
LIMIT 5;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Verification of document counts, geometry types, hash integrity, and regions.

-- Verify document count
ASSERT ROW_COUNT = 23
SELECT * FROM {{zone_name}}.delta_demos.document_store;

-- Verify geo location count
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.geo_locations;

-- Verify PDF count
ASSERT VALUE pdf_count = 7
SELECT COUNT(*) AS pdf_count FROM {{zone_name}}.delta_demos.document_store WHERE mime_type = 'application/pdf';

-- Verify POINT count
ASSERT VALUE point_count = 20
SELECT COUNT(*) AS point_count FROM {{zone_name}}.delta_demos.geo_locations WHERE loc_type = 'POINT';

-- Verify POLYGON and LINESTRING count
ASSERT VALUE polygon_linestring_count = 10
SELECT COUNT(*) AS polygon_linestring_count FROM {{zone_name}}.delta_demos.geo_locations WHERE loc_type IN ('POLYGON', 'LINESTRING');

-- Verify all hashes are 64 characters
ASSERT VALUE valid_hash_count = 23
SELECT COUNT(*) AS valid_hash_count FROM {{zone_name}}.delta_demos.document_store WHERE LENGTH(content_hash) = 64;

-- Verify all WKT values start with valid geometry type
ASSERT VALUE valid_wkt_count = 30
SELECT COUNT(*) AS valid_wkt_count FROM {{zone_name}}.delta_demos.geo_locations WHERE wkt LIKE 'POINT%' OR wkt LIKE 'POLYGON%' OR wkt LIKE 'LINESTRING%';

-- Verify region count
ASSERT VALUE region_count = 6
SELECT COUNT(DISTINCT region) AS region_count FROM {{zone_name}}.delta_demos.geo_locations;
