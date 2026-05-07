-- ============================================================================
-- Delta Binary & Geometry Advanced — Educational Queries
-- ============================================================================
-- WHAT: Advanced analytics over binary hashes, spatial WKT geometry, and
--       cross-table document/location relationships in Delta tables
-- WHY:  Real-world data pipelines require deduplication via content hashing,
--       spatial filtering with bounding boxes, and multi-table analytics that
--       combine document metadata with geographic context
-- HOW:  Window functions for ranking and running totals, CTEs for multi-step
--       analytics, self-joins for pairwise distance computation, and
--       content-hash grouping for duplicate detection — all stored as
--       VARCHAR in Delta Parquet files
-- ============================================================================


-- ============================================================================
-- LEARN: Content-Hash Deduplication — Finding Duplicate Documents
-- ============================================================================
-- SHA-256 hashes enable content-addressable storage: two files with the same
-- hash are byte-for-byte identical. This query groups documents by hash to
-- find duplicates and quantifies wasted storage from redundant copies.

ASSERT ROW_COUNT = 5
ASSERT VALUE copy_count = 2 WHERE hash_prefix = 'a1b2c3d4e5f67890'
SELECT SUBSTRING(content_hash, 1, 16) AS hash_prefix,
       COUNT(*) AS copy_count,
       MIN(name) AS first_name,
       MAX(name) AS last_name,
       MIN(size_bytes) * (COUNT(*) - 1) AS wasted_bytes
FROM {{zone_name}}.delta_demos.documents
GROUP BY content_hash
HAVING COUNT(*) > 1
ORDER BY wasted_bytes DESC;


-- ============================================================================
-- LEARN: WKT Bounding-Box Filter — Spatial Queries Without a Spatial Engine
-- ============================================================================
-- Without native spatial functions, you can still filter locations by
-- geographic bounding box using the stored latitude/longitude columns.
-- This finds all POINT landmarks within a European bounding box
-- (longitude -15 to 40, latitude 35 to 72).

ASSERT ROW_COUNT = 3
ASSERT VALUE name = 'Big Ben' WHERE id = 6
SELECT id, name, region, latitude, longitude, wkt
FROM {{zone_name}}.delta_demos.locations
WHERE loc_type = 'POINT'
  AND longitude BETWEEN -15 AND 40
  AND latitude BETWEEN 35 AND 72
ORDER BY latitude DESC;


-- ============================================================================
-- EXPLORE: Window — Largest Document per File Type
-- ============================================================================
-- ROW_NUMBER() partitioned by mime_type reveals the largest file in each
-- category. Using ROW_NUMBER (not RANK) ensures exactly one winner per type
-- even when duplicate-content files share the same size.

ASSERT ROW_COUNT = 7
ASSERT VALUE name = 'marketing_assets.zip' WHERE mime_type = 'application/zip'
ASSERT VALUE size_bytes = 4567890 WHERE mime_type = 'image/png'
SELECT id, name, mime_type, size_bytes, size_rank
FROM (
    SELECT id, name, mime_type, size_bytes,
           ROW_NUMBER() OVER (PARTITION BY mime_type ORDER BY size_bytes DESC, id) AS size_rank
    FROM {{zone_name}}.delta_demos.documents
)
WHERE size_rank = 1
ORDER BY size_bytes DESC;


-- ============================================================================
-- EXPLORE: Cross-Table Join — Documents Mapped to Locations
-- ============================================================================
-- Documents are geo-tagged via location_id. An INNER JOIN with the locations
-- table reveals where each document was created or belongs, enabling
-- geographic analysis of the document inventory.

ASSERT ROW_COUNT = 6
ASSERT VALUE doc_count = 10 WHERE region = 'North America'
ASSERT VALUE doc_count = 6 WHERE region = 'Europe'
SELECT l.region,
       COUNT(*) AS doc_count,
       ROUND(SUM(d.size_bytes) / 1048576.0, 2) AS total_size_mb,
       MIN(d.name) AS example_doc
FROM {{zone_name}}.delta_demos.documents d
INNER JOIN {{zone_name}}.delta_demos.locations l ON d.location_id = l.id
GROUP BY l.region
ORDER BY doc_count DESC;


-- ============================================================================
-- LEARN: CTE — Most Accessed Documents with Location Context
-- ============================================================================
-- A CTE aggregates audit_log access counts, then joins to documents and
-- locations to build a ranked view of the most popular files with their
-- geographic context. This three-table join is typical of operational
-- dashboards combining activity, content, and spatial data.

ASSERT ROW_COUNT = 5
ASSERT VALUE access_count = 5 WHERE doc_name = 'annual_report_2024.pdf'
ASSERT VALUE location_name = 'Empire State Building' WHERE doc_name = 'annual_report_2024.pdf'
WITH access_counts AS (
    SELECT doc_id, COUNT(*) AS access_count
    FROM {{zone_name}}.delta_demos.geo_audit_log
    GROUP BY doc_id
)
SELECT ac.doc_id,
       d.name AS doc_name,
       ac.access_count,
       l.name AS location_name,
       l.region
FROM access_counts ac
INNER JOIN {{zone_name}}.delta_demos.documents d ON ac.doc_id = d.id
LEFT JOIN {{zone_name}}.delta_demos.locations l ON d.location_id = l.id
ORDER BY ac.access_count DESC, ac.doc_id
LIMIT 5;


-- ============================================================================
-- LEARN: Approximate Spatial Distance Between European Landmarks
-- ============================================================================
-- A self-join computes pairwise distances between all European POINT locations
-- using the equirectangular approximation:
--   distance_km ≈ √((Δlat×111)² + (Δlon×111×cos(avg_lat))²)
-- This is accurate enough for nearby points and demonstrates spatial analytics
-- without a dedicated geometry engine.

ASSERT ROW_COUNT = 6
ASSERT VALUE distance_km = 340.0 WHERE loc_a = 'Eiffel Tower'  AND loc_b = 'Big Ben'
ASSERT VALUE distance_km = 3552.2 WHERE loc_a = 'Big Ben' AND loc_b = 'Pyramids of Giza'
SELECT a.name AS loc_a,
       b.name AS loc_b,
       ROUND(
           SQRT(
               POWER((a.latitude - b.latitude) * 111.0, 2) +
               POWER((a.longitude - b.longitude) * 111.0 * COS(RADIANS((a.latitude + b.latitude) / 2)), 2)
           ), 1
       ) AS distance_km
FROM {{zone_name}}.delta_demos.locations a
INNER JOIN {{zone_name}}.delta_demos.locations b
    ON a.id < b.id
WHERE a.loc_type = 'POINT' AND b.loc_type = 'POINT'
  AND a.region = 'Europe' AND b.region = 'Europe'
ORDER BY distance_km;


-- ============================================================================
-- EXPLORE: Running Total — Cumulative Document Storage by Month
-- ============================================================================
-- A window function computes a running total of document storage over time.
-- This reveals growth trends and helps capacity planning — a common pattern
-- for monitoring data lake ingestion rates.

ASSERT ROW_COUNT = 7
ASSERT VALUE cumulative_docs = 27 WHERE upload_month = '2024-06'
ASSERT VALUE cumulative_mb = 51.61 WHERE upload_month = '2024-06'
SELECT upload_month,
       docs_added,
       SUM(docs_added) OVER (ORDER BY upload_month) AS cumulative_docs,
       ROUND(SUM(month_bytes) OVER (ORDER BY upload_month) / 1048576.0, 2) AS cumulative_mb
FROM (
    SELECT SUBSTRING(created_at, 1, 7) AS upload_month,
           COUNT(*) AS docs_added,
           SUM(size_bytes) AS month_bytes
    FROM {{zone_name}}.delta_demos.documents
    GROUP BY SUBSTRING(created_at, 1, 7)
)
ORDER BY upload_month;


-- ============================================================================
-- LEARN: Elevation Ranking with LAG — Gap Analysis
-- ============================================================================
-- LAG() computes the difference between consecutive ranked elevations,
-- revealing how dramatically height drops between landmarks. The gap from
-- rank 1 (Kilimanjaro at 5895m) to rank 2 is enormous compared to the
-- gradual decline among mid-range landmarks.

ASSERT ROW_COUNT = 15
ASSERT VALUE elevation_m = 5895 WHERE elev_rank = 1
ASSERT VALUE gap_from_previous = -4810 WHERE elev_rank = 2
SELECT name,
       region,
       elevation_m,
       RANK() OVER (ORDER BY elevation_m DESC) AS elev_rank,
       elevation_m - LAG(elevation_m) OVER (ORDER BY elevation_m DESC) AS gap_from_previous
FROM {{zone_name}}.delta_demos.locations
WHERE loc_type = 'POINT'
ORDER BY elevation_m DESC;


-- ============================================================================
-- EXPLORE: User Activity Pivot — Audit Log Analytics
-- ============================================================================
-- FILTER clauses pivot action types into columns per user, revealing access
-- patterns. This is a standard security/compliance pattern: which users are
-- heavy downloaders vs. editors vs. passive viewers.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_actions = 11 WHERE user_name = 'alice'
ASSERT VALUE downloads = 4 WHERE user_name = 'bob'
SELECT user_name,
       COUNT(*) FILTER (WHERE action = 'view') AS views,
       COUNT(*) FILTER (WHERE action = 'download') AS downloads,
       COUNT(*) FILTER (WHERE action = 'edit') AS edits,
       COUNT(*) AS total_actions
FROM {{zone_name}}.delta_demos.geo_audit_log
GROUP BY user_name
ORDER BY total_actions DESC, user_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting verification of document counts, spatial data, hash integrity,
-- audit log, and deduplication invariants.

-- Verify document count after DELETE
ASSERT ROW_COUNT = 27
SELECT * FROM {{zone_name}}.delta_demos.documents;

-- Verify location count
ASSERT ROW_COUNT = 25
SELECT * FROM {{zone_name}}.delta_demos.locations;

-- Verify audit log count
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.geo_audit_log;

-- Verify all hashes are 64 characters (SHA-256 hex)
ASSERT VALUE valid_hash_count = 27
SELECT COUNT(*) AS valid_hash_count FROM {{zone_name}}.delta_demos.documents WHERE LENGTH(content_hash) = 64;

-- Verify 5 duplicate hash groups exist
ASSERT VALUE dup_groups = 5
SELECT COUNT(*) AS dup_groups FROM (SELECT content_hash FROM {{zone_name}}.delta_demos.documents GROUP BY content_hash HAVING COUNT(*) > 1);

-- Verify geo-tagged document count
ASSERT VALUE geo_tagged = 23
SELECT COUNT(*) AS geo_tagged FROM {{zone_name}}.delta_demos.documents WHERE location_id IS NOT NULL;

-- Verify 6 distinct regions
ASSERT VALUE region_count = 6
SELECT COUNT(DISTINCT region) AS region_count FROM {{zone_name}}.delta_demos.locations;

-- Verify geometry type distribution
ASSERT VALUE point_count = 15
SELECT COUNT(*) AS point_count FROM {{zone_name}}.delta_demos.locations WHERE loc_type = 'POINT';

ASSERT VALUE poly_line_count = 10
SELECT COUNT(*) AS poly_line_count FROM {{zone_name}}.delta_demos.locations WHERE loc_type IN ('POLYGON', 'LINESTRING');

-- Verify audit log action types
ASSERT VALUE action_types = 3
SELECT COUNT(DISTINCT action) AS action_types FROM {{zone_name}}.delta_demos.geo_audit_log;
