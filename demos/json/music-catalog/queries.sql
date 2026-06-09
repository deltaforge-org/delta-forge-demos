-- ============================================================================
-- JSON Music Catalog — Verification Queries
-- ============================================================================
-- Each query verifies a specific JSON feature: nested flattening, explode,
-- json_paths, column_mappings, multi-file reading, and array handling.
-- ============================================================================


-- ============================================================================
-- 1. EXPLODED ROW COUNT — 3 files should produce 3,503 track rows
-- ============================================================================
-- catalog_rock: 2,096 tracks + catalog_jazz: 370 + catalog_pop: 1,037 = 3,503

ASSERT ROW_COUNT = 3503
SELECT *
FROM {{zone_name}}.json_demos.album_tracks;


-- ============================================================================
-- 2. BROWSE TRACKS — See the exploded data with friendly column names
-- ============================================================================

ASSERT ROW_COUNT = 15
ASSERT VALUE name = 'For Those About To Rock We Salute You' WHERE details_track_id = 1
ASSERT VALUE vendor_name = 'AC/DC' WHERE details_track_id = 1
ASSERT VALUE details_name = 'For Those About To Rock (We Salute You)' WHERE details_track_id = 1
SELECT details_track_id, name, vendor_name, details_name, details_composer,
       details_genre_id, details_milliseconds, details_unit_price
FROM {{zone_name}}.json_demos.album_tracks
ORDER BY id, details_track_id
LIMIT 15;


-- ============================================================================
-- 3. ALBUM SUMMARY COUNT — 347 albums across 3 files
-- ============================================================================

ASSERT ROW_COUNT = 347
SELECT *
FROM {{zone_name}}.json_demos.album_summary;


-- ============================================================================
-- 4. BROWSE ALBUM SUMMARY — One row per album with track count
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE name = 'For Those About To Rock We Salute You' WHERE id = 1
ASSERT VALUE status = 'available' WHERE id = 1
SELECT id, name, sku, status, price, vendor, details
FROM {{zone_name}}.json_demos.album_summary
ORDER BY id
LIMIT 10;


-- ============================================================================
-- 5. VENDOR FLATTENING — Nested $.vendor.id and $.vendor.name extracted
-- ============================================================================
-- In album_tracks, the vendor object is flattened to separate columns.

ASSERT ROW_COUNT = 10
ASSERT VALUE vendor_name = 'AC/DC' WHERE vendor_id = 1
SELECT DISTINCT vendor_id, vendor_name
FROM {{zone_name}}.json_demos.album_tracks
ORDER BY vendor_name
LIMIT 10;


-- ============================================================================
-- 6. JSON PATHS — Vendor kept as JSON blob in album_summary
-- ============================================================================
-- In album_summary, $.vendor is preserved as a JSON string, not flattened.

ASSERT ROW_COUNT = 1
ASSERT VALUE name = 'For Those About To Rock We Salute You'
SELECT name, vendor
FROM {{zone_name}}.json_demos.album_summary
WHERE id = 1;


-- ============================================================================
-- 7. MULTI-FILE VERIFICATION — Tracks per source catalog file
-- ============================================================================
-- file_metadata df_file_name identifies which catalog each row came from.

ASSERT ROW_COUNT = 3
ASSERT VALUE track_count = 370 WHERE df_file_name LIKE '%catalog_jazz%'
ASSERT VALUE track_count = 1037 WHERE df_file_name LIKE '%catalog_pop%'
ASSERT VALUE track_count = 2096 WHERE df_file_name LIKE '%catalog_rock%'
SELECT df_file_name, COUNT(*) AS track_count
FROM {{zone_name}}.json_demos.album_tracks
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 8. GENRE DISTRIBUTION — Track count by genre across all catalogs
-- ============================================================================

ASSERT ROW_COUNT = 25
ASSERT VALUE track_count = 1297 WHERE details_genre_id = 1
SELECT details_genre_id, COUNT(*) AS track_count
FROM {{zone_name}}.json_demos.album_tracks
GROUP BY details_genre_id
ORDER BY track_count DESC;


-- ============================================================================
-- 9. TOP COMPOSERS — Most prolific composers by track count
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE details_composer = 'Steve Harris' WHERE tracks_composed >= 80
ASSERT VALUE tracks_composed >= 80 WHERE details_composer = 'Steve Harris'
SELECT details_composer, COUNT(*) AS tracks_composed
FROM {{zone_name}}.json_demos.album_tracks
WHERE details_composer IS NOT NULL
GROUP BY details_composer
ORDER BY tracks_composed DESC
LIMIT 10;


-- ============================================================================
-- 10. LONGEST TRACKS — Top 10 tracks by duration
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE minutes >= 48.8
SELECT details_name, name, vendor_name,
       ROUND(CAST(details_milliseconds AS DOUBLE) / 60000.0, 1) AS minutes
FROM {{zone_name}}.json_demos.album_tracks
ORDER BY CAST(details_milliseconds AS DOUBLE) DESC
LIMIT 10;


-- ============================================================================
-- 11. REVENUE BY ALBUM — Total track revenue per album
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE name = 'Greatest Hits' WHERE vendor_name = 'Lenny Kravitz'
ASSERT VALUE tracks = 57 WHERE vendor_name = 'Lenny Kravitz'
ASSERT VALUE total_revenue = 56.43 WHERE vendor_name = 'Lenny Kravitz'
SELECT name, vendor_name,
       COUNT(*) AS tracks,
       ROUND(SUM(CAST(details_unit_price AS DOUBLE)), 2) AS total_revenue
FROM {{zone_name}}.json_demos.album_tracks
GROUP BY name, vendor_name
ORDER BY total_revenue DESC
LIMIT 10;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total counts, flattening, multi-file, and
-- column-mapping invariants. Every sub-check must return 'PASS'.

ASSERT ROW_COUNT = 8
ASSERT VALUE result = 'PASS'
SELECT check_name, result FROM (

    -- Check 1: Exploded track count = 3,503
    SELECT 'track_count_3503' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.album_tracks) = 3503
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Album summary count = 347
    SELECT 'album_count_347' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.album_summary) = 347
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Vendor flattened in tracks (vendor_name populated)
    SELECT 'vendor_flattened' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.album_tracks WHERE vendor_name IS NOT NULL) = 3503
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: json_paths — vendor is JSON string in summary
    SELECT 'vendor_json_blob' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.album_summary WHERE vendor IS NOT NULL) = 347
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Column mappings — details_name exists with data
    SELECT 'column_mapping_details_name' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.album_tracks WHERE details_name IS NOT NULL) > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Multi-file — 3 distinct source files
    SELECT 'multi_file_3_sources' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.json_demos.album_tracks) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Spot check — AC/DC album "For Those About To Rock" exists
    SELECT 'spot_check_acdc' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json_demos.album_tracks
               WHERE name = 'For Those About To Rock We Salute You' AND vendor_name = 'AC/DC'
           ) > 0 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: All tracks have positive duration
    SELECT 'positive_duration' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.album_tracks WHERE CAST(details_milliseconds AS DOUBLE) <= 0) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
