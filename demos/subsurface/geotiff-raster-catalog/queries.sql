-- ============================================================================
-- Aerial Survey Raster Catalogue - Incremental Load and Verification
-- ============================================================================
-- Six tiles delivered over two days:
--
--   2026-03-11  ortho_n01e01, ortho_n01e02, ortho_n02e01
--   2026-03-12  ortho_n02e02, ortho_n03e01, dem_n01e01 (BigTIFF)
--
-- Eighteen image directories in total, because each tile carries a full
-- resolution level and two overviews. Every value below was decoded from the
-- tag directories by a second, independent TIFF reader before the engine saw
-- them.
--
-- Two faults are planted in the second delivery and the catalogue finds both:
--
--   1. ortho_n02e02 was delivered in UTM zone 14N while the rest of the
--      survey is 13N. Its coordinates are perfectly valid numbers, and they
--      put the tile several hundred kilometres east of where it belongs.
--   2. ortho_n03e01 is at half the resolution of the others. A file listing
--      cannot see that; the tag directory can.
--
-- None of this reads a pixel. The whole catalogue is built from tag
-- directories, which is why it would cost the same if these tiles were the
-- gigabytes their dimensions imply rather than the bytes they are.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.raster_survey.raster_tiles
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. ONE ROW PER IMAGE DIRECTORY
-- ============================================================================
-- Six files, eighteen rows. A reader that followed only the first directory
-- in each file would return six and look perfectly reasonable.

ASSERT ROW_COUNT = 18
SELECT *
FROM {{zone_name}}.raster_survey.raster_tiles;


-- ============================================================================
-- 3. THE OVERVIEW PYRAMID
-- ============================================================================
-- Three directories per tile: full resolution, then each halving. Both the
-- classic TIFFs and the BigTIFF chain the same way, which is the point of
-- asserting it across all six rather than one.

ASSERT ROW_COUNT = 6
ASSERT VALUE directories = 3 WHERE df_file_name = '2026-03-11_ortho_n01e01.tif'
ASSERT VALUE directories = 3 WHERE df_file_name = '2026-03-12_dem_n01e01.tif'
ASSERT VALUE full_width = 20000 WHERE df_file_name = '2026-03-11_ortho_n01e01.tif'
ASSERT VALUE smallest_width = 5000 WHERE df_file_name = '2026-03-11_ortho_n01e01.tif'
ASSERT VALUE full_width = 40000 WHERE df_file_name = '2026-03-12_dem_n01e01.tif'
ASSERT VALUE smallest_width = 10000 WHERE df_file_name = '2026-03-12_dem_n01e01.tif'
SELECT df_file_name,
       COUNT(*)      AS directories,
       MAX(width)    AS full_width,
       MIN(width)    AS smallest_width
FROM {{zone_name}}.raster_survey.raster_tiles
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. EACH OVERVIEW HALVES THE RASTER AND DOUBLES THE GROUND SAMPLE
-- ============================================================================
-- The relationship that makes a pyramid a pyramid. Directory 0 is full
-- resolution at 0.25 m, directory 1 is half the pixels at 0.5 m, directory 2
-- is a quarter at 1.0 m, and the ground each covers is identical.

ASSERT ROW_COUNT = 3
ASSERT VALUE width = 20000 WHERE directory_index = 0
ASSERT VALUE pixel_scale_x = 0.25 WHERE directory_index = 0
ASSERT VALUE width = 10000 WHERE directory_index = 1
ASSERT VALUE pixel_scale_x = 0.5 WHERE directory_index = 1
ASSERT VALUE width = 5000 WHERE directory_index = 2
ASSERT VALUE pixel_scale_x = 1.0 WHERE directory_index = 2
ASSERT VALUE ground_m = 5000 WHERE directory_index = 0
ASSERT VALUE ground_m = 5000 WHERE directory_index = 1
ASSERT VALUE ground_m = 5000 WHERE directory_index = 2
SELECT directory_index,
       width,
       pixel_scale_x,
       CAST(ROUND(width * pixel_scale_x) AS BIGINT) AS ground_m
FROM {{zone_name}}.raster_survey.raster_tiles
WHERE df_file_name = '2026-03-11_ortho_n01e01.tif'
ORDER BY directory_index;


-- ============================================================================
-- 5. LOAD THE 11 MARCH DELIVERY
-- ============================================================================

INSERT INTO {{zone_name}}.raster_survey.raster_catalog
SELECT SUBSTR(t.df_file_name, 12, LENGTH(t.df_file_name) - 15) AS tile,
       '2026-03-11'                                            AS delivered_on,
       t.df_file_name                                          AS source_file,
       t.directory_index,
       t.directory_index = 0                                   AS is_full_resolution,
       t.width,
       t.height,
       t.bands,
       t.bits_per_sample,
       t.pixel_scale_x                                         AS pixel_scale_m,
       t.epsg,
       t.origin_x,
       t.origin_y,
       t.width * t.pixel_scale_x                               AS ground_width_m,
       t.height * t.pixel_scale_y                              AS ground_height_m
FROM {{zone_name}}.raster_survey.raster_tiles t
WHERE t.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.raster_survey.raster_catalog c
      WHERE c.source_file = t.df_file_name
  );


-- ============================================================================
-- 6. THE FIRST DELIVERY IS CATALOGUED
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE directories = 9
ASSERT VALUE tiles = 3
ASSERT VALUE full_resolution = 3
ASSERT VALUE zones = 1
SELECT COUNT(*)                                          AS directories,
       COUNT(DISTINCT source_file)                       AS tiles,
       COUNT(*) FILTER (WHERE is_full_resolution)        AS full_resolution,
       COUNT(DISTINCT epsg)                              AS zones
FROM {{zone_name}}.raster_survey.raster_catalog
WHERE delivered_on = '2026-03-11';


-- ============================================================================
-- 7. THE SAME DELIVERY AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.raster_survey.raster_catalog
SELECT SUBSTR(t.df_file_name, 12, LENGTH(t.df_file_name) - 15) AS tile,
       '2026-03-11'                                            AS delivered_on,
       t.df_file_name                                          AS source_file,
       t.directory_index,
       t.directory_index = 0                                   AS is_full_resolution,
       t.width,
       t.height,
       t.bands,
       t.bits_per_sample,
       t.pixel_scale_x                                         AS pixel_scale_m,
       t.epsg,
       t.origin_x,
       t.origin_y,
       t.width * t.pixel_scale_x                               AS ground_width_m,
       t.height * t.pixel_scale_y                              AS ground_height_m
FROM {{zone_name}}.raster_survey.raster_tiles t
WHERE t.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.raster_survey.raster_catalog c
      WHERE c.source_file = t.df_file_name
  );


-- ============================================================================
-- 8. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE directories = 9
ASSERT VALUE tiles = 3
SELECT COUNT(*)                    AS directories,
       COUNT(DISTINCT source_file) AS tiles
FROM {{zone_name}}.raster_survey.raster_catalog
WHERE delivered_on = '2026-03-11';


-- ============================================================================
-- 9. LOAD THE 12 MARCH DELIVERY
-- ============================================================================

INSERT INTO {{zone_name}}.raster_survey.raster_catalog
SELECT SUBSTR(t.df_file_name, 12, LENGTH(t.df_file_name) - 15) AS tile,
       '2026-03-12'                                            AS delivered_on,
       t.df_file_name                                          AS source_file,
       t.directory_index,
       t.directory_index = 0                                   AS is_full_resolution,
       t.width,
       t.height,
       t.bands,
       t.bits_per_sample,
       t.pixel_scale_x                                         AS pixel_scale_m,
       t.epsg,
       t.origin_x,
       t.origin_y,
       t.width * t.pixel_scale_x                               AS ground_width_m,
       t.height * t.pixel_scale_y                              AS ground_height_m
FROM {{zone_name}}.raster_survey.raster_tiles t
WHERE t.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.raster_survey.raster_catalog c
      WHERE c.source_file = t.df_file_name
  );


-- ============================================================================
-- 10. THE COORDINATE SYSTEM OUTLIER
-- ============================================================================
-- The finding. Fifteen of the eighteen directories are in UTM zone 13N and
-- three are in 14N, and those three are all the same tile. Its coordinates
-- are perfectly valid numbers, which is precisely why nothing downstream
-- would have questioned them.

ASSERT ROW_COUNT = 2
ASSERT VALUE directories = 15 WHERE epsg = 32613
ASSERT VALUE tiles = 5 WHERE epsg = 32613
ASSERT VALUE directories = 3 WHERE epsg = 32614
ASSERT VALUE tiles = 1 WHERE epsg = 32614
SELECT epsg,
       COUNT(*)                    AS directories,
       COUNT(DISTINCT source_file) AS tiles
FROM {{zone_name}}.raster_survey.raster_catalog
GROUP BY epsg
ORDER BY epsg;


-- ============================================================================
-- 11. WHICH TILE IS IN THE WRONG ZONE
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE tile = 'ortho_n02e02'
ASSERT VALUE epsg = 32614
ASSERT VALUE delivered_on = '2026-03-12'
SELECT tile,
       MIN(epsg)          AS epsg,
       MIN(delivered_on)  AS delivered_on,
       COUNT(*)           AS directories
FROM {{zone_name}}.raster_survey.raster_catalog
WHERE epsg <> 32613
GROUP BY tile
ORDER BY tile;


-- ============================================================================
-- 12. THE RESOLUTION OUTLIER
-- ============================================================================
-- Of the six tiles delivered, five were flown at 0.25 m and one at 0.50 m.
-- Mosaicking them without noticing produces a seam nobody can explain later.

ASSERT ROW_COUNT = 2
ASSERT VALUE tiles = 5 WHERE pixel_scale_m = 0.25
ASSERT VALUE tiles = 1 WHERE pixel_scale_m = 0.5
SELECT pixel_scale_m,
       COUNT(*) AS tiles
FROM {{zone_name}}.raster_survey.raster_catalog
WHERE is_full_resolution
GROUP BY pixel_scale_m
ORDER BY pixel_scale_m;


-- ============================================================================
-- 13. WHAT THE SURVEY ACTUALLY COVERS
-- ============================================================================
-- Restricted to full resolution and the correct zone, because mixing in the
-- overviews would count the same ground three times and mixing in the wrong
-- zone would stretch the extent across a zone boundary.
--
-- 2900 megapixels of imagery across five tiles, which is the number that
-- decides whether the processing cluster is big enough.

ASSERT ROW_COUNT = 1
ASSERT VALUE tiles = 5
ASSERT VALUE megapixels = 2900
ASSERT VALUE west_m = 512000
ASSERT VALUE north_m = 3548000
SELECT COUNT(*)                                                AS tiles,
       CAST(ROUND(SUM(width * height) / 1000000.0) AS BIGINT)  AS megapixels,
       CAST(ROUND(MIN(origin_x)) AS BIGINT)                    AS west_m,
       CAST(ROUND(MAX(origin_y)) AS BIGINT)                    AS north_m
FROM {{zone_name}}.raster_survey.raster_catalog
WHERE is_full_resolution
  AND epsg = 32613;


-- ============================================================================
-- 14. THE ELEVATION MODEL IS NOT AN ORTHOPHOTO
-- ============================================================================
-- One band of 32-bit samples rather than three of 8-bit, which is what an
-- elevation model is, and it came as a BigTIFF because a 40000 by 40000
-- raster of 32-bit floats is past what a classic TIFF can address.

ASSERT ROW_COUNT = 2
ASSERT VALUE tiles = 5 WHERE bands = 3
ASSERT VALUE bits_per_sample = 8 WHERE bands = 3
ASSERT VALUE tiles = 1 WHERE bands = 1
ASSERT VALUE bits_per_sample = 32 WHERE bands = 1
SELECT bands,
       MIN(bits_per_sample) AS bits_per_sample,
       COUNT(*)             AS tiles
FROM {{zone_name}}.raster_survey.raster_catalog
WHERE is_full_resolution
GROUP BY bands
ORDER BY bands;


-- ============================================================================
-- 15. EVERY TILE CATALOGUED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT c.source_file, c.catalogued, t.landed
FROM (
    SELECT source_file, COUNT(*) AS catalogued
    FROM {{zone_name}}.raster_survey.raster_catalog
    GROUP BY source_file
) c
JOIN (
    SELECT df_file_name, COUNT(*) AS landed
    FROM {{zone_name}}.raster_survey.raster_tiles
    GROUP BY df_file_name
) t
  ON t.df_file_name = c.source_file
WHERE c.catalogued <> t.landed;


-- ============================================================================
-- 16. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT t.df_file_name
FROM {{zone_name}}.raster_survey.raster_tiles t
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.raster_survey.raster_catalog c
    WHERE c.source_file = t.df_file_name
);


-- ============================================================================
-- 17. THE STATE AFTER THE FIRST DELIVERY, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 9
SELECT *
FROM {{zone_name}}.raster_survey.raster_catalog VERSION AS OF 1;


-- ============================================================================
-- 18. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.raster_survey.raster_catalog;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The catalogue as a GIS lead would sign it off: one row per tile at full
-- resolution, with what it covers and whether it belongs to the survey.

ASSERT ROW_COUNT = 6
ASSERT VALUE epsg = 32613 WHERE tile = 'ortho_n01e01'
ASSERT VALUE pixel_scale_m = 0.25 WHERE tile = 'ortho_n01e01'
ASSERT VALUE ground_width_m = 5000 WHERE tile = 'ortho_n01e01'
ASSERT VALUE in_survey_zone = true WHERE tile = 'ortho_n01e01'
ASSERT VALUE epsg = 32614 WHERE tile = 'ortho_n02e02'
ASSERT VALUE in_survey_zone = false WHERE tile = 'ortho_n02e02'
ASSERT VALUE pixel_scale_m = 0.5 WHERE tile = 'ortho_n03e01'
ASSERT VALUE ground_width_m = 5000 WHERE tile = 'ortho_n03e01'
ASSERT VALUE bands = 1 WHERE tile = 'dem_n01e01'
ASSERT VALUE ground_width_m = 10000 WHERE tile = 'dem_n01e01'
SELECT tile,
       MIN(delivered_on)                            AS delivered_on,
       MAX(width)                                   AS full_width,
       MIN(bands)                                   AS bands,
       MIN(bits_per_sample)                         AS bits_per_sample,
       MIN(pixel_scale_m)                           AS pixel_scale_m,
       MIN(epsg)                                    AS epsg,
       MIN(epsg) = 32613                            AS in_survey_zone,
       CAST(ROUND(MAX(ground_width_m)) AS BIGINT)   AS ground_width_m
FROM {{zone_name}}.raster_survey.raster_catalog
WHERE is_full_resolution
GROUP BY tile
ORDER BY tile;
