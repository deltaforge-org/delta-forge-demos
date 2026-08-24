-- ============================================================================
-- Survey Navigation Database - Incremental Load and Verification
-- ============================================================================
-- Four real Volve ST0299 navigation files, delivered in two batches of two:
--
--   2026-03-11  ST0299-CMP-05002, ST0299-CMP-05003
--   2026-03-12  ST0299-CMP-05004, ST0299-CMP-05005
--
-- 95 position records each, 380 in total. Every value below was decoded from
-- the files by a second, independent P1/90 reader before the engine saw them.
--
-- Two properties of these files broke the reader when it first met them, and
-- both are asserted here so they stay fixed:
--
--   1. Every position record is a `C` record. A CMP deliverable has no `S` or
--      `R` records at all, so treating `C` as a comment returned zero rows
--      for the entire class of file, silently.
--   2. The longitudes are a single degree east, and P1/90 right justifies the
--      degrees in their field: the record carries `  15614.47E`. A parser
--      taking the first three characters as the degrees reads 156 and puts a
--      North Sea survey in central Siberia.
--
-- Neither would raise an error. The first returns an empty table and the
-- second returns a full one that is wrong, which is worse.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.survey_navigation.navigation_lines
    PATH '{{data_path}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. ALL FOUR LINES, READ IN PLACE
-- ============================================================================
-- 95 positions per line, 380 in total. The 40 header records per file
-- describe the survey and are not positions, so they are not rows.

ASSERT ROW_COUNT = 380
SELECT *
FROM {{zone_name}}.survey_navigation.navigation_lines;


-- ============================================================================
-- 3. EVERY POSITION IS A CMP RECORD
-- ============================================================================
-- Not one S or R record in the whole delivery. This is the count that is zero
-- when `C` is read as a comment.

ASSERT ROW_COUNT = 1
ASSERT VALUE record_type = 'C'
ASSERT VALUE positions = 380
SELECT record_type,
       COUNT(*) AS positions
FROM {{zone_name}}.survey_navigation.navigation_lines
GROUP BY record_type;


-- ============================================================================
-- 4. THE COORDINATES ARE IN THE NORTH SEA
-- ============================================================================
-- Volve sits at about 58.44 degrees north, 1.89 degrees east, in UTM zone
-- 31N. Asserted as counts inside a bounding box rather than as floating point
-- equality, so the check is exact without being brittle.
--
-- Every one of the 380 positions falls inside the box. Before the packed
-- longitude was parsed from the right, none of them did.

ASSERT ROW_COUNT = 1
ASSERT VALUE positions = 380
ASSERT VALUE in_the_north_sea = 380
ASSERT VALUE east_of_ten_degrees = 0
SELECT COUNT(*)                                                     AS positions,
       COUNT(*) FILTER (WHERE latitude BETWEEN 58.4 AND 58.5
                          AND longitude BETWEEN 1.8 AND 2.0)        AS in_the_north_sea,
       COUNT(*) FILTER (WHERE longitude > 10.0)                     AS east_of_ten_degrees
FROM {{zone_name}}.survey_navigation.navigation_lines;


-- ============================================================================
-- 5. LOAD THE 11 MARCH BATCH
-- ============================================================================

INSERT INTO {{zone_name}}.survey_navigation.shot_point_index
SELECT n.line_name    AS line,
       '2026-03-11'   AS delivered_on,
       n.df_file_name AS source_file,
       n.record_type,
       n.point_number,
       n.latitude,
       n.longitude,
       n.easting,
       n.northing
FROM {{zone_name}}.survey_navigation.navigation_lines n
WHERE n.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.survey_navigation.shot_point_index s
      WHERE s.source_file = n.df_file_name
  );


-- ============================================================================
-- 6. THE FIRST BATCH LANDED
-- ============================================================================
-- The line name comes out of the record, not out of the file name, which is
-- what makes it trustworthy: a file renamed in transit still says which line
-- it holds.

ASSERT ROW_COUNT = 2
ASSERT VALUE positions = 95 WHERE line = 'ST0299-05002'
ASSERT VALUE first_point = 1001 WHERE line = 'ST0299-05002'
ASSERT VALUE last_point = 1469 WHERE line = 'ST0299-05002'
ASSERT VALUE positions = 95 WHERE line = 'ST0299-05003'
ASSERT VALUE first_point = 1001 WHERE line = 'ST0299-05003'
ASSERT VALUE last_point = 1468 WHERE line = 'ST0299-05003'
SELECT line,
       COUNT(*)             AS positions,
       MIN(point_number)    AS first_point,
       MAX(point_number)    AS last_point
FROM {{zone_name}}.survey_navigation.shot_point_index
GROUP BY line
ORDER BY line;


-- ============================================================================
-- 7. THE SAME BATCH AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.survey_navigation.shot_point_index
SELECT n.line_name    AS line,
       '2026-03-11'   AS delivered_on,
       n.df_file_name AS source_file,
       n.record_type,
       n.point_number,
       n.latitude,
       n.longitude,
       n.easting,
       n.northing
FROM {{zone_name}}.survey_navigation.navigation_lines n
WHERE n.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.survey_navigation.shot_point_index s
      WHERE s.source_file = n.df_file_name
  );


-- ============================================================================
-- 8. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE positions = 190
ASSERT VALUE files = 2
SELECT COUNT(*)                    AS positions,
       COUNT(DISTINCT source_file) AS files
FROM {{zone_name}}.survey_navigation.shot_point_index
WHERE delivered_on = '2026-03-11';


-- ============================================================================
-- 9. LOAD THE 12 MARCH BATCH
-- ============================================================================

INSERT INTO {{zone_name}}.survey_navigation.shot_point_index
SELECT n.line_name    AS line,
       '2026-03-12'   AS delivered_on,
       n.df_file_name AS source_file,
       n.record_type,
       n.point_number,
       n.latitude,
       n.longitude,
       n.easting,
       n.northing
FROM {{zone_name}}.survey_navigation.navigation_lines n
WHERE n.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.survey_navigation.shot_point_index s
      WHERE s.source_file = n.df_file_name
  );


-- ============================================================================
-- 10. THE WHOLE SURVEY
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE positions = 190 WHERE delivered_on = '2026-03-11'
ASSERT VALUE lines = 2 WHERE delivered_on = '2026-03-11'
ASSERT VALUE positions = 190 WHERE delivered_on = '2026-03-12'
ASSERT VALUE lines = 2 WHERE delivered_on = '2026-03-12'
SELECT delivered_on,
       COUNT(DISTINCT line)        AS lines,
       COUNT(DISTINCT source_file) AS files,
       COUNT(*)                    AS positions
FROM {{zone_name}}.survey_navigation.shot_point_index
GROUP BY delivered_on
ORDER BY delivered_on;


-- ============================================================================
-- 11. THE SURVEY EXTENT, LINE BY LINE
-- ============================================================================
-- Grid coordinates rounded to the metre, which is exact and reads as a map
-- reference. The four lines step north as their numbers increase, which is
-- what a 2D grid over a field looks like.

ASSERT ROW_COUNT = 4
ASSERT VALUE west_m = 432081 WHERE line = 'ST0299-05002'
ASSERT VALUE east_m = 437934 WHERE line = 'ST0299-05002'
ASSERT VALUE south_m = 6476778 WHERE line = 'ST0299-05002'
ASSERT VALUE north_m = 6476867 WHERE line = 'ST0299-05002'
ASSERT VALUE west_m = 432103 WHERE line = 'ST0299-05003'
ASSERT VALUE south_m = 6477276 WHERE line = 'ST0299-05003'
ASSERT VALUE west_m = 432651 WHERE line = 'ST0299-05004'
ASSERT VALUE south_m = 6477772 WHERE line = 'ST0299-05004'
ASSERT VALUE west_m = 432642 WHERE line = 'ST0299-05005'
ASSERT VALUE east_m = 438480 WHERE line = 'ST0299-05005'
ASSERT VALUE south_m = 6478022 WHERE line = 'ST0299-05005'
ASSERT VALUE north_m = 6478114 WHERE line = 'ST0299-05005'
SELECT line,
       COUNT(*)                              AS positions,
       CAST(ROUND(MIN(easting)) AS BIGINT)   AS west_m,
       CAST(ROUND(MAX(easting)) AS BIGINT)   AS east_m,
       CAST(ROUND(MIN(northing)) AS BIGINT)  AS south_m,
       CAST(ROUND(MAX(northing)) AS BIGINT)  AS north_m
FROM {{zone_name}}.survey_navigation.shot_point_index
GROUP BY line
ORDER BY line;


-- ============================================================================
-- 12. THE TWO COORDINATE SYSTEMS AGREE
-- ============================================================================
-- Each record carries the same position twice, once geographic and once as a
-- UTM zone 31N grid reference. They were written by the navigation system
-- independently, so agreeing is evidence both were decoded correctly: the
-- packed sexagesimal and the plain decimal have no failure mode in common.
--
-- Zone 31N's central meridian is 3 degrees east, so a point at 1.9 degrees
-- east sits west of it and its easting is below the 500000 m false easting.

ASSERT ROW_COUNT = 1
ASSERT VALUE positions = 380
ASSERT VALUE west_of_central_meridian = 380
ASSERT VALUE consistent = 380
SELECT COUNT(*)                                                  AS positions,
       COUNT(*) FILTER (WHERE easting < 500000)                  AS west_of_central_meridian,
       COUNT(*) FILTER (WHERE longitude < 3.0 AND easting < 500000
                          AND latitude > 0 AND northing > 0)     AS consistent
FROM {{zone_name}}.survey_navigation.shot_point_index;


-- ============================================================================
-- 13. EVERY FILE LANDED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT s.source_file, s.indexed, n.landed
FROM (
    SELECT source_file, COUNT(*) AS indexed
    FROM {{zone_name}}.survey_navigation.shot_point_index
    GROUP BY source_file
) s
JOIN (
    SELECT df_file_name, COUNT(*) AS landed
    FROM {{zone_name}}.survey_navigation.navigation_lines
    GROUP BY df_file_name
) n
  ON n.df_file_name = s.source_file
WHERE s.indexed <> n.landed;


-- ============================================================================
-- 14. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT n.df_file_name
FROM {{zone_name}}.survey_navigation.navigation_lines n
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.survey_navigation.shot_point_index s
    WHERE s.source_file = n.df_file_name
);


-- ============================================================================
-- 15. THE STATE AFTER THE FIRST BATCH, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 190
SELECT *
FROM {{zone_name}}.survey_navigation.shot_point_index VERSION AS OF 1;


-- ============================================================================
-- 16. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.survey_navigation.shot_point_index;


-- ============================================================================
-- 17. WHICH LINES CROSS A PROSPECT
-- ============================================================================
-- The question the shot-point database exists to answer. A box over the
-- eastern part of the survey, and the index says which lines have shot points
-- inside it and which point numbers to pull.

ASSERT ROW_COUNT = 4
ASSERT VALUE points_in_box = 32 WHERE line = 'ST0299-05002'
ASSERT VALUE from_point = 1001 WHERE line = 'ST0299-05002'
ASSERT VALUE to_point = 1155 WHERE line = 'ST0299-05002'
ASSERT VALUE points_in_box = 32 WHERE line = 'ST0299-05003'
ASSERT VALUE points_in_box = 32 WHERE line = 'ST0299-05004'
ASSERT VALUE from_point = 1270 WHERE line = 'ST0299-05004'
ASSERT VALUE to_point = 1425 WHERE line = 'ST0299-05004'
ASSERT VALUE points_in_box = 32 WHERE line = 'ST0299-05005'
SELECT line,
       COUNT(*)          AS points_in_box,
       MIN(point_number) AS from_point,
       MAX(point_number) AS to_point
FROM {{zone_name}}.survey_navigation.shot_point_index
WHERE easting BETWEEN 436000 AND 438000
GROUP BY line
ORDER BY line;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE positions = 95 WHERE line = 'ST0299-05002'
ASSERT VALUE delivered_on = '2026-03-11' WHERE line = 'ST0299-05002'
ASSERT VALUE in_the_north_sea = 95 WHERE line = 'ST0299-05002'
ASSERT VALUE positions = 95 WHERE line = 'ST0299-05003'
ASSERT VALUE delivered_on = '2026-03-11' WHERE line = 'ST0299-05003'
ASSERT VALUE positions = 95 WHERE line = 'ST0299-05004'
ASSERT VALUE delivered_on = '2026-03-12' WHERE line = 'ST0299-05004'
ASSERT VALUE positions = 95 WHERE line = 'ST0299-05005'
ASSERT VALUE delivered_on = '2026-03-12' WHERE line = 'ST0299-05005'
ASSERT VALUE in_the_north_sea = 95 WHERE line = 'ST0299-05005'
SELECT line,
       MIN(delivered_on)                         AS delivered_on,
       COUNT(*)                                  AS positions,
       COUNT(*) FILTER (WHERE latitude BETWEEN 58.4 AND 58.5
                          AND longitude BETWEEN 1.8 AND 2.0) AS in_the_north_sea,
       CAST(ROUND(MIN(easting)) AS BIGINT)       AS west_m,
       CAST(ROUND(MAX(northing)) AS BIGINT)      AS north_m
FROM {{zone_name}}.survey_navigation.shot_point_index
GROUP BY line
ORDER BY line;
