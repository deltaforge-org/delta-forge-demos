-- ============================================================================
-- Drilling Survey Anti-Collision - Incremental Load and Verification
-- ============================================================================
-- Three definitive surveys from one platform template, delivered over two
-- days:
--
--   2026-03-11  15/9-F-11    40 stations, md 400 to 1570 m, builds to 62 deg
--               15/9-F-11 A  32 stations, md 490 to 1420 m, builds to 74 deg
--   2026-03-12  15/9-F-12    48 stations, md 360 to 1770 m, builds to 48 deg
--
-- 120 survey stations in total. Every value below was computed from the
-- documents by an independent parser before the engine saw them.
--
-- The trajectories are minimum-curvature, so the true vertical depths and the
-- north/east offsets are internally consistent rather than decorative. That
-- matters for the last query: a reader that mangled a station would produce a
-- well path that does not close, and the anti-collision answer would be
-- wrong in a way no row count would catch.
--
-- WITSML is read through the XML engine under a curated profile, so the
-- columns are the document's own element paths. A station's measured depth is
-- `wells_well_trajectory_trajectory_station_md`: unambiguous, and exactly why
-- the curated table renames them.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- The USING clause is the interesting part: WITSML, not XML. Detection is on
-- the document's namespace and elements rather than its extension, because
-- these dialects all ship as plain .xml and a document that merely mentions
-- the word is not one.

DISCOVER {{zone_name}}.drilling.survey_documents
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. ONE ROW PER SURVEY STATION
-- ============================================================================
-- The profile explodes //trajectoryStation, so 40 + 32 + 48 = 120 rows come
-- out of three documents that each hold one well. Without the explode there
-- would be three rows and the stations would be numbered columns.

ASSERT ROW_COUNT = 120
SELECT *
FROM {{zone_name}}.drilling.survey_documents;


-- ============================================================================
-- 3. THE WELL FIELDS REPEAT DOWN EVERY STATION
-- ============================================================================
-- The well is the row element and the station is what explodes, so the well's
-- own name, field and operator ride on all of its stations. That is what
-- makes the table groupable by well with no join.

ASSERT ROW_COUNT = 3
ASSERT VALUE stations = 40 WHERE wells_well_name = '15/9-F-11'
ASSERT VALUE stations = 32 WHERE wells_well_name = '15/9-F-11 A'
ASSERT VALUE stations = 48 WHERE wells_well_name = '15/9-F-12'
ASSERT VALUE wells_well_field = 'VOLVE-DEMO' WHERE wells_well_name = '15/9-F-11'
ASSERT VALUE wells_well_operator = 'DeltaForge Energy' WHERE wells_well_name = '15/9-F-11'
SELECT wells_well_name,
       wells_well_field,
       wells_well_operator,
       wells_well_attr_uid,
       COUNT(*) AS stations
FROM {{zone_name}}.drilling.survey_documents
GROUP BY wells_well_name, wells_well_field, wells_well_operator,
         wells_well_attr_uid
ORDER BY wells_well_name;


-- ============================================================================
-- 4. LOAD THE 11 MARCH SURVEYS
-- ============================================================================
-- The rename that makes the rest of this file readable, and the casts an
-- anti-collision calculation needs.

INSERT INTO {{zone_name}}.drilling.survey_stations
SELECT d.wells_well_name                                                 AS well,
       d.wells_well_attr_uid                                             AS well_uid,
       d.wells_well_field                                                AS field,
       '2026-03-11'                                                      AS delivered_on,
       d.df_file_name                                                    AS source_file,
       d.wells_well_trajectory_trajectory_station_attr_uid               AS station_uid,
       CAST(d.wells_well_trajectory_trajectory_station_md   AS DOUBLE)   AS md_m,
       CAST(d.wells_well_trajectory_trajectory_station_tvd  AS DOUBLE)   AS tvd_m,
       CAST(d.wells_well_trajectory_trajectory_station_incl AS DOUBLE)   AS inclination_deg,
       CAST(d.wells_well_trajectory_trajectory_station_azi  AS DOUBLE)   AS azimuth_deg,
       CAST(d.wells_well_trajectory_trajectory_station_disp_ns AS DOUBLE) AS north_m,
       CAST(d.wells_well_trajectory_trajectory_station_disp_ew AS DOUBLE) AS east_m
FROM {{zone_name}}.drilling.survey_documents d
WHERE d.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.drilling.survey_stations s
      WHERE s.source_file = d.df_file_name
  );


-- ============================================================================
-- 5. THE FIRST TWO WELLS LANDED
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE stations = 40 WHERE well = '15/9-F-11'
ASSERT VALUE top_md = 400 WHERE well = '15/9-F-11'
ASSERT VALUE total_depth_md = 1570 WHERE well = '15/9-F-11'
ASSERT VALUE total_depth_tvd = 1266 WHERE well = '15/9-F-11'
ASSERT VALUE max_inclination = 62 WHERE well = '15/9-F-11'
ASSERT VALUE stations = 32 WHERE well = '15/9-F-11 A'
ASSERT VALUE total_depth_md = 1420 WHERE well = '15/9-F-11 A'
ASSERT VALUE total_depth_tvd = 1159 WHERE well = '15/9-F-11 A'
ASSERT VALUE max_inclination = 74 WHERE well = '15/9-F-11 A'
SELECT well,
       COUNT(*)                                        AS stations,
       CAST(ROUND(MIN(md_m)) AS BIGINT)                AS top_md,
       CAST(ROUND(MAX(md_m)) AS BIGINT)                AS total_depth_md,
       CAST(ROUND(MAX(tvd_m)) AS BIGINT)               AS total_depth_tvd,
       CAST(ROUND(MAX(inclination_deg)) AS BIGINT)     AS max_inclination
FROM {{zone_name}}.drilling.survey_stations
WHERE delivered_on = '2026-03-11'
GROUP BY well
ORDER BY well;


-- ============================================================================
-- 6. THE SAME DELIVERY AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.drilling.survey_stations
SELECT d.wells_well_name                                                 AS well,
       d.wells_well_attr_uid                                             AS well_uid,
       d.wells_well_field                                                AS field,
       '2026-03-11'                                                      AS delivered_on,
       d.df_file_name                                                    AS source_file,
       d.wells_well_trajectory_trajectory_station_attr_uid               AS station_uid,
       CAST(d.wells_well_trajectory_trajectory_station_md   AS DOUBLE)   AS md_m,
       CAST(d.wells_well_trajectory_trajectory_station_tvd  AS DOUBLE)   AS tvd_m,
       CAST(d.wells_well_trajectory_trajectory_station_incl AS DOUBLE)   AS inclination_deg,
       CAST(d.wells_well_trajectory_trajectory_station_azi  AS DOUBLE)   AS azimuth_deg,
       CAST(d.wells_well_trajectory_trajectory_station_disp_ns AS DOUBLE) AS north_m,
       CAST(d.wells_well_trajectory_trajectory_station_disp_ew AS DOUBLE) AS east_m
FROM {{zone_name}}.drilling.survey_documents d
WHERE d.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.drilling.survey_stations s
      WHERE s.source_file = d.df_file_name
  );


-- ============================================================================
-- 7. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE stations = 72
ASSERT VALUE wells = 2
SELECT COUNT(*)                    AS stations,
       COUNT(DISTINCT well)        AS wells,
       COUNT(DISTINCT source_file) AS documents
FROM {{zone_name}}.drilling.survey_stations
WHERE delivered_on = '2026-03-11';


-- ============================================================================
-- 8. LOAD THE 12 MARCH SURVEY
-- ============================================================================

INSERT INTO {{zone_name}}.drilling.survey_stations
SELECT d.wells_well_name                                                 AS well,
       d.wells_well_attr_uid                                             AS well_uid,
       d.wells_well_field                                                AS field,
       '2026-03-12'                                                      AS delivered_on,
       d.df_file_name                                                    AS source_file,
       d.wells_well_trajectory_trajectory_station_attr_uid               AS station_uid,
       CAST(d.wells_well_trajectory_trajectory_station_md   AS DOUBLE)   AS md_m,
       CAST(d.wells_well_trajectory_trajectory_station_tvd  AS DOUBLE)   AS tvd_m,
       CAST(d.wells_well_trajectory_trajectory_station_incl AS DOUBLE)   AS inclination_deg,
       CAST(d.wells_well_trajectory_trajectory_station_azi  AS DOUBLE)   AS azimuth_deg,
       CAST(d.wells_well_trajectory_trajectory_station_disp_ns AS DOUBLE) AS north_m,
       CAST(d.wells_well_trajectory_trajectory_station_disp_ew AS DOUBLE) AS east_m
FROM {{zone_name}}.drilling.survey_documents d
WHERE d.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.drilling.survey_stations s
      WHERE s.source_file = d.df_file_name
  );


-- ============================================================================
-- 9. THE WHOLE TEMPLATE
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE stations = 40 WHERE well = '15/9-F-11'
ASSERT VALUE stations = 32 WHERE well = '15/9-F-11 A'
ASSERT VALUE stations = 48 WHERE well = '15/9-F-12'
ASSERT VALUE total_depth_md = 1770 WHERE well = '15/9-F-12'
ASSERT VALUE total_depth_tvd = 1508 WHERE well = '15/9-F-12'
ASSERT VALUE max_inclination = 48 WHERE well = '15/9-F-12'
ASSERT VALUE delivered_on = '2026-03-12' WHERE well = '15/9-F-12'
SELECT well,
       MIN(delivered_on)                            AS delivered_on,
       COUNT(*)                                     AS stations,
       CAST(ROUND(MAX(md_m)) AS BIGINT)             AS total_depth_md,
       CAST(ROUND(MAX(tvd_m)) AS BIGINT)            AS total_depth_tvd,
       CAST(ROUND(MAX(inclination_deg)) AS BIGINT)  AS max_inclination,
       CAST(ROUND(MAX(azimuth_deg)) AS BIGINT)      AS azimuth
FROM {{zone_name}}.drilling.survey_stations
GROUP BY well
ORDER BY well;


-- ============================================================================
-- 10. THE VERTICAL SECTION ABOVE THE KICK-OFF
-- ============================================================================
-- Every well is drilled vertically before it kicks off, and a vertical
-- station has an inclination of exactly zero. Twelve of the 120 stations are
-- above their well's kick-off point.
--
-- Where inclination is zero, measured depth and true vertical depth are the
-- same number, which is the arithmetic check that the survey was integrated
-- rather than copied.

ASSERT ROW_COUNT = 1
ASSERT VALUE vertical_stations = 12
ASSERT VALUE md_equals_tvd = 12
SELECT COUNT(*)                                                  AS vertical_stations,
       COUNT(*) FILTER (WHERE ABS(md_m - tvd_m) < 0.01)          AS md_equals_tvd
FROM {{zone_name}}.drilling.survey_stations
WHERE inclination_deg = 0;


-- ============================================================================
-- 11. TRUE VERTICAL DEPTH NEVER EXCEEDS MEASURED DEPTH
-- ============================================================================
-- A trajectory that violated this would be a well that got deeper than the
-- pipe run into it. The count that matters is zero, across all 120 stations.

ASSERT ROW_COUNT = 0
SELECT well, station_uid, md_m, tvd_m
FROM {{zone_name}}.drilling.survey_stations
WHERE tvd_m > md_m + 0.01;


-- ============================================================================
-- 12. ANTI-COLLISION: THE CLOSEST APPROACH
-- ============================================================================
-- The query the surveys were loaded for. Two wells from the same slot pattern
-- are compared at every pair of stations within 25 m of each other in true
-- vertical depth, and the horizontal separation is the plane distance between
-- their north and east offsets.
--
-- They come within 3 m of each other at 550 m TVD, just below the template,
-- which is where wells on a template are always closest and always most at
-- risk. Seventy station pairs fall in the depth window and seven of them are
-- inside 20 m.

ASSERT ROW_COUNT = 1
ASSERT VALUE compared_pairs = 70
ASSERT VALUE closest_approach_m = 3
ASSERT VALUE inside_20_m = 7
ASSERT VALUE inside_50_m = 10
SELECT COUNT(*)                                                        AS compared_pairs,
       CAST(ROUND(MIN(SQRT(POWER(a.north_m - b.north_m, 2)
                         + POWER(a.east_m - b.east_m, 2)))) AS BIGINT) AS closest_approach_m,
       COUNT(*) FILTER (WHERE SQRT(POWER(a.north_m - b.north_m, 2)
                                 + POWER(a.east_m - b.east_m, 2)) < 20) AS inside_20_m,
       COUNT(*) FILTER (WHERE SQRT(POWER(a.north_m - b.north_m, 2)
                                 + POWER(a.east_m - b.east_m, 2)) < 50) AS inside_50_m
FROM {{zone_name}}.drilling.survey_stations a
JOIN {{zone_name}}.drilling.survey_stations b
  ON b.well = '15/9-F-11 A'
 AND ABS(a.tvd_m - b.tvd_m) <= 25.0
WHERE a.well = '15/9-F-11';


-- ============================================================================
-- 13. EVERY DOCUMENT LANDED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT s.source_file, s.curated, d.landed
FROM (
    SELECT source_file, COUNT(*) AS curated
    FROM {{zone_name}}.drilling.survey_stations
    GROUP BY source_file
) s
JOIN (
    SELECT df_file_name, COUNT(*) AS landed
    FROM {{zone_name}}.drilling.survey_documents
    GROUP BY df_file_name
) d
  ON d.df_file_name = s.source_file
WHERE s.curated <> d.landed;


-- ============================================================================
-- 14. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT d.df_file_name
FROM {{zone_name}}.drilling.survey_documents d
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.drilling.survey_stations s
    WHERE s.source_file = d.df_file_name
);


-- ============================================================================
-- 15. THE STATE AFTER THE FIRST DELIVERY, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 72
SELECT *
FROM {{zone_name}}.drilling.survey_stations VERSION AS OF 1;


-- ============================================================================
-- 16. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.drilling.survey_stations;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The template as a well engineer would sign it off: three wells, where each
-- one ends, and how far it stepped out from the slot it started in.

ASSERT ROW_COUNT = 3
ASSERT VALUE stations = 40 WHERE well = '15/9-F-11'
ASSERT VALUE total_depth_tvd = 1266 WHERE well = '15/9-F-11'
ASSERT VALUE step_out_m = 649 WHERE well = '15/9-F-11'
ASSERT VALUE stations = 32 WHERE well = '15/9-F-11 A'
ASSERT VALUE total_depth_tvd = 1159 WHERE well = '15/9-F-11 A'
ASSERT VALUE step_out_m = 509 WHERE well = '15/9-F-11 A'
ASSERT VALUE stations = 48 WHERE well = '15/9-F-12'
ASSERT VALUE total_depth_tvd = 1508 WHERE well = '15/9-F-12'
ASSERT VALUE step_out_m = 707 WHERE well = '15/9-F-12'
SELECT well,
       MIN(delivered_on)                             AS delivered_on,
       COUNT(*)                                      AS stations,
       CAST(ROUND(MAX(md_m)) AS BIGINT)              AS total_depth_md,
       CAST(ROUND(MAX(tvd_m)) AS BIGINT)             AS total_depth_tvd,
       CAST(ROUND(MAX(inclination_deg)) AS BIGINT)   AS max_inclination,
       CAST(ROUND(MAX(SQRT(POWER(north_m, 2) + POWER(east_m, 2)))) AS BIGINT) AS step_out_m
FROM {{zone_name}}.drilling.survey_stations
GROUP BY well
ORDER BY well;
