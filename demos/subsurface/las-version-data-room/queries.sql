-- ============================================================================
-- Licence Divestment Data Room - Reading LAS 1.2, 2.0 and 3.0 as One Table
-- ============================================================================
-- Six wells from Dutch block P/12, logged between 1979 and 2024, in three
-- versions of the Log ASCII Standard:
--
--   P12-01  1979  LAS 1.2   SPACE   120 steps   gamma only
--   P12-03  1984  LAS 1.2   SPACE   140 steps   gamma and sonic
--   P12-07  2003  LAS 2.0   SPACE   160 steps   triple combo
--   P12-09  2007  LAS 2.0   SPACE   180 steps   triple combo
--   P12-11  2021  LAS 3.0   COMMA   200 steps   triple combo + lithology
--   P12-14  2024  LAS 3.0   TAB     220 steps   triple combo + lithology
--
-- 1020 depth steps. Every count and every value below was recomputed from
-- the files on disk by a second, independent LAS reader written from the
-- CWLS standards, before the engine ever saw them.
--
-- What this demo is about that las-well-log-library is not: the LAS VERSION,
-- and the three specific ways a version changes what a byte means.
--
--   Query 4  is the 1.2 trap. A 1.2 file puts a non-numeric well entry's
--            label before the colon and its value after, so a reader that
--            assumes 2.0 answers `WELL` for the name of the well. Nothing
--            errors. The table is queryable and uniformly wrong.
--   Query 6  is the 3.0 delimiter. `DLM. COMMA` means whitespace no longer
--            separates readings, and a scanner that still split on it would
--            read `2400.0000,40.0000,...` as one token per row.
--   Query 7  is the 3.0 curve type. `{S}` declares a text curve, which LAS
--            2.0 has no way to express at all.
--   Query 8  is absence. LAS says a reading is missing in three different
--            ways and all three have to become the same NULL.
--   Query 9  is the 3.0 multi-topic file. P12-14 ships its core analysis in
--            the same file as its log, at a different grain.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- Six files, one format, no extension trusted: LAS is recognised from its
-- section markers, and the same detection covers 1.2, 2.0 and 3.0 because
-- all three open with a version section.

DISCOVER {{zone_name}}.data_room.log_files
    PATH '{{data_subdir}}/data_room'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE WHOLE ROOM, READ IN PLACE
-- ============================================================================
-- 120 + 140 + 160 + 180 + 200 + 220 = 1020 depth steps across six wells and
-- three versions of the standard, in one table, with no conversion step.

ASSERT ROW_COUNT = 1020
SELECT *
FROM {{zone_name}}.data_room.log_files;


-- ============================================================================
-- 3. INVENTORY BY WELL
-- ============================================================================
-- The well name comes from each file's own `~W` section and rides on every
-- row, so grouping the room by well is grouping the rows themselves. Six
-- wells, six real names.
--
-- This is the query that fails loudest if the version is ignored. Read as
-- 2.0, the two 1.2 files both report a well called `WELL`, so the room
-- collapses to five groups and 260 depth steps land in a well that does not
-- exist.

ASSERT ROW_COUNT = 6
ASSERT VALUE steps = 120 WHERE well = 'P12-01'
ASSERT VALUE steps = 140 WHERE well = 'P12-03'
ASSERT VALUE steps = 160 WHERE well = 'P12-07'
ASSERT VALUE steps = 180 WHERE well = 'P12-09'
ASSERT VALUE steps = 200 WHERE well = 'P12-11'
ASSERT VALUE steps = 220 WHERE well = 'P12-14'
ASSERT VALUE field = 'RIJNVELD' WHERE well = 'P12-01'
ASSERT VALUE field = 'RIJNVELD' WHERE well = 'P12-14'
SELECT well_well           AS well,
       well_fld            AS field,
       COUNT(*)            AS steps,
       MIN(dept)           AS top_depth,
       MAX(dept)           AS base_depth
FROM {{zone_name}}.data_room.log_files
GROUP BY well_well, well_fld
ORDER BY well_well;


-- ============================================================================
-- 4. THE LAS 1.2 WELL SECTION, READ THE 1.2 WAY
-- ============================================================================
-- The two oldest wells were exported by a tape conversion in the 1980s and
-- are LAS 1.2. In 1.2 the well block is written
--
--   WELL .        WELL : P12-01
--   UWI  . UNIQUE WELL ID : NLP1201
--
-- and in 2.0 it is written the other way round. A reader that assumes 2.0
-- returns `WELL`, `UNIQUE WELL ID`, `FIELD`, `LOCATION`, `COMPANY` and
-- `SERVICE COMPANY` for these six columns: the labels, on every row, for
-- every 1.2 file. Every value asserted here is a value the 2.0 reading
-- cannot produce.

ASSERT ROW_COUNT = 2
ASSERT VALUE uwi = 'NLP1201' WHERE well = 'P12-01'
ASSERT VALUE uwi = 'NLP1203' WHERE well = 'P12-03'
ASSERT VALUE field = 'RIJNVELD' WHERE well = 'P12-01'
ASSERT VALUE operator = 'RIJNVELD ENERGIE BV' WHERE well = 'P12-01'
ASSERT VALUE service_co = 'SEISMOGRAPH SERVICE LTD' WHERE well = 'P12-01'
ASSERT VALUE logged_on = '01-JUN-1979' WHERE well = 'P12-01'
ASSERT VALUE logged_on = '01-JUN-1984' WHERE well = 'P12-03'
ASSERT VALUE country = 'NL' WHERE well = 'P12-03'
ASSERT VALUE location = 'BLOCK P/12' WHERE well = 'P12-03'
SELECT DISTINCT
       well_well AS well,
       well_uwi  AS uwi,
       well_fld  AS field,
       well_comp AS operator,
       well_srvc AS service_co,
       well_date AS logged_on,
       well_ctry AS country,
       well_loc  AS location
FROM {{zone_name}}.data_room.log_files
WHERE df_file_name LIKE '%_v1_2.las'
ORDER BY well;


-- ============================================================================
-- 5. THE NUMERIC WELL ENTRIES KEEP THEIR PLACE IN EVERY VERSION
-- ============================================================================
-- The 1.2 layout applies only to entries whose value is not a number. STRT,
-- STOP, STEP and NULL are written value-first in 1.2 exactly as in 2.0, and
-- a reader that swapped those too would lose the null sentinel and read
-- -999.25 as a real gamma reading in every 1.2 file in the room.
--
-- The proof that it did not is on the next query, where P12-01's average
-- gamma is 54.5 rather than a large negative number.

ASSERT ROW_COUNT = 6
ASSERT VALUE declared_null = '-999.2500' WHERE well = 'P12-01'
ASSERT VALUE declared_null = '-999.2500' WHERE well = 'P12-14'
ASSERT VALUE declared_top = '1800.0000' WHERE well = 'P12-01'
ASSERT VALUE declared_step = '0.5000' WHERE well = 'P12-03'
ASSERT VALUE declared_step = '0.2500' WHERE well = 'P12-07'
ASSERT VALUE declared_step = '0.1250' WHERE well = 'P12-11'
SELECT DISTINCT
       well_well AS well,
       well_strt AS declared_top,
       well_stop AS declared_base,
       well_step AS declared_step,
       well_null AS declared_null
FROM {{zone_name}}.data_room.log_files
ORDER BY well;


-- ============================================================================
-- 6. THE LAS 3.0 DELIMITER
-- ============================================================================
-- P12-11 declares `DLM. COMMA` and P12-14 declares `DLM. TAB`. Their data
-- rows are `2400.0000,40.0000,90.0000,2.2000,0.1000,SAND` and the tab
-- equivalent. A scanner that split on whitespace would see one token per row
-- in the comma file, group six of them into a phantom depth step, and return
-- either garbage or nothing at all.
--
-- These are ordinary numbers, so they aggregate. An average gamma of 53.67
-- is arithmetic over 175 parsed doubles, not over a string.

ASSERT ROW_COUNT = 2
ASSERT VALUE steps = 200 WHERE well = 'P12-11'
ASSERT VALUE steps = 220 WHERE well = 'P12-14'
ASSERT VALUE top_depth = 2400.0 WHERE well = 'P12-11'
ASSERT VALUE base_depth = 2424.875 WHERE well = 'P12-11'
ASSERT VALUE top_depth = 2450.0 WHERE well = 'P12-14'
ASSERT VALUE base_depth = 2477.375 WHERE well = 'P12-14'
ASSERT VALUE live_gr = 175 WHERE well = 'P12-11'
ASSERT VALUE live_gr = 220 WHERE well = 'P12-14'
ASSERT VALUE avg_gr = 53.67 WHERE well = 'P12-11'
ASSERT VALUE avg_gr = 53.84 WHERE well = 'P12-14'
ASSERT VALUE min_gr = 40.0 WHERE well = 'P12-11'
ASSERT VALUE max_gr = 69.5 WHERE well = 'P12-14'
SELECT well_well                               AS well,
       COUNT(*)                                AS steps,
       MIN(dept)                               AS top_depth,
       MAX(dept)                               AS base_depth,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)   AS live_gr,
       ROUND(AVG(gr), 2)                       AS avg_gr,
       MIN(gr)                                 AS min_gr,
       MAX(gr)                                 AS max_gr
FROM {{zone_name}}.data_room.log_files
WHERE df_file_name LIKE '%_v3_0.las'
GROUP BY well_well
ORDER BY well_well;


-- ============================================================================
-- 7. THE LAS 3.0 TEXT CURVE
-- ============================================================================
-- LAS 3.0 ends a curve's description with its type in braces. The lithology
-- curve is declared `{S}`, and it is the only curve in the room that is not
-- a number. LAS 2.0 has no way to say this, which is why the four older
-- wells have no lithology at all rather than an empty one.
--
-- SHALE, SILTY is the value that proves the quoting: it contains the comma
-- that separates fields in P12-11, so the file wraps it in double quotes.
-- Split naively, it would become two columns and shift every row after it.

ASSERT ROW_COUNT = 4
ASSERT VALUE steps = 168 WHERE lithology = 'SAND'
ASSERT VALUE steps = 84 WHERE lithology = 'SHALE, SILTY'
ASSERT VALUE steps = 84 WHERE lithology = 'LIMESTONE'
ASSERT VALUE steps = 71 WHERE lithology = 'SHALE'
SELECT lith     AS lithology,
       COUNT(*) AS steps
FROM {{zone_name}}.data_room.log_files
WHERE lith IS NOT NULL
GROUP BY lith
ORDER BY lith;


-- ============================================================================
-- 8. THE THREE WAYS A LAS READING IS ABSENT
-- ============================================================================
-- LAS says "no reading here" three different ways, and all three have to
-- become the same NULL or the arithmetic below is wrong:
--
--   the sentinel     P12-01 writes -999.2500 for the twelve depths where the
--                    cable stuck, and P12-14 for eighteen density readings.
--   the blank field  only a delimited file can leave a field empty. P12-11
--                    writes nothing at all between two commas for 25 gamma
--                    readings and 13 lithologies.
--   the absent curve P12-01 and P12-03 have no density curve in the file, so
--                    all 260 of their rows have no density to have.
--
-- P12-01's average gamma is the falsifiable one. Over the 108 real readings
-- it is 54.5. If the -999.25 sentinel were treated as a reading it would be
-- -50.875, which is not a gamma ray value any rock has ever produced.

ASSERT ROW_COUNT = 1
ASSERT VALUE sentinel_gr_p01 = 12
ASSERT VALUE blank_gr_p11 = 25
ASSERT VALUE blank_lith_p11 = 13
ASSERT VALUE sentinel_rhob_p14 = 18
ASSERT VALUE never_logged_rhob = 260
ASSERT VALUE avg_gr_p01 = 54.5
SELECT COUNT(*) FILTER (WHERE well_well = 'P12-01' AND gr IS NULL)   AS sentinel_gr_p01,
       COUNT(*) FILTER (WHERE well_well = 'P12-11' AND gr IS NULL)   AS blank_gr_p11,
       COUNT(*) FILTER (WHERE well_well = 'P12-11' AND lith IS NULL) AS blank_lith_p11,
       COUNT(*) FILTER (WHERE well_well = 'P12-14' AND rhob IS NULL) AS sentinel_rhob_p14,
       COUNT(*) FILTER (WHERE well_well IN ('P12-01', 'P12-03')
                          AND rhob IS NULL)                          AS never_logged_rhob,
       ROUND(AVG(gr) FILTER (WHERE well_well = 'P12-01'), 2)         AS avg_gr_p01
FROM {{zone_name}}.data_room.log_files;


-- ============================================================================
-- 9. A LAS 3.0 FILE CARRIES MORE THAN ONE TABLE
-- ============================================================================
-- P12-14 ships its core analysis in the same file as its log: a
-- `~Core_Definition` block naming three columns and a `~Core_Data` block
-- with twelve rows, after the log data and before the end of the file.
--
-- A core row is not a depth step. Its grain is one cut core, its columns are
-- core top, core bottom and core porosity, and the twelve rows sit at 9000
-- to 9033 metres because they are laboratory sample depths from a different
-- reference. Read as more log rows they would give P12-14 232 depth steps
-- instead of 220 and a base depth of 9033 instead of 2477.375, which would
-- say the well is three and a half kilometres deeper than it is.
--
-- Classifying a section by its first letter alone is what causes this:
-- `~Core_Definition` and `~Curve` both begin with C.

ASSERT ROW_COUNT = 1
ASSERT VALUE steps = 220
ASSERT VALUE top_depth = 2450.0
ASSERT VALUE base_depth = 2477.375
ASSERT VALUE below_3000m = 0
ASSERT VALUE at_core_depths = 0
ASSERT VALUE live_dept = 220
SELECT COUNT(*)                                             AS steps,
       MIN(dept)                                            AS top_depth,
       MAX(dept)                                            AS base_depth,
       COUNT(*) FILTER (WHERE dept > 3000.0)                AS below_3000m,
       COUNT(*) FILTER (WHERE dept BETWEEN 8999.0 AND 9034.0) AS at_core_depths,
       COUNT(*) FILTER (WHERE dept IS NOT NULL)             AS live_dept
FROM {{zone_name}}.data_room.log_files
WHERE well_well = 'P12-14';


-- ============================================================================
-- 10. THE AUDIT THE BID COMMITTEE ASKED FOR
-- ============================================================================
-- One row per well: the version it arrived in, who logged it and when, and
-- how much of each curve is real. This is the whole point of reading the
-- room in place. Nothing was converted, nothing was exported to CSV first,
-- and the version differences never left the reader.
--
-- The LAS version comes from the file name because the data room's export
-- convention stamps it there. The file's own `~V` section is where it is
-- authoritative, and that is what the reader used to decide how to read the
-- well block; the name is only how the audit labels the row.

INSERT INTO {{zone_name}}.data_room.room_audit
SELECT well_well                                        AS well,
       CASE
           WHEN df_file_name LIKE '%_v1_2.las' THEN '1.2'
           WHEN df_file_name LIKE '%_v2_0.las' THEN '2.0'
           WHEN df_file_name LIKE '%_v3_0.las' THEN '3.0'
       END                                              AS las_version,
       well_date                                        AS logged_on,
       well_srvc                                        AS service_co,
       well_uwi                                         AS uwi,
       df_file_name                                     AS source_file,
       COUNT(*)                                         AS steps,
       MIN(dept)                                        AS top_depth,
       MAX(dept)                                        AS base_depth,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)           AS live_gr,
       COUNT(*) FILTER (WHERE dt IS NOT NULL)           AS live_dt,
       COUNT(*) FILTER (WHERE rhob IS NOT NULL)         AS live_rhob,
       COUNT(*) FILTER (WHERE nphi IS NOT NULL)         AS live_nphi,
       COUNT(*) FILTER (WHERE lith IS NOT NULL)         AS live_lith
FROM {{zone_name}}.data_room.log_files
GROUP BY well_well, df_file_name, well_date, well_srvc, well_uwi;


-- ============================================================================
-- 11. THE AUDIT, READ BACK
-- ============================================================================
-- Coverage by version, and the shape of forty-five years of tool
-- development. The 1979 well has a gamma ray and nothing else; the 2024 well
-- has everything including an interpreted lithology. Every zero here is a
-- curve that was never run, which is the answer the bid committee needs, and
-- it is different from a curve that ran and returned nothing.

ASSERT ROW_COUNT = 6
ASSERT VALUE las_version = '1.2' WHERE well = 'P12-01'
ASSERT VALUE las_version = '2.0' WHERE well = 'P12-07'
ASSERT VALUE las_version = '3.0' WHERE well = 'P12-11'
ASSERT VALUE live_gr = 108 WHERE well = 'P12-01'
ASSERT VALUE live_dt = 0 WHERE well = 'P12-01'
ASSERT VALUE live_rhob = 0 WHERE well = 'P12-03'
ASSERT VALUE live_dt = 140 WHERE well = 'P12-03'
ASSERT VALUE live_nphi = 160 WHERE well = 'P12-07'
ASSERT VALUE live_lith = 0 WHERE well = 'P12-09'
ASSERT VALUE live_lith = 187 WHERE well = 'P12-11'
ASSERT VALUE live_rhob = 202 WHERE well = 'P12-14'
ASSERT VALUE live_lith = 220 WHERE well = 'P12-14'
SELECT well,
       las_version,
       logged_on,
       service_co,
       steps,
       live_gr,
       live_dt,
       live_rhob,
       live_nphi,
       live_lith
FROM {{zone_name}}.data_room.room_audit
ORDER BY well;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One row holding every invariant the demo rests on: the room is 1020 depth
-- steps over six wells and three LAS versions; every well resolved to a real
-- name, so none of the six is called `WELL`; the two 1.2 files contributed
-- 260 of those steps with their headers intact; the two 3.0 files contributed
-- 420 with their commas and tabs parsed as separators; 407 rows carry a text
-- lithology that only LAS 3.0 can declare; and the deepest reading in the
-- whole room is 2477.375 metres, because the twelve core rows in P12-14 at
-- 9000 metres were never depth steps.

ASSERT ROW_COUNT = 1
ASSERT VALUE steps = 1020
ASSERT VALUE wells = 6
ASSERT VALUE files = 6
ASSERT VALUE wells_named_after_a_label = 0
ASSERT VALUE steps_from_las_1_2 = 260
ASSERT VALUE steps_from_las_2_0 = 340
ASSERT VALUE steps_from_las_3_0 = 420
ASSERT VALUE rows_with_lithology = 407
ASSERT VALUE live_gr = 983
ASSERT VALUE avg_gr = 53.92
ASSERT VALUE top_depth = 1800.0
ASSERT VALUE base_depth = 2477.375
SELECT COUNT(*)                                            AS steps,
       COUNT(DISTINCT well_well)                           AS wells,
       COUNT(DISTINCT df_file_name)                        AS files,
       COUNT(DISTINCT well_well) FILTER (
           WHERE well_well IN ('WELL', 'FIELD', 'COMPANY',
                               'LOCATION', 'UNIQUE WELL ID')
       )                                                   AS wells_named_after_a_label,
       COUNT(*) FILTER (WHERE df_file_name LIKE '%_v1_2.las') AS steps_from_las_1_2,
       COUNT(*) FILTER (WHERE df_file_name LIKE '%_v2_0.las') AS steps_from_las_2_0,
       COUNT(*) FILTER (WHERE df_file_name LIKE '%_v3_0.las') AS steps_from_las_3_0,
       COUNT(*) FILTER (WHERE lith IS NOT NULL)            AS rows_with_lithology,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)              AS live_gr,
       ROUND(AVG(gr), 2)                                   AS avg_gr,
       MIN(dept)                                           AS top_depth,
       MAX(dept)                                           AS base_depth
FROM {{zone_name}}.data_room.log_files;
