-- ============================================================================
-- 2D Survey CDP Index - Incremental Load and Verification
-- ============================================================================
-- Two real Volve ST0299 2D lines, delivered a day apart:
--
--   2026-03-11  ST0299-05005    984 traces, CDP 2100 to 3083
--   2026-03-12  ST0299-15010   1030 traces, CDP 2104 to 3133
--
-- Both are IBM floating point, 3000 samples at a 1 ms interval, with a
-- coordinate scalar of -100. Every count and range below was decoded from the
-- files by a second, independent SEG-Y reader before the engine saw them.
--
-- Three things this data does that a written fixture would not have thought
-- to do, and the demo leans on all three:
--
--   1. Ninety-five traces on the first line and ninety-one on the second
--      carry a source coordinate of exactly zero. That is the file declining
--      to say, not a position in the Gulf of Guinea.
--   2. The offsets are negative. Some processors write a signed offset and a
--      catalogue that assumes otherwise loses the near traces.
--   3. The revision field reads as 256, because SEG-Y rev 1 writes the
--      revision as a major byte and a minor byte, so 1.0 is 0x0100. A reader
--      treating it as a plain integer sees revision 256 and can either
--      reject the file or ignore the field; ignoring it is correct.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.seismic_survey.survey_lines
    PATH '{{data_subdir}}/landing'
    WITH (
        FILE_METADATA = true,
        include_samples = 'false'
    )
    PRINT;


-- ============================================================================
-- 2. BOTH LINES, HEADERS ONLY
-- ============================================================================
-- 984 + 1030 = 2014 traces, read from 24 MB of file without decoding a single
-- sample.

ASSERT ROW_COUNT = 2014
SELECT *
FROM {{zone_name}}.seismic_survey.survey_lines;


-- ============================================================================
-- 3. THE SURVEY GEOMETRY, PER LINE
-- ============================================================================
-- The acquisition parameters agree across both lines, which is what says they
-- belong to the same survey: 3000 samples at 1 ms each.

ASSERT ROW_COUNT = 2
ASSERT VALUE traces = 984 WHERE df_file_name = '2026-03-11_ST0299-05005_MIG_FIN.segy'
ASSERT VALUE first_cdp = 2100 WHERE df_file_name = '2026-03-11_ST0299-05005_MIG_FIN.segy'
ASSERT VALUE last_cdp = 3083 WHERE df_file_name = '2026-03-11_ST0299-05005_MIG_FIN.segy'
ASSERT VALUE traces = 1030 WHERE df_file_name = '2026-03-12_ST0299-15010_MIG_FIN.segy'
ASSERT VALUE first_cdp = 2104 WHERE df_file_name = '2026-03-12_ST0299-15010_MIG_FIN.segy'
ASSERT VALUE last_cdp = 3133 WHERE df_file_name = '2026-03-12_ST0299-15010_MIG_FIN.segy'
ASSERT VALUE sample_count = 3000 WHERE df_file_name = '2026-03-11_ST0299-05005_MIG_FIN.segy'
ASSERT VALUE sample_interval_us = 1000 WHERE df_file_name = '2026-03-11_ST0299-05005_MIG_FIN.segy'
SELECT df_file_name,
       COUNT(*)                       AS traces,
       MIN(ensemble)                  AS first_cdp,
       MAX(ensemble)                  AS last_cdp,
       MIN(sample_count)              AS sample_count,
       MIN(sample_interval_us)        AS sample_interval_us
FROM {{zone_name}}.seismic_survey.survey_lines
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. THE COORDINATE SCALAR WAS APPLIED
-- ============================================================================
-- SEG-Y stores coordinates as integers with a separate scalar in bytes 71-72,
-- here -100, meaning divide. The CDP eastings are around 432000 to 438500 in
-- UTM zone 31N. A reader that ignored the scalar would report 43205444.

ASSERT ROW_COUNT = 1
ASSERT VALUE min_easting = 432054
ASSERT VALUE max_easting = 438530
ASSERT VALUE min_northing = 6478032
ASSERT VALUE max_northing = 6479875
SELECT CAST(ROUND(MIN(cdp_x)) AS BIGINT) AS min_easting,
       CAST(ROUND(MAX(cdp_x)) AS BIGINT) AS max_easting,
       CAST(ROUND(MIN(cdp_y)) AS BIGINT) AS min_northing,
       CAST(ROUND(MAX(cdp_y)) AS BIGINT) AS max_northing
FROM {{zone_name}}.seismic_survey.survey_lines;


-- ============================================================================
-- 5. THE TRACES THAT DO NOT KNOW WHERE THEY WERE SHOT
-- ============================================================================
-- 95 on the first line, 91 on the second. Every one of them still has a good
-- CDP coordinate, which is why the index keeps the trace and nulls only the
-- source position.

ASSERT ROW_COUNT = 2
ASSERT VALUE no_source_xy = 95 WHERE df_file_name = '2026-03-11_ST0299-05005_MIG_FIN.segy'
ASSERT VALUE with_source_xy = 889 WHERE df_file_name = '2026-03-11_ST0299-05005_MIG_FIN.segy'
ASSERT VALUE no_source_xy = 91 WHERE df_file_name = '2026-03-12_ST0299-15010_MIG_FIN.segy'
ASSERT VALUE with_source_xy = 939 WHERE df_file_name = '2026-03-12_ST0299-15010_MIG_FIN.segy'
SELECT df_file_name,
       COUNT(*) FILTER (WHERE source_x = 0 AND source_y = 0)  AS no_source_xy,
       COUNT(*) FILTER (WHERE source_x <> 0 OR source_y <> 0) AS with_source_xy
FROM {{zone_name}}.seismic_survey.survey_lines
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 6. LOAD THE 11 MARCH LINE
-- ============================================================================
-- The zero source coordinates become NULL here, once, rather than in every
-- query downstream forever.

INSERT INTO {{zone_name}}.seismic_survey.cdp_index
SELECT 'ST0299-05005'                                        AS line,
       '2026-03-11'                                          AS delivered_on,
       s.df_file_name                                        AS source_file,
       s.trace_sequence_line,
       s.field_record,
       s.ensemble                                            AS cdp,
       s.offset                                              AS offset_m,
       s.cdp_x,
       s.cdp_y,
       CASE WHEN s.source_x <> 0 OR s.source_y <> 0 THEN s.source_x END AS source_x,
       CASE WHEN s.source_x <> 0 OR s.source_y <> 0 THEN s.source_y END AS source_y,
       s.sample_count,
       s.sample_interval_us
FROM {{zone_name}}.seismic_survey.survey_lines s
WHERE s.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.seismic_survey.cdp_index c
      WHERE c.source_file = s.df_file_name
  );


-- ============================================================================
-- 7. THE FIRST LINE IS INDEXED
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE traces = 984
ASSERT VALUE cdps = 984
ASSERT VALUE with_source = 889
ASSERT VALUE without_source = 95
SELECT COUNT(*)                                        AS traces,
       COUNT(DISTINCT cdp)                             AS cdps,
       COUNT(*) FILTER (WHERE source_x IS NOT NULL)    AS with_source,
       COUNT(*) FILTER (WHERE source_x IS NULL)        AS without_source
FROM {{zone_name}}.seismic_survey.cdp_index
WHERE line = 'ST0299-05005';


-- ============================================================================
-- 8. THE SAME LINE AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.seismic_survey.cdp_index
SELECT 'ST0299-05005'                                        AS line,
       '2026-03-11'                                          AS delivered_on,
       s.df_file_name                                        AS source_file,
       s.trace_sequence_line,
       s.field_record,
       s.ensemble                                            AS cdp,
       s.offset                                              AS offset_m,
       s.cdp_x,
       s.cdp_y,
       CASE WHEN s.source_x <> 0 OR s.source_y <> 0 THEN s.source_x END AS source_x,
       CASE WHEN s.source_x <> 0 OR s.source_y <> 0 THEN s.source_y END AS source_y,
       s.sample_count,
       s.sample_interval_us
FROM {{zone_name}}.seismic_survey.survey_lines s
WHERE s.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.seismic_survey.cdp_index c
      WHERE c.source_file = s.df_file_name
  );


-- ============================================================================
-- 9. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE traces = 984
ASSERT VALUE lines = 1
SELECT COUNT(*)                    AS traces,
       COUNT(DISTINCT source_file) AS lines
FROM {{zone_name}}.seismic_survey.cdp_index
WHERE delivered_on = '2026-03-11';


-- ============================================================================
-- 10. LOAD THE 12 MARCH LINE
-- ============================================================================

INSERT INTO {{zone_name}}.seismic_survey.cdp_index
SELECT 'ST0299-15010'                                        AS line,
       '2026-03-12'                                          AS delivered_on,
       s.df_file_name                                        AS source_file,
       s.trace_sequence_line,
       s.field_record,
       s.ensemble                                            AS cdp,
       s.offset                                              AS offset_m,
       s.cdp_x,
       s.cdp_y,
       CASE WHEN s.source_x <> 0 OR s.source_y <> 0 THEN s.source_x END AS source_x,
       CASE WHEN s.source_x <> 0 OR s.source_y <> 0 THEN s.source_y END AS source_y,
       s.sample_count,
       s.sample_interval_us
FROM {{zone_name}}.seismic_survey.survey_lines s
WHERE s.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.seismic_survey.cdp_index c
      WHERE c.source_file = s.df_file_name
  );


-- ============================================================================
-- 11. THE SURVEY INDEX, LINE BY LINE
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE traces = 984 WHERE line = 'ST0299-05005'
ASSERT VALUE first_cdp = 2100 WHERE line = 'ST0299-05005'
ASSERT VALUE last_cdp = 3083 WHERE line = 'ST0299-05005'
ASSERT VALUE west_m = 432054 WHERE line = 'ST0299-05005'
ASSERT VALUE east_m = 438193 WHERE line = 'ST0299-05005'
ASSERT VALUE traces = 1030 WHERE line = 'ST0299-15010'
ASSERT VALUE first_cdp = 2104 WHERE line = 'ST0299-15010'
ASSERT VALUE last_cdp = 3133 WHERE line = 'ST0299-15010'
ASSERT VALUE west_m = 432105 WHERE line = 'ST0299-15010'
ASSERT VALUE east_m = 438530 WHERE line = 'ST0299-15010'
SELECT line,
       MIN(delivered_on)                  AS delivered_on,
       COUNT(*)                           AS traces,
       MIN(cdp)                           AS first_cdp,
       MAX(cdp)                           AS last_cdp,
       CAST(ROUND(MIN(cdp_x)) AS BIGINT)  AS west_m,
       CAST(ROUND(MAX(cdp_x)) AS BIGINT)  AS east_m,
       CAST(ROUND(MIN(cdp_y)) AS BIGINT)  AS south_m,
       CAST(ROUND(MAX(cdp_y)) AS BIGINT)  AS north_m
FROM {{zone_name}}.seismic_survey.cdp_index
GROUP BY line
ORDER BY line;


-- ============================================================================
-- 12. THE WHOLE INDEX
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE traces = 2014
ASSERT VALUE lines = 2
ASSERT VALUE with_source = 1828
ASSERT VALUE without_source = 186
SELECT COUNT(*)                                      AS traces,
       COUNT(DISTINCT line)                          AS lines,
       COUNT(*) FILTER (WHERE source_x IS NOT NULL)  AS with_source,
       COUNT(*) FILTER (WHERE source_x IS NULL)      AS without_source
FROM {{zone_name}}.seismic_survey.cdp_index;


-- ============================================================================
-- 13. EVERY LINE INDEXED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT c.source_file, c.indexed, s.landed
FROM (
    SELECT source_file, COUNT(*) AS indexed
    FROM {{zone_name}}.seismic_survey.cdp_index
    GROUP BY source_file
) c
JOIN (
    SELECT df_file_name, COUNT(*) AS landed
    FROM {{zone_name}}.seismic_survey.survey_lines
    GROUP BY df_file_name
) s
  ON s.df_file_name = c.source_file
WHERE c.indexed <> s.landed;


-- ============================================================================
-- 14. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT s.df_file_name
FROM {{zone_name}}.seismic_survey.survey_lines s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.seismic_survey.cdp_index c
    WHERE c.source_file = s.df_file_name
);


-- ============================================================================
-- 15. THE STATE AFTER THE FIRST LINE, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 984
SELECT *
FROM {{zone_name}}.seismic_survey.cdp_index VERSION AS OF 1;


-- ============================================================================
-- 16. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.seismic_survey.cdp_index;


-- ============================================================================
-- 17. WHICH LINES CROSS A PROSPECT
-- ============================================================================
-- The question the index exists to answer, and the one that used to mean
-- opening every line in the survey. A box around 435000 to 436000 east and
-- 6478000 to 6480000 north, and the index says which lines have CDPs inside
-- it and which CDP numbers to load.

ASSERT ROW_COUNT = 2
ASSERT VALUE cdps_in_box = 160 WHERE line = 'ST0299-05005'
ASSERT VALUE from_cdp = 2572 WHERE line = 'ST0299-05005'
ASSERT VALUE to_cdp = 2731 WHERE line = 'ST0299-05005'
ASSERT VALUE cdps_in_box = 160 WHERE line = 'ST0299-15010'
ASSERT VALUE from_cdp = 2568 WHERE line = 'ST0299-15010'
ASSERT VALUE to_cdp = 2727 WHERE line = 'ST0299-15010'
SELECT line,
       COUNT(*)   AS cdps_in_box,
       MIN(cdp)   AS from_cdp,
       MAX(cdp)   AS to_cdp
FROM {{zone_name}}.seismic_survey.cdp_index
WHERE cdp_x BETWEEN 435000 AND 436000
  AND cdp_y BETWEEN 6478000 AND 6480000
GROUP BY line
ORDER BY line;


-- ============================================================================
-- 18. THE OFFSETS ARE NEGATIVE, AND THAT IS THE FILE'S CHOICE
-- ============================================================================
-- Every trace on both lines carries an offset between -1223 and -35. A
-- catalogue that filtered on a positive near-offset range would return
-- nothing at all and look like an empty survey rather than a sign
-- convention.

ASSERT ROW_COUNT = 1
ASSERT VALUE nearest = -35
ASSERT VALUE farthest = -1223
ASSERT VALUE positive_offsets = 0
SELECT MAX(offset_m)                              AS nearest,
       MIN(offset_m)                              AS farthest,
       COUNT(*) FILTER (WHERE offset_m > 0)       AS positive_offsets,
       COUNT(DISTINCT offset_m)                   AS distinct_offsets
FROM {{zone_name}}.seismic_survey.cdp_index;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE traces = 984 WHERE line = 'ST0299-05005'
ASSERT VALUE with_source = 889 WHERE line = 'ST0299-05005'
ASSERT VALUE without_source = 95 WHERE line = 'ST0299-05005'
ASSERT VALUE sample_count = 3000 WHERE line = 'ST0299-05005'
ASSERT VALUE delivered_on = '2026-03-11' WHERE line = 'ST0299-05005'
ASSERT VALUE traces = 1030 WHERE line = 'ST0299-15010'
ASSERT VALUE with_source = 939 WHERE line = 'ST0299-15010'
ASSERT VALUE without_source = 91 WHERE line = 'ST0299-15010'
ASSERT VALUE sample_count = 3000 WHERE line = 'ST0299-15010'
ASSERT VALUE delivered_on = '2026-03-12' WHERE line = 'ST0299-15010'
SELECT line,
       MIN(delivered_on)                             AS delivered_on,
       COUNT(*)                                      AS traces,
       COUNT(DISTINCT cdp)                           AS cdps,
       COUNT(*) FILTER (WHERE source_x IS NOT NULL)  AS with_source,
       COUNT(*) FILTER (WHERE source_x IS NULL)      AS without_source,
       MIN(sample_count)                             AS sample_count,
       MIN(sample_interval_us)                       AS sample_interval_us
FROM {{zone_name}}.seismic_survey.cdp_index
GROUP BY line
ORDER BY line;
