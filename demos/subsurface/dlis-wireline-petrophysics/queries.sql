-- ============================================================================
-- Volve Composite Log Ingestion - Incremental Load and Verification
-- ============================================================================
-- Real Volve DLIS composites, in two deliveries:
--
--   2026-03-11   LWD      15/9-F-9   1204 frames, 6 channels
--                         15/9-F-11  1637 frames, 6 channels
--   2026-03-12   wireline 15/9-F-15 C 19791 frames, 12 channels
--
-- Every count and range asserted below was decoded from these files by a
-- second, independent RP66 reader before the engine saw them, so a
-- disagreement here is a finding rather than a drifted literal.
--
-- Three real properties of this data drive the whole demo:
--
--   1. The wireline delivery is six channels wider than the LWD one, so the
--      curated table has to grow when it lands.
--   2. DEPTH is recorded in tenths of an inch in two files and in millimetres
--      in the third. Appending the raw channel would give a depth column with
--      two unit systems in it and nothing to tell them apart.
--   3. Every channel writes -999.25 where no reading was taken, and that is a
--      number, not a null, until something makes it one.
--
-- The loader handles all three on the way in. That is what a curated table is
-- for; the alternative is every downstream query handling them again.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- DLIS is recognised from the storage unit label and the visible-record
-- structure. The channel names in the registered table come from the CHANNEL
-- and FRAME sets inside the file, which is the only place they are written
-- down: a DLIS frame is a bare run of bytes whose meaning is entirely in the
-- metadata that preceded it.

DISCOVER {{zone_name}}.petrophysics.wireline_composite
    PATH '{{data_path}}/landing/wireline'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE LWD DELIVERY, READ IN PLACE
-- ============================================================================
-- 1204 + 1637 = 2841 frames across the two LWD composites.

ASSERT ROW_COUNT = 2841
SELECT *
FROM {{zone_name}}.petrophysics.lwd_composite;


-- ============================================================================
-- 3. THE WIRELINE DELIVERY, READ IN PLACE
-- ============================================================================

ASSERT ROW_COUNT = 19791
SELECT *
FROM {{zone_name}}.petrophysics.wireline_composite;


-- ============================================================================
-- 4. THE DEPTH UNIT PROBLEM, BEFORE IT IS FIXED
-- ============================================================================
-- Both LWD files log the same field over comparable intervals, and their raw
-- DEPTH channels are three orders of magnitude apart. 15/9-F-9 is in tenths
-- of an inch and 15/9-F-11 is in millimetres. Nothing in the numbers says so.

ASSERT ROW_COUNT = 2
ASSERT VALUE raw_depth_min = 354240 WHERE df_file_name = '2026-03-11_15_9-F-9_WLC_COMPOSITE_1.dlis'
ASSERT VALUE raw_depth_max = 426420 WHERE df_file_name = '2026-03-11_15_9-F-9_WLC_COMPOSITE_1.dlis'
ASSERT VALUE raw_depth_min = 183300 WHERE df_file_name = '2026-03-11_15_9-F-11_WLC_COMPOSITE_1.dlis'
ASSERT VALUE raw_depth_max = 346900 WHERE df_file_name = '2026-03-11_15_9-F-11_WLC_COMPOSITE_1.dlis'
SELECT df_file_name,
       COUNT(*)                             AS frames,
       CAST(ROUND(MIN(depth)) AS BIGINT)     AS raw_depth_min,
       CAST(ROUND(MAX(depth)) AS BIGINT)     AS raw_depth_max
FROM {{zone_name}}.petrophysics.lwd_composite
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 5. LOAD THE 11 MARCH LWD DELIVERY
-- ============================================================================
-- The loader does three things the raw files cannot: it names the well, it
-- converts depth to metres using the unit each file was written in, and it
-- turns the -999.25 no-reading sentinel into a real NULL.
--
-- NOT EXISTS on source_file is the watermark, so re-running the day is a
-- no-op rather than a duplicate.

INSERT INTO {{zone_name}}.petrophysics.well_logs
SELECT CASE
           WHEN STRPOS(l.df_file_name, '15_9-F-11_') > 0 THEN '15/9-F-11'
           ELSE '15/9-F-9'
       END                                       AS well,
       'LWD'                                     AS tool_string,
       '2026-03-11'                              AS delivered_on,
       l.df_file_name                            AS source_file,
       l.frame_number,
       l.depth * CASE
                     WHEN STRPOS(l.df_file_name, '15_9-F-11_') > 0 THEN 0.001
                     ELSE 0.00254
                 END                             AS depth_m,
       NULLIF(l.gr,   -999.25)                   AS gr,
       NULLIF(l.rdep, -999.25)                   AS rdep,
       NULLIF(l.rmed, -999.25)                   AS rmed,
       NULLIF(l.rop,  -999.25)                   AS rop,
       NULLIF(l.bs,   -999.25)                   AS bs
FROM {{zone_name}}.petrophysics.lwd_composite l
WHERE l.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.petrophysics.well_logs c
      WHERE c.source_file = l.df_file_name
  );


-- ============================================================================
-- 6. THE LWD DELIVERY LANDED, IN METRES
-- ============================================================================
-- The same two wells, now on a scale that can be compared. 15/9-F-9 logged
-- 900 to 1083 m and 15/9-F-11 logged 183 to 347 m, which is what the raw
-- numbers meant all along.

ASSERT ROW_COUNT = 2
ASSERT VALUE frames = 1204 WHERE well = '15/9-F-9'
ASSERT VALUE top_m = 900 WHERE well = '15/9-F-9'
ASSERT VALUE base_m = 1083 WHERE well = '15/9-F-9'
ASSERT VALUE frames = 1637 WHERE well = '15/9-F-11'
ASSERT VALUE top_m = 183 WHERE well = '15/9-F-11'
ASSERT VALUE base_m = 347 WHERE well = '15/9-F-11'
SELECT well,
       COUNT(*)                                AS frames,
       CAST(ROUND(MIN(depth_m)) AS BIGINT)      AS top_m,
       CAST(ROUND(MAX(depth_m)) AS BIGINT)      AS base_m
FROM {{zone_name}}.petrophysics.well_logs
WHERE tool_string = 'LWD'
GROUP BY well
ORDER BY well;


-- ============================================================================
-- 7. THE SENTINEL IS GONE
-- ============================================================================
-- 1167 of 15/9-F-9's 1204 frames carry a real gamma reading and 37 do not.
-- Read as a number, those 37 would drag the well's mean gamma to -21.

ASSERT ROW_COUNT = 2
ASSERT VALUE live_gr = 1167 WHERE well = '15/9-F-9'
ASSERT VALUE blank_gr = 37 WHERE well = '15/9-F-9'
ASSERT VALUE live_rdep = 1020 WHERE well = '15/9-F-9'
ASSERT VALUE live_gr = 1370 WHERE well = '15/9-F-11'
ASSERT VALUE blank_gr = 267 WHERE well = '15/9-F-11'
ASSERT VALUE live_rdep = 1225 WHERE well = '15/9-F-11'
SELECT well,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)   AS live_gr,
       COUNT(*) FILTER (WHERE gr IS NULL)       AS blank_gr,
       COUNT(*) FILTER (WHERE rdep IS NOT NULL) AS live_rdep,
       COUNT(*) FILTER (WHERE bs IS NOT NULL)   AS live_bs
FROM {{zone_name}}.petrophysics.well_logs
WHERE tool_string = 'LWD'
GROUP BY well
ORDER BY well;


-- ============================================================================
-- 8. NO SENTINEL SURVIVED AS A VALUE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.petrophysics.well_logs
WHERE gr < -900 OR rdep < -900 OR rmed < -900 OR rop < -900 OR bs < -900;


-- ============================================================================
-- 9. THE SAME LOAD AGAIN
-- ============================================================================
-- Byte for byte the statement from step 5.

INSERT INTO {{zone_name}}.petrophysics.well_logs
SELECT CASE
           WHEN STRPOS(l.df_file_name, '15_9-F-11_') > 0 THEN '15/9-F-11'
           ELSE '15/9-F-9'
       END                                       AS well,
       'LWD'                                     AS tool_string,
       '2026-03-11'                              AS delivered_on,
       l.df_file_name                            AS source_file,
       l.frame_number,
       l.depth * CASE
                     WHEN STRPOS(l.df_file_name, '15_9-F-11_') > 0 THEN 0.001
                     ELSE 0.00254
                 END                             AS depth_m,
       NULLIF(l.gr,   -999.25)                   AS gr,
       NULLIF(l.rdep, -999.25)                   AS rdep,
       NULLIF(l.rmed, -999.25)                   AS rmed,
       NULLIF(l.rop,  -999.25)                   AS rop,
       NULLIF(l.bs,   -999.25)                   AS bs
FROM {{zone_name}}.petrophysics.lwd_composite l
WHERE l.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.petrophysics.well_logs c
      WHERE c.source_file = l.df_file_name
  );


-- ============================================================================
-- 10. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE frames = 2841
ASSERT VALUE files = 2
SELECT COUNT(*)                    AS frames,
       COUNT(DISTINCT source_file) AS files
FROM {{zone_name}}.petrophysics.well_logs
WHERE delivered_on = '2026-03-11';


-- ============================================================================
-- 11. THE WIRELINE DELIVERY IS WIDER THAN THE TABLE
-- ============================================================================
-- Six channels the LWD string never recorded: a caliper, bulk density and its
-- correction, photoelectric factor, neutron porosity and sonic. The curated
-- table grows to take them, and the rows already in it keep NULL for the
-- columns their tool never carried, which is the truthful answer.

ALTER TABLE {{zone_name}}.petrophysics.well_logs ADD COLUMN cali DOUBLE;
ALTER TABLE {{zone_name}}.petrophysics.well_logs ADD COLUMN den DOUBLE;
ALTER TABLE {{zone_name}}.petrophysics.well_logs ADD COLUMN denc DOUBLE;
ALTER TABLE {{zone_name}}.petrophysics.well_logs ADD COLUMN pef DOUBLE;
ALTER TABLE {{zone_name}}.petrophysics.well_logs ADD COLUMN neu DOUBLE;
ALTER TABLE {{zone_name}}.petrophysics.well_logs ADD COLUMN ac DOUBLE;


-- ============================================================================
-- 12. LOAD THE 12 MARCH WIRELINE DELIVERY
-- ============================================================================

INSERT INTO {{zone_name}}.petrophysics.well_logs
SELECT '15/9-F-15 C'                AS well,
       'WIRELINE'                   AS tool_string,
       '2026-03-12'                 AS delivered_on,
       w.df_file_name               AS source_file,
       w.frame_number,
       w.depth * 0.00254            AS depth_m,
       NULLIF(w.gr,   -999.25)      AS gr,
       NULLIF(w.rdep, -999.25)      AS rdep,
       NULLIF(w.rmed, -999.25)      AS rmed,
       NULLIF(w.rop,  -999.25)      AS rop,
       NULLIF(w.bs,   -999.25)      AS bs,
       NULLIF(w.cali, -999.25)      AS cali,
       NULLIF(w.den,  -999.25)      AS den,
       NULLIF(w.denc, -999.25)      AS denc,
       NULLIF(w.pef,  -999.25)      AS pef,
       NULLIF(w.neu,  -999.25)      AS neu,
       NULLIF(w.ac,   -999.25)      AS ac
FROM {{zone_name}}.petrophysics.wireline_composite w
WHERE w.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.petrophysics.well_logs c
      WHERE c.source_file = w.df_file_name
  );


-- ============================================================================
-- 13. BOTH DELIVERIES, SIDE BY SIDE
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE frames = 2841 WHERE delivered_on = '2026-03-11'
ASSERT VALUE files = 2 WHERE delivered_on = '2026-03-11'
ASSERT VALUE wells = 2 WHERE delivered_on = '2026-03-11'
ASSERT VALUE frames = 19791 WHERE delivered_on = '2026-03-12'
ASSERT VALUE files = 1 WHERE delivered_on = '2026-03-12'
ASSERT VALUE wells = 1 WHERE delivered_on = '2026-03-12'
SELECT delivered_on,
       COUNT(DISTINCT source_file) AS files,
       COUNT(DISTINCT well)        AS wells,
       COUNT(*)                    AS frames
FROM {{zone_name}}.petrophysics.well_logs
GROUP BY delivered_on
ORDER BY delivered_on;


-- ============================================================================
-- 14. THE NEW COLUMNS ARE POPULATED ONLY WHERE THE TOOL RECORDED THEM
-- ============================================================================
-- Density exists on 1955 of the wireline well's frames and on none of the LWD
-- frames, because the LWD string had no density sonde. That is the difference
-- between a missing measurement and a zero.

ASSERT ROW_COUNT = 2
ASSERT VALUE live_den = 0 WHERE tool_string = 'LWD'
ASSERT VALUE live_neu = 0 WHERE tool_string = 'LWD'
ASSERT VALUE live_cali = 0 WHERE tool_string = 'LWD'
ASSERT VALUE live_den = 1955 WHERE tool_string = 'WIRELINE'
ASSERT VALUE live_neu = 2020 WHERE tool_string = 'WIRELINE'
ASSERT VALUE live_cali = 2040 WHERE tool_string = 'WIRELINE'
ASSERT VALUE live_ac = 2370 WHERE tool_string = 'WIRELINE'
ASSERT VALUE live_pef = 1955 WHERE tool_string = 'WIRELINE'
SELECT tool_string,
       COUNT(*)                                 AS frames,
       COUNT(*) FILTER (WHERE cali IS NOT NULL) AS live_cali,
       COUNT(*) FILTER (WHERE den IS NOT NULL)  AS live_den,
       COUNT(*) FILTER (WHERE neu IS NOT NULL)  AS live_neu,
       COUNT(*) FILTER (WHERE pef IS NOT NULL)  AS live_pef,
       COUNT(*) FILTER (WHERE ac IS NOT NULL)   AS live_ac
FROM {{zone_name}}.petrophysics.well_logs
GROUP BY tool_string
ORDER BY tool_string;


-- ============================================================================
-- 15. EVERY FILE LANDED EXACTLY ONCE
-- ============================================================================
-- The curated frame count per file has to equal the frame count the file
-- holds. A duplicated load doubles every row here and a partial load shortens
-- one, so this catches both without knowing which day the demo is on.

ASSERT ROW_COUNT = 3
ASSERT VALUE frames = 1204 WHERE source_file = '2026-03-11_15_9-F-9_WLC_COMPOSITE_1.dlis'
ASSERT VALUE frames = 1637 WHERE source_file = '2026-03-11_15_9-F-11_WLC_COMPOSITE_1.dlis'
ASSERT VALUE frames = 19791 WHERE source_file = '2026-03-12_15_9-F-15C_WLC_COMPOSITE_2.dlis'
SELECT source_file,
       COUNT(*)                 AS frames,
       MIN(frame_number)        AS first_frame,
       MAX(frame_number)        AS last_frame
FROM {{zone_name}}.petrophysics.well_logs
GROUP BY source_file
ORDER BY source_file;


-- ============================================================================
-- 16. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT l.df_file_name
FROM {{zone_name}}.petrophysics.lwd_composite l
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.petrophysics.well_logs c
    WHERE c.source_file = l.df_file_name
);


-- ============================================================================
-- 17. THE STATE AFTER THE FIRST DELIVERY, BY TIME TRAVEL
-- ============================================================================
-- Version 0 is the empty table the setup created and version 1 is the LWD
-- load, so version 1 is the eleventh of March and nothing else. The question
-- "what did the evaluation see before the wireline run" has an answer that
-- does not depend on anyone having kept a copy.

ASSERT ROW_COUNT = 2841
SELECT *
FROM {{zone_name}}.petrophysics.well_logs VERSION AS OF 1;


-- ============================================================================
-- 18. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.petrophysics.well_logs;


-- ============================================================================
-- 19. NET RESERVOIR FROM THE WIRELINE WELL
-- ============================================================================
-- The evaluation the logs were run for. A gamma reading under 60 gAPI is
-- clean sand rather than shale, and 1492 of the wireline well's frames are
-- both clean and carry a density reading.

ASSERT ROW_COUNT = 1
ASSERT VALUE net_reservoir_samples = 1492
SELECT COUNT(*) AS net_reservoir_samples
FROM {{zone_name}}.petrophysics.well_logs
WHERE well = '15/9-F-15 C'
  AND gr < 60
  AND den IS NOT NULL;


-- ============================================================================
-- 20. NET PAY
-- ============================================================================
-- Density porosity from the standard sandstone transform, (2.65 - rhob) over
-- (2.65 - 1.0). Clean sand with more than 15 percent porosity is pay: 280
-- frames of it, all between 3126 and 3220 m, which is the Hugin Formation.

ASSERT ROW_COUNT = 1
ASSERT VALUE net_pay_samples = 280
ASSERT VALUE pay_top_m = 3126
ASSERT VALUE pay_base_m = 3220
SELECT COUNT(*)                                  AS net_pay_samples,
       CAST(ROUND(MIN(depth_m)) AS BIGINT)       AS pay_top_m,
       CAST(ROUND(MAX(depth_m)) AS BIGINT)       AS pay_base_m
FROM {{zone_name}}.petrophysics.well_logs
WHERE well = '15/9-F-15 C'
  AND gr < 60
  AND den IS NOT NULL
  AND (2.65 - den) / 1.65 > 0.15;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One row per delivered file with everything the load promised: the frames
-- arrived once, the depths are in metres, the sentinel is a null, and the
-- wireline channels exist only where a wireline tool recorded them.

ASSERT ROW_COUNT = 3
ASSERT VALUE frames = 1204 WHERE well = '15/9-F-9'
ASSERT VALUE top_m = 900 WHERE well = '15/9-F-9'
ASSERT VALUE base_m = 1083 WHERE well = '15/9-F-9'
ASSERT VALUE live_gr = 1167 WHERE well = '15/9-F-9'
ASSERT VALUE live_den = 0 WHERE well = '15/9-F-9'
ASSERT VALUE frames = 1637 WHERE well = '15/9-F-11'
ASSERT VALUE top_m = 183 WHERE well = '15/9-F-11'
ASSERT VALUE base_m = 347 WHERE well = '15/9-F-11'
ASSERT VALUE live_gr = 1370 WHERE well = '15/9-F-11'
ASSERT VALUE frames = 19791 WHERE well = '15/9-F-15 C'
ASSERT VALUE top_m = 216 WHERE well = '15/9-F-15 C'
ASSERT VALUE base_m = 3232 WHERE well = '15/9-F-15 C'
ASSERT VALUE live_gr = 19551 WHERE well = '15/9-F-15 C'
ASSERT VALUE live_den = 1955 WHERE well = '15/9-F-15 C'
SELECT well,
       MIN(tool_string)                          AS tool_string,
       MIN(delivered_on)                         AS delivered_on,
       COUNT(*)                                  AS frames,
       CAST(ROUND(MIN(depth_m)) AS BIGINT)       AS top_m,
       CAST(ROUND(MAX(depth_m)) AS BIGINT)       AS base_m,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)    AS live_gr,
       COUNT(*) FILTER (WHERE den IS NOT NULL)   AS live_den
FROM {{zone_name}}.petrophysics.well_logs
GROUP BY well
ORDER BY well;
