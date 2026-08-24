-- ============================================================================
-- LIS Tape Archive Recovery - Incremental Load and Verification
-- ============================================================================
-- Two tapes, digitised in two batches:
--
--   2026-03-11  15/9-F-4    Volve composite, 12 curves, 21505 frames
--   2026-03-12  15/9-F-4 A  reprocessed,     26 curves,   640 frames
--
-- Every count below was decoded from the tapes by a second, independent
-- LIS-79 reader before the engine saw them, so a disagreement here is a
-- finding rather than a drifted literal.
--
-- Three things about LIS make this worth doing in place rather than through a
-- conversion service, and all three are visible in the queries:
--
--   1. The tapes are Tape Image Format framed. Twelve bytes of tape offsets
--      wrap every physical record and hide the record structure underneath.
--   2. The curve list lives in a Data Format Specification Record, which is
--      logical record type 64. Type 34 is Well Site Data and looking for the
--      frame layout there finds nothing, so a full log reads as zero rows.
--   3. The 26-curve specification on the second tape does not fit in one
--      1024-byte physical record, so it arrives split with the successor and
--      predecessor attribute bits set. A reader that stops at the first piece
--      sees a short curve list and a frame width that no longer divides the
--      data.
--
-- If any of the three were wrong the frame arithmetic would not close, and
-- the first two queries would not return the numbers they do.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.log_archive.volve_composite
    PATH '{{data_path}}/landing/volve'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE VOLVE TAPE, READ IN PLACE
-- ============================================================================
-- 21505 frames of 48 bytes each, with nothing left over. That the arithmetic
-- closes exactly is the evidence the framing was stripped, the specification
-- record was found, and the frame width is right: get any of the three wrong
-- and the division leaves a remainder.

ASSERT ROW_COUNT = 21505
SELECT *
FROM {{zone_name}}.log_archive.volve_composite;


-- ============================================================================
-- 3. THE REPROCESSED TAPE, WITH ITS SPLIT SPECIFICATION
-- ============================================================================
-- 640 frames of 26 curves. A reader that read only the first physical record
-- of the specification would see fewer curves and a narrower frame, and would
-- return a different number here without complaining about anything.

ASSERT ROW_COUNT = 640
SELECT *
FROM {{zone_name}}.log_archive.reprocessed_composite;


-- ============================================================================
-- 4. THE TAPE HAS NO WAY TO SAY "NOT RECORDED"
-- ============================================================================
-- Every one of the 21505 frames carries a number in every curve, including
-- the intervals where the tool was not run. Those read as values a hair away
-- from zero, which is a number, and averages over them are meaningless.
--
-- Density is the clearest case: it exists on 4808 frames and the tape claims
-- a value on all 21505.

ASSERT ROW_COUNT = 1
ASSERT VALUE frames = 21505
ASSERT VALUE den_claimed = 21505
ASSERT VALUE den_physical = 4808
SELECT COUNT(*)                                            AS frames,
       COUNT(*) FILTER (WHERE den IS NOT NULL)             AS den_claimed,
       COUNT(*) FILTER (WHERE den >= 1.0 AND den <= 3.5)   AS den_physical
FROM {{zone_name}}.log_archive.volve_composite;


-- ============================================================================
-- 5. AND IT CARRIES READINGS THAT CANNOT EXIST
-- ============================================================================
-- 1114 of the density correction readings are below minus one gram per cubic
-- centimetre, and the worst is minus 57338 at 2840 m. No correction is that.
-- This is what a forty-year-old tape actually holds, and it is the reason the
-- curated table applies validity rules rather than trusting the bytes.

ASSERT ROW_COUNT = 1
ASSERT VALUE impossible_denc = 1114
ASSERT VALUE worst_denc = -57338
SELECT COUNT(*)                                  AS impossible_denc,
       CAST(ROUND(MIN(denc)) AS BIGINT)          AS worst_denc,
       CAST(ROUND(MIN(dept)) AS BIGINT)          AS shallowest_m,
       CAST(ROUND(MAX(dept)) AS BIGINT)          AS deepest_m
FROM {{zone_name}}.log_archive.volve_composite
WHERE denc < -1.0;


-- The depth the worst one sits at, which is inside the logged section rather
-- than off the end of the tape. It is a bad reading, not a framing error.

ASSERT ROW_COUNT = 1
ASSERT VALUE depth_m = 2840
SELECT CAST(ROUND(dept) AS BIGINT) AS depth_m,
       CAST(ROUND(denc) AS BIGINT) AS denc
FROM {{zone_name}}.log_archive.volve_composite
ORDER BY denc ASC
LIMIT 1;


-- ============================================================================
-- 6. LOAD THE 11 MARCH BATCH
-- ============================================================================
-- The loader projects the tape onto the nine core curves and applies one
-- validity rule per curve. The rules are physical, not statistical: a caliper
-- under an inch is not a small hole, a sonic under ten microseconds per foot
-- is not a fast formation, and a density outside 1.0 to 3.5 is not rock.
--
-- NOT EXISTS on source_file is the watermark, so re-running a batch is a
-- no-op rather than a duplicate.

INSERT INTO {{zone_name}}.log_archive.tape_archive
SELECT 'VOLVE-COMPOSITE-1'                                   AS tape,
       '15/9-F-4'                                            AS well,
       '2026-03-11'                                          AS digitised_on,
       v.df_file_name                                        AS source_file,
       v.frame_number,
       v.dept,
       CASE WHEN v.gr   >= 0.01                    THEN v.gr   END AS gr,
       CASE WHEN v.cali >= 1.0                     THEN v.cali END AS cali,
       CASE WHEN v.rdep >= 0.01                    THEN v.rdep END AS rdep,
       CASE WHEN v.rmed >= 0.01                    THEN v.rmed END AS rmed,
       CASE WHEN v.den BETWEEN 1.0 AND 3.5         THEN v.den  END AS den,
       CASE WHEN v.neu  >= 0.001                   THEN v.neu  END AS neu,
       CASE WHEN v.ac   >= 10.0                    THEN v.ac   END AS ac,
       CASE WHEN v.bs   >= 1.0                     THEN v.bs   END AS bs
FROM {{zone_name}}.log_archive.volve_composite v
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.log_archive.tape_archive c
    WHERE c.source_file = v.df_file_name
);


-- ============================================================================
-- 7. THE FIRST TAPE LANDED, WITH ITS GAPS MARKED
-- ============================================================================
-- The same 21505 frames, but now the curves say where they have readings.
-- Gamma is nearly continuous over the well at 20977 frames; density, neutron
-- and sonic were run only over the lower section and have about 4800 each.

ASSERT ROW_COUNT = 1
ASSERT VALUE frames = 21505
ASSERT VALUE live_gr = 20977
ASSERT VALUE live_cali = 4834
ASSERT VALUE live_rdep = 9132
ASSERT VALUE live_den = 4808
ASSERT VALUE live_neu = 4938
ASSERT VALUE live_ac = 4815
ASSERT VALUE live_bs = 21505
ASSERT VALUE top_m = 233
ASSERT VALUE base_m = 3510
SELECT COUNT(*)                                     AS frames,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)       AS live_gr,
       COUNT(*) FILTER (WHERE cali IS NOT NULL)     AS live_cali,
       COUNT(*) FILTER (WHERE rdep IS NOT NULL)     AS live_rdep,
       COUNT(*) FILTER (WHERE den IS NOT NULL)      AS live_den,
       COUNT(*) FILTER (WHERE neu IS NOT NULL)      AS live_neu,
       COUNT(*) FILTER (WHERE ac IS NOT NULL)       AS live_ac,
       COUNT(*) FILTER (WHERE bs IS NOT NULL)       AS live_bs,
       CAST(ROUND(MIN(dept)) AS BIGINT)             AS top_m,
       CAST(ROUND(MAX(dept)) AS BIGINT)             AS base_m
FROM {{zone_name}}.log_archive.tape_archive
WHERE digitised_on = '2026-03-11';


-- ============================================================================
-- 8. THE SAME BATCH AGAIN
-- ============================================================================
-- Byte for byte the statement from step 6.

INSERT INTO {{zone_name}}.log_archive.tape_archive
SELECT 'VOLVE-COMPOSITE-1'                                   AS tape,
       '15/9-F-4'                                            AS well,
       '2026-03-11'                                          AS digitised_on,
       v.df_file_name                                        AS source_file,
       v.frame_number,
       v.dept,
       CASE WHEN v.gr   >= 0.01                    THEN v.gr   END AS gr,
       CASE WHEN v.cali >= 1.0                     THEN v.cali END AS cali,
       CASE WHEN v.rdep >= 0.01                    THEN v.rdep END AS rdep,
       CASE WHEN v.rmed >= 0.01                    THEN v.rmed END AS rmed,
       CASE WHEN v.den BETWEEN 1.0 AND 3.5         THEN v.den  END AS den,
       CASE WHEN v.neu  >= 0.001                   THEN v.neu  END AS neu,
       CASE WHEN v.ac   >= 10.0                    THEN v.ac   END AS ac,
       CASE WHEN v.bs   >= 1.0                     THEN v.bs   END AS bs
FROM {{zone_name}}.log_archive.volve_composite v
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.log_archive.tape_archive c
    WHERE c.source_file = v.df_file_name
);


-- ============================================================================
-- 9. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE frames = 21505
ASSERT VALUE tapes = 1
SELECT COUNT(*)                    AS frames,
       COUNT(DISTINCT source_file) AS tapes
FROM {{zone_name}}.log_archive.tape_archive
WHERE digitised_on = '2026-03-11';


-- ============================================================================
-- 10. LOAD THE 12 MARCH BATCH
-- ============================================================================
-- The reprocessed tape, projected onto the same nine curves. Its other
-- seventeen are read and discarded here rather than widening the table, which
-- is the difference between a curated table and a dump of whatever arrived.

INSERT INTO {{zone_name}}.log_archive.tape_archive
SELECT 'REPROCESSED-1'                                       AS tape,
       '15/9-F-4 A'                                          AS well,
       '2026-03-12'                                          AS digitised_on,
       r.df_file_name                                        AS source_file,
       r.frame_number,
       r.dept,
       CASE WHEN r.gr   >= 0.01                    THEN r.gr   END AS gr,
       CASE WHEN r.cali >= 1.0                     THEN r.cali END AS cali,
       CASE WHEN r.rdep >= 0.01                    THEN r.rdep END AS rdep,
       CASE WHEN r.rmed >= 0.01                    THEN r.rmed END AS rmed,
       CASE WHEN r.den BETWEEN 1.0 AND 3.5         THEN r.den  END AS den,
       CASE WHEN r.neu  >= 0.001                   THEN r.neu  END AS neu,
       CASE WHEN r.ac   >= 10.0                    THEN r.ac   END AS ac,
       CASE WHEN r.bs   >= 1.0                     THEN r.bs   END AS bs
FROM {{zone_name}}.log_archive.reprocessed_composite r
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.log_archive.tape_archive c
    WHERE c.source_file = r.df_file_name
);


-- ============================================================================
-- 11. BOTH BATCHES, SIDE BY SIDE
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE frames = 21505 WHERE digitised_on = '2026-03-11'
ASSERT VALUE tapes = 1 WHERE digitised_on = '2026-03-11'
ASSERT VALUE frames = 640 WHERE digitised_on = '2026-03-12'
ASSERT VALUE tapes = 1 WHERE digitised_on = '2026-03-12'
ASSERT VALUE top_m = 2700 WHERE digitised_on = '2026-03-12'
ASSERT VALUE base_m = 2797 WHERE digitised_on = '2026-03-12'
SELECT digitised_on,
       COUNT(DISTINCT source_file)        AS tapes,
       COUNT(DISTINCT well)               AS wells,
       COUNT(*)                           AS frames,
       CAST(ROUND(MIN(dept)) AS BIGINT)   AS top_m,
       CAST(ROUND(MAX(dept)) AS BIGINT)   AS base_m
FROM {{zone_name}}.log_archive.tape_archive
GROUP BY digitised_on
ORDER BY digitised_on;


-- ============================================================================
-- 12. THE WHOLE ARCHIVE
-- ============================================================================
-- 21505 + 640 = 22145 frames, from two tapes nothing else in the estate can
-- open.

ASSERT ROW_COUNT = 1
ASSERT VALUE frames = 22145
ASSERT VALUE tapes = 2
ASSERT VALUE wells = 2
SELECT COUNT(*)                    AS frames,
       COUNT(DISTINCT source_file) AS tapes,
       COUNT(DISTINCT well)        AS wells
FROM {{zone_name}}.log_archive.tape_archive;


-- ============================================================================
-- 13. EVERY TAPE LANDED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE frames = 21505 WHERE source_file = '2026-03-11_15_9-F-4_WLC_COMPOSITE_1.lis'
ASSERT VALUE frames = 640 WHERE source_file = '2026-03-12_15_9-F-4A_REPROCESSED.lis'
SELECT source_file,
       MIN(tape)   AS tape,
       COUNT(*)    AS frames
FROM {{zone_name}}.log_archive.tape_archive
GROUP BY source_file
ORDER BY source_file;


-- ============================================================================
-- 14. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT v.df_file_name
FROM {{zone_name}}.log_archive.volve_composite v
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.log_archive.tape_archive c
    WHERE c.source_file = v.df_file_name
);


-- ============================================================================
-- 15. THE STATE AFTER THE FIRST BATCH, BY TIME TRAVEL
-- ============================================================================
-- Version 0 is the empty table the setup created and version 1 is the first
-- batch, so version 1 is the Volve tape and nothing else.

ASSERT ROW_COUNT = 21505
SELECT *
FROM {{zone_name}}.log_archive.tape_archive VERSION AS OF 1;


-- ============================================================================
-- 16. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.log_archive.tape_archive;


-- ============================================================================
-- 17. THE HOLE SECTIONS, FROM THE BIT SIZE CURVE
-- ============================================================================
-- Four bit sizes on the Volve tape: 36, 17.5, 12.25 and 8.5 inches. That is
-- the well's casing programme, recovered from a tape rather than from a
-- drilling report, and it is the kind of question the archive was digitised
-- to answer.

ASSERT ROW_COUNT = 4
ASSERT RESULT SET ORDERED (8.5), (12.25), (17.5), (36.0)
SELECT bs AS bit_size_in
FROM {{zone_name}}.log_archive.tape_archive
WHERE well = '15/9-F-4'
GROUP BY bs
ORDER BY bs;


-- ============================================================================
-- 18. NET PAY OVER THE LOGGED SECTION
-- ============================================================================
-- The evaluation the tape was digitised for. Clean sand is gamma under 60
-- gAPI; density porosity is the standard sandstone transform, (2.65 - rhob)
-- over (2.65 - 1.0). 1096 frames qualify, between 2772 and 3423 m.
--
-- Without the validity rules from step 6 this query would have returned the
-- whole well, because the unlogged intervals read as a density near zero and
-- a density near zero is a porosity of 1.6.

ASSERT ROW_COUNT = 1
ASSERT VALUE net_reservoir = 3627
ASSERT VALUE net_pay = 1096
ASSERT VALUE pay_top_m = 2772
ASSERT VALUE pay_base_m = 3423
SELECT COUNT(*)                                                    AS net_reservoir,
       COUNT(*) FILTER (WHERE (2.65 - den) / 1.65 > 0.15)          AS net_pay,
       CAST(ROUND(MIN(dept) FILTER (WHERE (2.65 - den) / 1.65 > 0.15)) AS BIGINT) AS pay_top_m,
       CAST(ROUND(MAX(dept) FILTER (WHERE (2.65 - den) / 1.65 > 0.15)) AS BIGINT) AS pay_base_m
FROM {{zone_name}}.log_archive.tape_archive
WHERE well = '15/9-F-4'
  AND gr < 60
  AND den IS NOT NULL;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One row per tape, carrying everything the recovery promised: the frames
-- came off the tape once, the depths are real, and each curve reports only
-- where the tool was actually recording.

ASSERT ROW_COUNT = 2
ASSERT VALUE frames = 21505 WHERE well = '15/9-F-4'
ASSERT VALUE top_m = 233 WHERE well = '15/9-F-4'
ASSERT VALUE base_m = 3510 WHERE well = '15/9-F-4'
ASSERT VALUE live_gr = 20977 WHERE well = '15/9-F-4'
ASSERT VALUE live_den = 4808 WHERE well = '15/9-F-4'
ASSERT VALUE frames = 640 WHERE well = '15/9-F-4 A'
ASSERT VALUE top_m = 2700 WHERE well = '15/9-F-4 A'
ASSERT VALUE base_m = 2797 WHERE well = '15/9-F-4 A'
ASSERT VALUE live_gr = 640 WHERE well = '15/9-F-4 A'
ASSERT VALUE live_den = 640 WHERE well = '15/9-F-4 A'
SELECT well,
       MIN(tape)                                  AS tape,
       MIN(digitised_on)                          AS digitised_on,
       COUNT(*)                                   AS frames,
       CAST(ROUND(MIN(dept)) AS BIGINT)           AS top_m,
       CAST(ROUND(MAX(dept)) AS BIGINT)           AS base_m,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)     AS live_gr,
       COUNT(*) FILTER (WHERE den IS NOT NULL)    AS live_den
FROM {{zone_name}}.log_archive.tape_archive
GROUP BY well
ORDER BY well;
