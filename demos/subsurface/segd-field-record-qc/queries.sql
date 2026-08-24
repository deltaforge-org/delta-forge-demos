-- ============================================================================
-- SEG-D Field Record QC - Incremental Load and Verification
-- ============================================================================
-- Two acquisition days have landed:
--
--   2026-03-11   fr_1041, fr_1042, fr_1043    72 traces
--   2026-03-12   fr_1044, fr_1045, fr_1046    66 traces
--
-- The loader runs once per acquisition date. It appends the traces of every
-- record in that day's drop that the curated table does not already hold, and
-- the record's own file name is the watermark, so a re-run adds nothing.
--
-- The middle of this file runs the loader three times: once for 11 March,
-- once for 11 March AGAIN, and once for 12 March. The assertion after the
-- second run is the one that matters, because a loader that reloads instead
-- of appending would double the day's traces there and nowhere else.
--
-- Every number was computed by decoding the SEG-D files independently of the
-- generator that wrote them, so a failure is an engine change rather than a
-- literal that drifted.
--
-- Two faults are planted in the second day's drop and the QC queries at the
-- end find both: record 1044 is short of channels, and record 1045 came off a
-- truck still configured for a different sample interval.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- PRINT returns the CREATE EXTERNAL TABLE that DISCOVER generated, without
-- running anything. The USING clause is the point: SEG-D was identified from
-- the general header, not from the .segd extension, and the options carried
-- through unchanged.

DISCOVER {{zone_name}}.seismic_acquisition.field_records
    PATH '{{data_subdir}}/landing'
    WITH (
        FILE_METADATA = true,
        include_samples = 'false'
    )
    PRINT;


-- ============================================================================
-- 2. THE LANDING FOLDER
-- ============================================================================
-- Everything on disk, read in place. Five full 24-channel records plus one
-- short one: 24 * 5 + 18 = 138.

ASSERT ROW_COUNT = 138
SELECT *
FROM {{zone_name}}.seismic_acquisition.field_records;


-- ============================================================================
-- 3. THE DROPS, AS THE LOADER SEES THEM
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE traces = 24 WHERE df_file_name = '2026-03-11_fr_1041.segd'
ASSERT VALUE traces = 24 WHERE df_file_name = '2026-03-11_fr_1042.segd'
ASSERT VALUE traces = 24 WHERE df_file_name = '2026-03-11_fr_1043.segd'
ASSERT VALUE traces = 18 WHERE df_file_name = '2026-03-12_fr_1044.segd'
ASSERT VALUE traces = 24 WHERE df_file_name = '2026-03-12_fr_1045.segd'
ASSERT VALUE traces = 24 WHERE df_file_name = '2026-03-12_fr_1046.segd'
SELECT df_file_name,
       COUNT(*)                AS traces,
       MIN(file_number)        AS file_number,
       MIN(sample_count)       AS sample_count,
       MIN(sample_interval_us) AS sample_interval_us
FROM {{zone_name}}.seismic_acquisition.field_records
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. LOAD THE 11 MARCH DROP
-- ============================================================================
-- The scheduled run for one acquisition date. NOT EXISTS against the curated
-- table is the watermark: a record whose traces are already present is
-- skipped whole, so the loader never has to know what it did last time.

INSERT INTO {{zone_name}}.seismic_acquisition.trace_inventory
SELECT '2026-03-11'   AS acquisition_date,
       l.df_file_name AS source_file,
       l.file_number,
       l.scan_type,
       l.channel_set,
       l.trace_number,
       l.sample_count,
       l.sample_interval_us
FROM {{zone_name}}.seismic_acquisition.field_records l
WHERE l.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.seismic_acquisition.trace_inventory c
      WHERE c.source_file = l.df_file_name
  );


-- ============================================================================
-- 5. THE FIRST DAY LANDED
-- ============================================================================
-- Three records, 72 traces. Asserted per acquisition date rather than as a
-- running total, so the number stays true however many times this file runs.

ASSERT ROW_COUNT = 1
ASSERT VALUE traces = 72
ASSERT VALUE records = 3
SELECT COUNT(*)                       AS traces,
       COUNT(DISTINCT source_file)    AS records
FROM {{zone_name}}.seismic_acquisition.trace_inventory
WHERE acquisition_date = '2026-03-11';


-- ============================================================================
-- 6. THE SAME RUN AGAIN
-- ============================================================================
-- Byte for byte the statement from step 4. A loader that reloads its source
-- would put 144 traces on 11 March; an incremental one leaves 72.

INSERT INTO {{zone_name}}.seismic_acquisition.trace_inventory
SELECT '2026-03-11'   AS acquisition_date,
       l.df_file_name AS source_file,
       l.file_number,
       l.scan_type,
       l.channel_set,
       l.trace_number,
       l.sample_count,
       l.sample_interval_us
FROM {{zone_name}}.seismic_acquisition.field_records l
WHERE l.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.seismic_acquisition.trace_inventory c
      WHERE c.source_file = l.df_file_name
  );


-- ============================================================================
-- 7. THE RE-RUN ADDED NOTHING
-- ============================================================================
-- The assertion the whole demo turns on.

ASSERT ROW_COUNT = 1
ASSERT VALUE traces = 72
ASSERT VALUE records = 3
SELECT COUNT(*)                       AS traces,
       COUNT(DISTINCT source_file)    AS records
FROM {{zone_name}}.seismic_acquisition.trace_inventory
WHERE acquisition_date = '2026-03-11';


-- ============================================================================
-- 8. LOAD THE 12 MARCH DROP
-- ============================================================================
-- The next day's scheduled run. Same statement, different date.

INSERT INTO {{zone_name}}.seismic_acquisition.trace_inventory
SELECT '2026-03-12'   AS acquisition_date,
       l.df_file_name AS source_file,
       l.file_number,
       l.scan_type,
       l.channel_set,
       l.trace_number,
       l.sample_count,
       l.sample_interval_us
FROM {{zone_name}}.seismic_acquisition.field_records l
WHERE l.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.seismic_acquisition.trace_inventory c
      WHERE c.source_file = l.df_file_name
  );


-- ============================================================================
-- 9. BOTH DAYS, SIDE BY SIDE
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE traces = 72 WHERE acquisition_date = '2026-03-11'
ASSERT VALUE records = 3 WHERE acquisition_date = '2026-03-11'
ASSERT VALUE traces = 66 WHERE acquisition_date = '2026-03-12'
ASSERT VALUE records = 3 WHERE acquisition_date = '2026-03-12'
SELECT acquisition_date,
       COUNT(DISTINCT source_file) AS records,
       COUNT(*)                    AS traces
FROM {{zone_name}}.seismic_acquisition.trace_inventory
GROUP BY acquisition_date
ORDER BY acquisition_date;


-- ============================================================================
-- 10. EVERY RECORD LANDED EXACTLY ONCE
-- ============================================================================
-- The curated trace count per record has to equal the trace count the file
-- itself holds. A duplicated load shows up here as a doubled count on every
-- record, and a partial load as a short one, so this is the check that does
-- not depend on knowing which day the demo is on.

ASSERT ROW_COUNT = 0
SELECT c.source_file,
       c.curated_traces,
       l.landed_traces
FROM (
    SELECT source_file, COUNT(*) AS curated_traces
    FROM {{zone_name}}.seismic_acquisition.trace_inventory
    GROUP BY source_file
) c
JOIN (
    SELECT df_file_name, COUNT(*) AS landed_traces
    FROM {{zone_name}}.seismic_acquisition.field_records
    GROUP BY df_file_name
) l
  ON l.df_file_name = c.source_file
WHERE c.curated_traces <> l.landed_traces;


-- ============================================================================
-- 11. NOTHING WAS LEFT BEHIND EITHER
-- ============================================================================
-- The other half of the same question: every record on disk reached the
-- curated table.

ASSERT ROW_COUNT = 0
SELECT l.df_file_name
FROM {{zone_name}}.seismic_acquisition.field_records l
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.seismic_acquisition.trace_inventory c
    WHERE c.source_file = l.df_file_name
);


-- ============================================================================
-- 12. THE STATE AFTER THE FIRST DROP, BY TIME TRAVEL
-- ============================================================================
-- Version 0 is the empty table the setup created and version 1 is the first
-- load, so version 1 is 11 March and nothing else. That the history still
-- holds it is what makes an incremental table auditable: the question "what
-- did we know on the eleventh" has an answer that does not depend on anyone
-- having kept a copy.

ASSERT ROW_COUNT = 72
SELECT *
FROM {{zone_name}}.seismic_acquisition.trace_inventory VERSION AS OF 1;


-- ============================================================================
-- 13. THE LOAD HISTORY
-- ============================================================================
-- One commit per load that had something to write.

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.seismic_acquisition.trace_inventory;


-- ============================================================================
-- 14. QC: CHANNEL COUNT PER FIELD RECORD
-- ============================================================================
-- Now the curated table earns its keep. This is the question the acquisition
-- log cannot answer honestly, because the log records what the crew intended
-- to shoot.

ASSERT ROW_COUNT = 6
ASSERT VALUE channels = 24 WHERE file_number = 1041
ASSERT VALUE channels = 24 WHERE file_number = 1042
ASSERT VALUE channels = 24 WHERE file_number = 1043
ASSERT VALUE channels = 18 WHERE file_number = 1044
ASSERT VALUE channels = 24 WHERE file_number = 1045
ASSERT VALUE channels = 24 WHERE file_number = 1046
ASSERT VALUE last_channel = 18 WHERE file_number = 1044
SELECT file_number,
       MIN(acquisition_date) AS acquisition_date,
       COUNT(*)              AS channels,
       MIN(trace_number)     AS first_channel,
       MAX(trace_number)     AS last_channel
FROM {{zone_name}}.seismic_acquisition.trace_inventory
GROUP BY file_number
ORDER BY file_number;


-- ============================================================================
-- 15. QC: SAMPLE GEOMETRY IS NOT UNIFORM
-- ============================================================================
-- The second planted fault. One record was written at 2 ms and 1024 samples
-- while the rest of the acquisition is 4 ms and 512, which is a reprocessing
-- job if it reaches the processing centre unflagged. The sample count is
-- derived from the record-length field and the base scan interval in the
-- general header, so this is the file's own arithmetic rather than a guess
-- from the file size.

ASSERT ROW_COUNT = 2
ASSERT VALUE records = 5 WHERE sample_interval_us = 4000
ASSERT VALUE traces = 114 WHERE sample_interval_us = 4000
ASSERT VALUE sample_count = 512 WHERE sample_interval_us = 4000
ASSERT VALUE records = 1 WHERE sample_interval_us = 2000
ASSERT VALUE traces = 24 WHERE sample_interval_us = 2000
ASSERT VALUE sample_count = 1024 WHERE sample_interval_us = 2000
SELECT sample_interval_us,
       MIN(sample_count)           AS sample_count,
       COUNT(DISTINCT file_number) AS records,
       COUNT(*)                    AS traces
FROM {{zone_name}}.seismic_acquisition.trace_inventory
GROUP BY sample_interval_us
ORDER BY sample_interval_us;


-- ============================================================================
-- 16. THE TAPE LABEL AGREES WITH THE HEADERS
-- ============================================================================
-- The file number lives in binary-coded decimal in both the general header
-- and every trace header. A record whose headers disagree with the name it
-- was archived under is a record nobody can trace back to a sweep point, so
-- the count that matters here is zero.
--
-- This is also the assertion that would fail if the BCD nibbles were read as
-- plain bytes: 1041 is stored as 0x10 0x41 and reads as 4161 that way.

ASSERT ROW_COUNT = 0
SELECT source_file, file_number
FROM {{zone_name}}.seismic_acquisition.trace_inventory
WHERE STRPOS(source_file, CONCAT('fr_', CAST(file_number AS VARCHAR), '.segd')) = 0;


-- ============================================================================
-- 17. THE SAMPLE PAYLOAD IS THERE WHEN IT IS ASKED FOR
-- ============================================================================
-- The QC table skipped sample decoding. The same reader over the same record
-- with decoding left on returns the traces with their samples attached.

ASSERT ROW_COUNT = 1
ASSERT VALUE traces = 24
ASSERT VALUE traces_with_samples = 24
SELECT COUNT(*)                                            AS traces,
       COUNT(*) FILTER (WHERE samples IS NOT NULL)         AS traces_with_samples
FROM {{zone_name}}.seismic_acquisition.record_1043;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One row per field record with its QC verdict, drawn from the curated Delta
-- table rather than from the files: the whole point of the load is that this
-- query costs nothing and does not need the tapes mounted.

ASSERT ROW_COUNT = 6
ASSERT VALUE verdict = 'OK' WHERE file_number = 1041
ASSERT VALUE verdict = 'OK' WHERE file_number = 1042
ASSERT VALUE verdict = 'OK' WHERE file_number = 1043
ASSERT VALUE verdict = 'SHORT SWATH' WHERE file_number = 1044
ASSERT VALUE verdict = 'GEOMETRY MISMATCH' WHERE file_number = 1045
ASSERT VALUE verdict = 'OK' WHERE file_number = 1046
ASSERT VALUE channels = 18 WHERE file_number = 1044
ASSERT VALUE sample_interval_us = 2000 WHERE file_number = 1045
ASSERT VALUE sample_count = 1024 WHERE file_number = 1045
ASSERT VALUE acquisition_date = '2026-03-11' WHERE file_number = 1041
ASSERT VALUE acquisition_date = '2026-03-12' WHERE file_number = 1044
SELECT file_number,
       MIN(acquisition_date)     AS acquisition_date,
       COUNT(*)                  AS channels,
       MIN(sample_count)         AS sample_count,
       MIN(sample_interval_us)   AS sample_interval_us,
       MIN(channel_set)          AS channel_set,
       CASE
           WHEN COUNT(*) < 24                   THEN 'SHORT SWATH'
           WHEN MIN(sample_interval_us) <> 4000 THEN 'GEOMETRY MISMATCH'
           ELSE 'OK'
       END                       AS verdict
FROM {{zone_name}}.seismic_acquisition.trace_inventory
GROUP BY file_number
ORDER BY file_number;
