-- ============================================================================
-- Reservoir Simulation History Match Review - Incremental Load and Verification
-- ============================================================================
-- Two history-match iterations, delivered a day apart, four files each:
--
--   2026-03-11  HM12   EGRID 2366, INIT 2399, UNRST 13830, SMSPEC 29 elements
--   2026-03-12  HM13   the same shape, a different aquifer strength
--
-- 37248 array elements in total. Every count was decoded from the files by a
-- second, independent ECLIPSE reader before the engine saw them.
--
-- The grid is 12 x 12 x 8, which is 1152 cells, and that number matters: an
-- ECLIPSE numeric array is written in records of at most 1000 elements, so
-- every property here spans TWO records. A reader that assumes one record per
-- keyword returns the first thousand cells and no error, which is a model
-- with a corner missing and nothing to say so.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.simulation.sim_arrays
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. EVERY ARRAY ELEMENT IN BOTH RUNS
-- ============================================================================

ASSERT ROW_COUNT = 37248
SELECT *
FROM {{zone_name}}.simulation.sim_arrays;


-- ============================================================================
-- 3. WHAT EACH FILE HOLDS
-- ============================================================================
-- Four file types, four different keyword sets, one table. This is the
-- inventory query an engineer runs before anything else, and it is the reason
-- long form is worth the awkwardness.

ASSERT ROW_COUNT = 8
ASSERT VALUE elements = 2366 WHERE df_file_name = '2026-03-11_HM12.EGRID'
ASSERT VALUE keywords = 4 WHERE df_file_name = '2026-03-11_HM12.EGRID'
ASSERT VALUE elements = 2399 WHERE df_file_name = '2026-03-11_HM12.INIT'
ASSERT VALUE elements = 13830 WHERE df_file_name = '2026-03-11_HM12.UNRST'
ASSERT VALUE keywords = 3 WHERE df_file_name = '2026-03-11_HM12.UNRST'
ASSERT VALUE elements = 29 WHERE df_file_name = '2026-03-11_HM12.SMSPEC'
ASSERT VALUE elements = 13830 WHERE df_file_name = '2026-03-12_HM13.UNRST'
SELECT df_file_name,
       COUNT(DISTINCT keyword) AS keywords,
       COUNT(*)                AS elements
FROM {{zone_name}}.simulation.sim_arrays
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. EVERY PROPERTY ARRAY SPANS TWO RECORDS
-- ============================================================================
-- 1152 cells against a 1000-element record limit. If the second record were
-- being dropped, every one of these would read 1000 and the model would be
-- missing its last 152 cells.

ASSERT ROW_COUNT = 4
ASSERT VALUE elements = 1152 WHERE keyword = 'ACTNUM'
ASSERT VALUE elements = 1152 WHERE keyword = 'PORO'
ASSERT VALUE elements = 1152 WHERE keyword = 'PERMX'
ASSERT VALUE elements = 1014 WHERE keyword = 'COORD'
SELECT keyword,
       COUNT(*) AS elements
FROM {{zone_name}}.simulation.sim_arrays
WHERE df_file_name = '2026-03-11_HM12.EGRID'
   OR df_file_name = '2026-03-11_HM12.INIT'
GROUP BY keyword
HAVING COUNT(*) > 1000
ORDER BY keyword;


-- ============================================================================
-- 5. OCCURRENCE SEPARATES THE REPORT STEPS
-- ============================================================================
-- A restart file writes PRESSURE once per report step, so the six arrays all
-- share a keyword and differ only by occurrence. Six occurrences of 1152
-- cells is 6912 pressures.

ASSERT ROW_COUNT = 6
ASSERT VALUE cells = 1152 WHERE occurrence = 0
ASSERT VALUE cells = 1152 WHERE occurrence = 5
ASSERT VALUE mean_pressure = 310 WHERE occurrence = 0
ASSERT VALUE mean_pressure = 292 WHERE occurrence = 1
ASSERT VALUE mean_pressure = 277 WHERE occurrence = 2
ASSERT VALUE mean_pressure = 263 WHERE occurrence = 3
ASSERT VALUE mean_pressure = 252 WHERE occurrence = 4
ASSERT VALUE mean_pressure = 243 WHERE occurrence = 5
SELECT occurrence,
       COUNT(*)                          AS cells,
       CAST(ROUND(AVG(value)) AS BIGINT) AS mean_pressure
FROM {{zone_name}}.simulation.sim_arrays
WHERE df_file_name = '2026-03-11_HM12.UNRST'
  AND keyword = 'PRESSURE'
GROUP BY occurrence
ORDER BY occurrence;


-- ============================================================================
-- 6. THE ACTIVE CELL COUNT
-- ============================================================================
-- ACTNUM is 1 for a cell the simulator solves and 0 for one it does not. The
-- model is cut to a fault block, so its top layer has an inactive rim: 1108
-- of 1152.

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 1152
ASSERT VALUE active = 1108
ASSERT VALUE inactive = 44
SELECT COUNT(*)                                AS cells,
       COUNT(*) FILTER (WHERE value = 1)       AS active,
       COUNT(*) FILTER (WHERE value = 0)       AS inactive
FROM {{zone_name}}.simulation.sim_arrays
WHERE df_file_name = '2026-03-11_HM12.EGRID'
  AND keyword = 'ACTNUM';


-- ============================================================================
-- 7. CHARACTER ARRAYS LAND IN THE TEXT COLUMN
-- ============================================================================
-- A summary file names its vectors as 8-character strings rather than
-- numbers, so those rows carry element_type CHAR and a text value with the
-- numeric value null. Three wells times three vectors is nine of each.

ASSERT ROW_COUNT = 3
ASSERT VALUE elements = 9 WHERE keyword = 'KEYWORDS'
ASSERT VALUE distinct_values = 3 WHERE keyword = 'KEYWORDS'
ASSERT VALUE elements = 9 WHERE keyword = 'WGNAMES'
ASSERT VALUE distinct_values = 3 WHERE keyword = 'WGNAMES'
ASSERT VALUE elements = 9 WHERE keyword = 'UNITS'
ASSERT VALUE numeric_values = 0 WHERE keyword = 'KEYWORDS'
SELECT keyword,
       COUNT(*)                                 AS elements,
       COUNT(DISTINCT text)                     AS distinct_values,
       COUNT(*) FILTER (WHERE value IS NOT NULL) AS numeric_values
FROM {{zone_name}}.simulation.sim_arrays
WHERE df_file_name = '2026-03-11_HM12.SMSPEC'
  AND element_type = 'CHAR'
GROUP BY keyword
ORDER BY keyword;


-- ============================================================================
-- 8. THE WELL VECTOR CATALOGUE
-- ============================================================================
-- Which wells the run reports and what it reports for them, recovered from
-- the two character arrays by lining them up on their element index.

ASSERT ROW_COUNT = 9
ASSERT RESULT SET INCLUDES ('INJ-1', 'WBHP'), ('PROD-1', 'WOPR'), ('PROD-2', 'WWPR')
SELECT n.text AS well,
       k.text AS vector
FROM {{zone_name}}.simulation.sim_arrays k
JOIN {{zone_name}}.simulation.sim_arrays n
  ON n.df_file_name = k.df_file_name
 AND n.element_index = k.element_index
 AND n.keyword = 'WGNAMES'
WHERE k.df_file_name = '2026-03-11_HM12.SMSPEC'
  AND k.keyword = 'KEYWORDS'
ORDER BY well, vector;


-- ============================================================================
-- 9. LOAD THE 11 MARCH RUN
-- ============================================================================
-- The pivot. Pressure and saturation come from the restart file and porosity
-- from the initialisation file, and all three line up because the element
-- index IS the cell index.

INSERT INTO {{zone_name}}.simulation.cell_pressure
SELECT 'HM12'                AS run,
       '2026-03-11'          AS delivered_on,
       p.df_file_name        AS source_file,
       p.occurrence          AS report_step,
       p.element_index       AS cell_index,
       p.value               AS pressure,
       s.value               AS swat,
       i.value               AS poro
FROM {{zone_name}}.simulation.sim_arrays p
JOIN {{zone_name}}.simulation.sim_arrays s
  ON s.df_file_name = p.df_file_name
 AND s.keyword = 'SWAT'
 AND s.occurrence = p.occurrence
 AND s.element_index = p.element_index
JOIN {{zone_name}}.simulation.sim_arrays i
  ON i.df_file_name = '2026-03-11_HM12.INIT'
 AND i.keyword = 'PORO'
 AND i.element_index = p.element_index
WHERE p.df_file_name = '2026-03-11_HM12.UNRST'
  AND p.keyword = 'PRESSURE'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.simulation.cell_pressure c
      WHERE c.source_file = p.df_file_name
  );


-- ============================================================================
-- 10. THE FIRST RUN IS PIVOTED
-- ============================================================================
-- 6 report steps of 1152 cells: 6912 rows, each carrying what long form held
-- in three separate places.

ASSERT ROW_COUNT = 1
ASSERT VALUE rows_loaded = 6912
ASSERT VALUE steps = 6
ASSERT VALUE cells = 1152
SELECT COUNT(*)                        AS rows_loaded,
       COUNT(DISTINCT report_step)     AS steps,
       COUNT(DISTINCT cell_index)      AS cells
FROM {{zone_name}}.simulation.cell_pressure
WHERE run = 'HM12';


-- ============================================================================
-- 11. THE SAME RUN AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.simulation.cell_pressure
SELECT 'HM12'                AS run,
       '2026-03-11'          AS delivered_on,
       p.df_file_name        AS source_file,
       p.occurrence          AS report_step,
       p.element_index       AS cell_index,
       p.value               AS pressure,
       s.value               AS swat,
       i.value               AS poro
FROM {{zone_name}}.simulation.sim_arrays p
JOIN {{zone_name}}.simulation.sim_arrays s
  ON s.df_file_name = p.df_file_name
 AND s.keyword = 'SWAT'
 AND s.occurrence = p.occurrence
 AND s.element_index = p.element_index
JOIN {{zone_name}}.simulation.sim_arrays i
  ON i.df_file_name = '2026-03-11_HM12.INIT'
 AND i.keyword = 'PORO'
 AND i.element_index = p.element_index
WHERE p.df_file_name = '2026-03-11_HM12.UNRST'
  AND p.keyword = 'PRESSURE'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.simulation.cell_pressure c
      WHERE c.source_file = p.df_file_name
  );


-- ============================================================================
-- 12. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE rows_loaded = 6912
SELECT COUNT(*) AS rows_loaded
FROM {{zone_name}}.simulation.cell_pressure
WHERE run = 'HM12';


-- ============================================================================
-- 13. LOAD THE 12 MARCH RUN
-- ============================================================================

INSERT INTO {{zone_name}}.simulation.cell_pressure
SELECT 'HM13'                AS run,
       '2026-03-12'          AS delivered_on,
       p.df_file_name        AS source_file,
       p.occurrence          AS report_step,
       p.element_index       AS cell_index,
       p.value               AS pressure,
       s.value               AS swat,
       i.value               AS poro
FROM {{zone_name}}.simulation.sim_arrays p
JOIN {{zone_name}}.simulation.sim_arrays s
  ON s.df_file_name = p.df_file_name
 AND s.keyword = 'SWAT'
 AND s.occurrence = p.occurrence
 AND s.element_index = p.element_index
JOIN {{zone_name}}.simulation.sim_arrays i
  ON i.df_file_name = '2026-03-12_HM13.INIT'
 AND i.keyword = 'PORO'
 AND i.element_index = p.element_index
WHERE p.df_file_name = '2026-03-12_HM13.UNRST'
  AND p.keyword = 'PRESSURE'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.simulation.cell_pressure c
      WHERE c.source_file = p.df_file_name
  );


-- ============================================================================
-- 14. BOTH RUNS
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE rows_loaded = 6912 WHERE run = 'HM12'
ASSERT VALUE delivered_on = '2026-03-11' WHERE run = 'HM12'
ASSERT VALUE rows_loaded = 6912 WHERE run = 'HM13'
ASSERT VALUE delivered_on = '2026-03-12' WHERE run = 'HM13'
SELECT run,
       MIN(delivered_on)           AS delivered_on,
       COUNT(*)                    AS rows_loaded,
       COUNT(DISTINCT report_step) AS steps,
       COUNT(DISTINCT cell_index)  AS cells
FROM {{zone_name}}.simulation.cell_pressure
GROUP BY run
ORDER BY run;


-- ============================================================================
-- 15. THE PRESSURE DECLINE, RUN AGAINST RUN
-- ============================================================================
-- The comparison the review exists for. Both runs start at the same initial
-- pressure and diverge from the first step, because HM12's aquifer does not
-- support the reservoir.

ASSERT ROW_COUNT = 6
ASSERT VALUE hm12 = 310 WHERE report_step = 0
ASSERT VALUE hm13 = 310 WHERE report_step = 0
ASSERT VALUE hm12 = 292 WHERE report_step = 1
ASSERT VALUE hm13 = 300 WHERE report_step = 1
ASSERT VALUE hm12 = 243 WHERE report_step = 5
ASSERT VALUE hm13 = 272 WHERE report_step = 5
SELECT report_step,
       CAST(ROUND(AVG(pressure) FILTER (WHERE run = 'HM12')) AS BIGINT) AS hm12,
       CAST(ROUND(AVG(pressure) FILTER (WHERE run = 'HM13')) AS BIGINT) AS hm13
FROM {{zone_name}}.simulation.cell_pressure
GROUP BY report_step
ORDER BY report_step;


-- ============================================================================
-- 16. WHICH RUN MATCHES THE OBSERVED DECLINE
-- ============================================================================
-- The observed pressures come from the production database, not from the
-- simulator, so they are supplied here as the values they are. HM13 is closer
-- at every step: a mean absolute error of 4 bar against HM12's 20, and a
-- worst step of 8 bar against 37.

ASSERT ROW_COUNT = 2
ASSERT VALUE mean_absolute_error = 20 WHERE run = 'HM12'
ASSERT VALUE mean_absolute_error = 4 WHERE run = 'HM13'
ASSERT VALUE worst_step_error = 37 WHERE run = 'HM12'
ASSERT VALUE worst_step_error = 8 WHERE run = 'HM13'
WITH observed(report_step, observed_pressure) AS (
    VALUES (0, 310.0), (1, 302.0), (2, 295.0), (3, 289.0), (4, 284.0), (5, 280.0)
),
modelled AS (
    SELECT run, report_step, AVG(pressure) AS modelled_pressure
    FROM {{zone_name}}.simulation.cell_pressure
    GROUP BY run, report_step
)
SELECT m.run,
       CAST(ROUND(AVG(ABS(m.modelled_pressure - o.observed_pressure))) AS BIGINT) AS mean_absolute_error,
       CAST(ROUND(MAX(ABS(m.modelled_pressure - o.observed_pressure))) AS BIGINT) AS worst_step_error
FROM modelled m
JOIN observed o ON o.report_step = m.report_step
GROUP BY m.run
ORDER BY m.run;


-- ============================================================================
-- 17. STATIC AND DYNAMIC PROPERTIES ON THE SAME CELL
-- ============================================================================
-- Porosity came from the initialisation file and pressure from the restart
-- file, and they meet on the cell index. Every one of the 6912 rows has both,
-- which is the check that the two files were lined up correctly rather than
-- joined into a partial result.

ASSERT ROW_COUNT = 1
ASSERT VALUE rows_loaded = 13824
ASSERT VALUE with_porosity = 13824
ASSERT VALUE without_porosity = 0
SELECT COUNT(*)                                     AS rows_loaded,
       COUNT(*) FILTER (WHERE poro IS NOT NULL)     AS with_porosity,
       COUNT(*) FILTER (WHERE poro IS NULL)         AS without_porosity
FROM {{zone_name}}.simulation.cell_pressure;


-- ============================================================================
-- 18. THE STATE AFTER THE FIRST RUN, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 6912
SELECT *
FROM {{zone_name}}.simulation.cell_pressure VERSION AS OF 1;


-- ============================================================================
-- 19. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.simulation.cell_pressure;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The review summary: both runs, their final pressure, and how far each ended
-- from the observed 280 bar.

ASSERT ROW_COUNT = 2
ASSERT VALUE final_pressure = 243 WHERE run = 'HM12'
ASSERT VALUE miss_at_end = 37 WHERE run = 'HM12'
ASSERT VALUE final_pressure = 272 WHERE run = 'HM13'
ASSERT VALUE miss_at_end = 8 WHERE run = 'HM13'
ASSERT VALUE cells = 1152 WHERE run = 'HM12'
ASSERT VALUE cells = 1152 WHERE run = 'HM13'
SELECT run,
       MIN(delivered_on)                            AS delivered_on,
       COUNT(DISTINCT cell_index)                   AS cells,
       COUNT(DISTINCT report_step)                  AS steps,
       CAST(ROUND(AVG(pressure)) AS BIGINT)         AS final_pressure,
       CAST(ROUND(ABS(AVG(pressure) - 280.0)) AS BIGINT) AS miss_at_end
FROM {{zone_name}}.simulation.cell_pressure
WHERE report_step = 5
GROUP BY run
ORDER BY run;
