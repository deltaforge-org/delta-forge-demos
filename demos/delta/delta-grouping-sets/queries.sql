-- ============================================================================
-- Manufacturing Production Reporting — Educational Queries
-- ============================================================================
-- WHAT: Multi-level production reports using GROUPING SETS, CUBE, ROLLUP
-- WHY:  Factory management needs subtotals at many granularities from one query
-- HOW:  GROUPING SETS define which grouping combinations to produce;
--       ROLLUP generates hierarchical subtotals; CUBE generates all combinations;
--       GROUPING() distinguishes aggregated NULLs from real NULLs
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Simple GROUP BY production_line
-- ============================================================================
-- Standard GROUP BY produces one aggregation level: per-line totals.
-- This is the starting point before introducing multi-level groupings.
-- 3 lines x 12 runs each = 36 total rows aggregated into 3 output rows.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_units = 12120 WHERE production_line = 'Line-A'
ASSERT VALUE total_units = 12720 WHERE production_line = 'Line-C'
SELECT production_line,
       SUM(units_produced)   AS total_units,
       SUM(defect_count)     AS total_defects,
       ROUND(AVG(units_produced * 1.0), 2) AS avg_units,
       ROUND(SUM(defect_count) * 100.0 / SUM(units_produced), 2) AS defect_rate_pct
FROM {{zone_name}}.delta_demos.production_runs
GROUP BY production_line
ORDER BY production_line;


-- ============================================================================
-- LEARN: GROUPING SETS — Multiple Aggregation Levels in One Query
-- ============================================================================
-- GROUPING SETS lets you specify exactly which grouping combinations to compute.
-- Here we request three levels in a single scan of the data:
--   (production_line)  — 3 rows, one per line
--   (shift)            — 3 rows, one per shift
--   ()                 — 1 row, grand total
-- Total: 3 + 3 + 1 = 7 rows.
-- Columns not in the current grouping appear as NULL in the output.

ASSERT ROW_COUNT = 7
ASSERT VALUE total_units = 12720 WHERE production_line = 'Line-C' AND shift IS NULL
ASSERT VALUE total_units = 12720 WHERE shift = 'Morning' AND production_line IS NULL
ASSERT VALUE total_units = 36360 WHERE production_line IS NULL AND shift IS NULL
SELECT production_line,
       shift,
       SUM(units_produced)   AS total_units,
       SUM(defect_count)     AS total_defects,
       ROUND(SUM(defect_count) * 100.0 / SUM(units_produced), 2) AS defect_rate_pct
FROM {{zone_name}}.delta_demos.production_runs
GROUP BY GROUPING SETS (
    (production_line),
    (shift),
    ()
)
ORDER BY production_line NULLS LAST, shift NULLS LAST;


-- ============================================================================
-- LEARN: ROLLUP — Hierarchical Subtotals
-- ============================================================================
-- ROLLUP(production_line, shift) is shorthand for:
--   GROUPING SETS ((production_line, shift), (production_line), ())
-- It produces a hierarchy: detail rows, then per-line subtotals, then a grand total.
--   Detail (line, shift): 3 x 3 = 9 rows
--   Subtotals (line):     3 rows
--   Grand total:          1 row
--   Total:                13 rows

ASSERT ROW_COUNT = 13
ASSERT VALUE total_units = 4240 WHERE production_line = 'Line-A' AND shift = 'Morning'
ASSERT VALUE total_units = 12120 WHERE production_line = 'Line-A' AND shift IS NULL
ASSERT VALUE total_units = 36360 WHERE production_line IS NULL AND shift IS NULL
SELECT production_line,
       shift,
       SUM(units_produced)   AS total_units,
       SUM(defect_count)     AS total_defects,
       ROUND(AVG(runtime_hours), 2) AS avg_runtime,
       ROUND(SUM(defect_count) * 100.0 / SUM(units_produced), 2) AS defect_rate_pct
FROM {{zone_name}}.delta_demos.production_runs
GROUP BY ROLLUP (production_line, shift)
ORDER BY production_line NULLS LAST, shift NULLS LAST;


-- ============================================================================
-- LEARN: CUBE — All Possible Grouping Combinations
-- ============================================================================
-- CUBE(production_line, shift) is shorthand for:
--   GROUPING SETS ((production_line, shift), (production_line), (shift), ())
-- It produces every possible combination of the grouped columns.
--   Detail (line, shift): 9 rows
--   By line only:         3 rows
--   By shift only:        3 rows
--   Grand total:          1 row
--   Total:                16 rows

ASSERT ROW_COUNT = 16
ASSERT VALUE total_defects = 282 WHERE production_line IS NULL AND shift IS NULL
ASSERT VALUE total_units = 11520 WHERE shift = 'Night' AND production_line IS NULL
SELECT production_line,
       shift,
       SUM(units_produced)   AS total_units,
       SUM(defect_count)     AS total_defects,
       ROUND(AVG(units_produced * 1.0), 2) AS avg_units,
       ROUND(SUM(defect_count) * 100.0 / SUM(units_produced), 2) AS defect_rate_pct
FROM {{zone_name}}.delta_demos.production_runs
GROUP BY CUBE (production_line, shift)
ORDER BY production_line NULLS LAST, shift NULLS LAST;


-- ============================================================================
-- LEARN: GROUPING() Function — Labeling Aggregation Levels
-- ============================================================================
-- GROUPING(column) returns 0 if the column is part of the current grouping key,
-- and 1 if it has been aggregated away (i.e., the NULL is a "super-aggregate" NULL).
-- This lets you distinguish real NULLs in data from grouping-introduced NULLs,
-- and build human-readable labels for each aggregation level.
--
-- Levels produced by CUBE(production_line, shift):
--   GROUPING(line)=0, GROUPING(shift)=0 => 'Line+Shift Detail'  (9 rows)
--   GROUPING(line)=0, GROUPING(shift)=1 => 'Line Subtotal'      (3 rows)
--   GROUPING(line)=1, GROUPING(shift)=0 => 'Shift Subtotal'     (3 rows)
--   GROUPING(line)=1, GROUPING(shift)=1 => 'Grand Total'        (1 row)

ASSERT ROW_COUNT = 16
ASSERT VALUE total_units = 12120 WHERE report_level = 'Line Subtotal' AND production_line = 'Line-A'
ASSERT VALUE defect_rate_pct = 0.92 WHERE report_level = 'Shift Subtotal' AND shift = 'Night'
ASSERT VALUE defect_rate_pct = 0.78 WHERE report_level = 'Grand Total'
SELECT CASE
         WHEN GROUPING(production_line) = 0 AND GROUPING(shift) = 0 THEN 'Line+Shift Detail'
         WHEN GROUPING(production_line) = 0 AND GROUPING(shift) = 1 THEN 'Line Subtotal'
         WHEN GROUPING(production_line) = 1 AND GROUPING(shift) = 0 THEN 'Shift Subtotal'
         ELSE 'Grand Total'
       END AS report_level,
       production_line,
       shift,
       SUM(units_produced)   AS total_units,
       SUM(defect_count)     AS total_defects,
       ROUND(SUM(defect_count) * 100.0 / SUM(units_produced), 2) AS defect_rate_pct
FROM {{zone_name}}.delta_demos.production_runs
GROUP BY CUBE (production_line, shift)
ORDER BY GROUPING(production_line), GROUPING(shift), production_line, shift;


-- ============================================================================
-- LEARN: ROLLUP + HAVING — Filtering to Subtotals Only
-- ============================================================================
-- HAVING with GROUPING() lets you keep only the aggregation rows you need.
-- Here we use ROLLUP(production_line, shift) but filter to only the subtotal
-- and grand total rows (where shift has been rolled up), dropping the detail rows.
-- GROUPING(shift) = 1 means shift is aggregated away: 3 line subtotals + 1 grand total.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_units = 12120 WHERE production_line = 'Line-A'
ASSERT VALUE defect_rate_pct = 0.99 WHERE production_line = 'Line-B'
ASSERT VALUE avg_units_per_run = 1010.0 WHERE production_line IS NULL
SELECT production_line,
       SUM(units_produced)   AS total_units,
       SUM(defect_count)     AS total_defects,
       ROUND(SUM(defect_count) * 100.0 / SUM(units_produced), 2) AS defect_rate_pct,
       ROUND(AVG(units_produced * 1.0), 2) AS avg_units_per_run
FROM {{zone_name}}.delta_demos.production_runs
GROUP BY ROLLUP (production_line, shift)
HAVING GROUPING(shift) = 1
ORDER BY production_line NULLS LAST;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Summary verification ensuring the dataset and aggregations produce expected results.

-- Verify total row count
ASSERT ROW_COUNT = 36
SELECT * FROM {{zone_name}}.delta_demos.production_runs;

-- Verify distinct production lines
ASSERT VALUE distinct_lines = 3
SELECT COUNT(DISTINCT production_line) AS distinct_lines
FROM {{zone_name}}.delta_demos.production_runs;

-- Verify distinct shifts
ASSERT VALUE distinct_shifts = 3
SELECT COUNT(DISTINCT shift) AS distinct_shifts
FROM {{zone_name}}.delta_demos.production_runs;

-- Verify total units across all runs
ASSERT VALUE total_units = 36360
SELECT SUM(units_produced) AS total_units
FROM {{zone_name}}.delta_demos.production_runs;

-- Verify total defects across all runs
ASSERT VALUE total_defects = 282
SELECT SUM(defect_count) AS total_defects
FROM {{zone_name}}.delta_demos.production_runs;

-- Verify overall defect rate
ASSERT VALUE overall_defect_rate = 0.78
SELECT ROUND(SUM(defect_count) * 100.0 / SUM(units_produced), 2) AS overall_defect_rate
FROM {{zone_name}}.delta_demos.production_runs;

-- Verify each line has exactly 12 runs (3 shifts x 4 dates)
ASSERT VALUE bad_line_count = 0
SELECT COUNT(*) AS bad_line_count FROM (
    SELECT production_line, COUNT(*) AS c
    FROM {{zone_name}}.delta_demos.production_runs
    GROUP BY production_line
) WHERE c != 12;

-- Verify Line-C has the highest output and lowest defect rate
ASSERT VALUE best_line = 'Line-C'
SELECT production_line AS best_line FROM (
    SELECT production_line,
           ROUND(SUM(defect_count) * 100.0 / SUM(units_produced), 2) AS rate
    FROM {{zone_name}}.delta_demos.production_runs
    GROUP BY production_line
    ORDER BY rate ASC
    LIMIT 1
);
