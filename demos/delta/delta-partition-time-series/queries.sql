-- ============================================================================
-- Delta Time-Based Partitioning — Educational Queries
-- ============================================================================
-- WHAT: Explores time-based partitioning on a factory sensor dataset where
--       temperature readings are organized into monthly partition directories
--       (year_month = '2024-01' through '2024-06').
-- WHY:  Time-based partitioning is the most common strategy in production data
--       lakes. Queries almost always filter by time window, so the engine can
--       skip entire partition directories that fall outside the range. This
--       dramatically reduces I/O for time-range scans, updates, and deletes.
-- HOW:  We start with a monthly overview, demonstrate partition pruning at
--       different granularities, perform a partition-scoped UPDATE (sensor
--       recalibration), backfill a new month via INSERT INTO...SELECT, remove
--       anomalous readings with a scoped DELETE, and finish with a trend
--       analysis across all partitions.
-- ============================================================================


-- ============================================================================
-- Q1  EXPLORE: Baseline — Monthly Partition Overview
-- ============================================================================
-- Each year_month value maps to a physical partition directory on disk.
-- Grouping by the partition column is highly efficient because the engine
-- can read partition metadata without scanning every row.
-- ============================================================================
ASSERT ROW_COUNT = 6
ASSERT VALUE row_count = 15 WHERE year_month = '2024-01'
ASSERT VALUE row_count = 15 WHERE year_month = '2024-06'
SELECT
    year_month,
    COUNT(*)        AS row_count,
    ROUND(AVG(reading), 1) AS avg_reading,
    MIN(reading)    AS min_reading,
    MAX(reading)    AS max_reading
FROM {{zone_name}}.delta_demos.line_metrics
GROUP BY year_month
ORDER BY year_month;


-- ============================================================================
-- Q2  LEARN: Time-Range Query — Q1 Data Only (Partition Pruning)
-- ============================================================================
-- Filtering with WHERE year_month IN (...) lets the engine skip partition
-- directories entirely. Here we request Q1 (Jan-Mar), so the April, May,
-- and June directories are never opened — a 50% I/O reduction.
-- ============================================================================
ASSERT ROW_COUNT = 2
ASSERT VALUE avg_reading = 22.6 WHERE line_name = 'line-a'
ASSERT VALUE reading_count = 18 WHERE line_name = 'line-b'
SELECT
    line_name,
    COUNT(*)        AS reading_count,
    ROUND(AVG(reading), 1) AS avg_reading,
    MIN(reading)    AS min_reading,
    MAX(reading)    AS max_reading
FROM {{zone_name}}.delta_demos.line_metrics
WHERE year_month IN ('2024-01', '2024-02', '2024-03')
GROUP BY line_name
ORDER BY line_name;


-- ============================================================================
-- Q3  LEARN: Single-Month Precision — Maximum Pruning
-- ============================================================================
-- Narrowing to a single partition value (year_month = '2024-03') means the
-- engine reads only one directory — an 83% reduction compared to a full scan.
-- This is the ideal access pattern for dashboards and reports scoped to a
-- specific period.
-- ============================================================================
ASSERT ROW_COUNT = 5
ASSERT VALUE avg_reading = 22.9 WHERE sensor_id = 'S01'
ASSERT VALUE avg_reading = 29.9 WHERE sensor_id = 'S04'
SELECT
    sensor_id,
    ROUND(AVG(reading), 1) AS avg_reading,
    MIN(reading)    AS min_reading,
    MAX(reading)    AS max_reading,
    COUNT(*)        AS reading_count
FROM {{zone_name}}.delta_demos.line_metrics
WHERE year_month = '2024-03'
GROUP BY sensor_id
ORDER BY sensor_id;


-- ============================================================================
-- Q4  LEARN: Partition-Scoped UPDATE — April Recalibration (+1.5 C)
-- ============================================================================
-- A sensor recalibration discovered that all April readings were 1.5 C too
-- low. Because the WHERE clause targets a single partition, only the
-- year_month='2024-04' directory is rewritten. The other five partitions
-- remain untouched — a major efficiency win for large tables.
-- ============================================================================
ASSERT ROW_COUNT = 15
UPDATE {{zone_name}}.delta_demos.line_metrics
SET reading = ROUND(reading + 1.5, 1)
WHERE year_month = '2024-04';


-- ============================================================================
-- Q5  EXPLORE: Verify April Recalibration
-- ============================================================================
-- Confirm the +1.5 C adjustment was applied. We compute the original value
-- by subtracting 1.5 so you can see both side by side.
-- ============================================================================
ASSERT ROW_COUNT = 15
ASSERT VALUE calibrated_reading = 24.3 WHERE id = 46
SELECT
    id,
    sensor_id,
    line_name,
    reading         AS calibrated_reading,
    ROUND(reading - 1.5, 1) AS original_reading,
    recorded_at
FROM {{zone_name}}.delta_demos.line_metrics
WHERE year_month = '2024-04'
ORDER BY id;


-- ============================================================================
-- Q6  LEARN: Historical Backfill — INSERT INTO...SELECT
-- ============================================================================
-- A common pattern: generate projected data for the next month by copying
-- the latest partition with an adjustment. Here we create July from June
-- with a -0.5 C seasonal correction. The engine writes a brand-new
-- year_month='2024-07' partition directory without touching existing data.
-- ============================================================================
ASSERT ROW_COUNT = 15
INSERT INTO {{zone_name}}.delta_demos.line_metrics
SELECT
    id + 90                                         AS id,
    sensor_id,
    ROUND(reading - 0.5, 1)                         AS reading,
    unit,
    line_name,
    REPLACE(recorded_at, '2024-06', '2024-07')      AS recorded_at,
    '2024-07'                                        AS year_month
FROM {{zone_name}}.delta_demos.line_metrics
WHERE year_month = '2024-06';


-- ============================================================================
-- Q7  EXPLORE: Verify Backfill — July Partition Created
-- ============================================================================
-- The table now has seven partitions. The new July partition was created
-- atomically — readers never saw a partial state thanks to Delta's
-- transactional log.
-- ============================================================================
ASSERT ROW_COUNT = 7
ASSERT VALUE row_count = 15 WHERE year_month = '2024-07'
SELECT
    year_month,
    COUNT(*)        AS row_count,
    ROUND(AVG(reading), 1) AS avg_reading
FROM {{zone_name}}.delta_demos.line_metrics
GROUP BY year_month
ORDER BY year_month;


-- ============================================================================
-- Q8  LEARN: Partition-Scoped DELETE — Remove Anomalous Readings
-- ============================================================================
-- Three February readings from the furnace-proximity sensors (S04, S05)
-- spiked above 35 C — anomalous for that month. The DELETE targets only
-- the year_month='2024-02' partition, rewriting just that directory while
-- leaving all other months intact.
-- ============================================================================
ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.delta_demos.line_metrics
WHERE year_month = '2024-02'
  AND reading > 35;


-- ============================================================================
-- Q9  EXPLORE: Monthly Trend Analysis
-- ============================================================================
-- With anomalies removed and recalibration applied, we can see the seasonal
-- temperature progression. Grouping by the partition column (year_month) is
-- the most efficient aggregation pattern — the engine reads partition-level
-- metadata first and scans only the necessary files.
-- ============================================================================
ASSERT ROW_COUNT = 7
ASSERT VALUE avg_reading = 24.5 WHERE year_month = '2024-01'
ASSERT VALUE avg_reading = 26.1 WHERE year_month = '2024-07'
SELECT
    year_month,
    COUNT(*)        AS row_count,
    ROUND(AVG(reading), 1) AS avg_reading,
    MIN(reading)    AS min_reading,
    MAX(reading)    AS max_reading
FROM {{zone_name}}.delta_demos.line_metrics
GROUP BY year_month
ORDER BY year_month;


-- ============================================================================
-- Q10 VERIFY: All Checks
-- ============================================================================
-- Final integrity sweep:
--   1. Total rows = 102 (90 original + 15 July backfill - 3 Feb anomalies)
--   2. January partition untouched at 15 rows
--   3. April recalibration applied (id=46 reading = 24.3)
--   4. July backfill exists with 15 rows
--   5. No anomalous February readings remain (reading > 35)
-- ============================================================================
ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 102
ASSERT VALUE jan_rows = 15
ASSERT VALUE apr_check_reading = 24.3
ASSERT VALUE jul_rows = 15
ASSERT VALUE feb_anomalous_count = 0
SELECT
    COUNT(*)                                                                AS total_rows,
    SUM(CASE WHEN year_month = '2024-01' THEN 1 ELSE 0 END)               AS jan_rows,
    SUM(CASE WHEN year_month = '2024-07' THEN 1 ELSE 0 END)               AS jul_rows,
    ROUND(MAX(CASE WHEN id = 46 THEN reading END), 1)                      AS apr_check_reading,
    SUM(CASE WHEN year_month = '2024-02' AND reading > 35 THEN 1 ELSE 0 END) AS feb_anomalous_count
FROM {{zone_name}}.delta_demos.line_metrics;
