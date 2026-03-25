-- ============================================================================
-- Delta Z-ORDER — Multi-Column Data Layout Optimization — Educational Queries
-- ============================================================================
-- WHAT: Z-ORDER reorganizes data using a space-filling curve so rows with
--       similar values across multiple columns are stored in the same files.
-- WHY:  Without Z-ORDER, data is written in insertion order. Queries that filter
--       on region, sensor_type, or date must scan ALL files. Z-ORDER enables
--       data skipping: Parquet file-level min/max stats let the engine skip
--       entire files that cannot contain matching rows.
-- HOW:  OPTIMIZE ... ZORDER BY reads all data, interleaves column values using
--       a Z-curve (Morton code), and rewrites into optimally-ordered files.
--       The Delta transaction log records this as a single atomic operation.
-- ============================================================================


-- ============================================================================
-- Query 1: PRE-ZORDER — Data distribution across regions and sensor types
-- ============================================================================
-- Right now, data is spread across 3 batch insert files plus update files.
-- Rows are in insertion order, NOT co-located by region or sensor_type.
-- A query filtering on region='us-east' must scan ALL files because us-east
-- data was inserted in batch 1 and batch 3.
-- 4 regions x 4 sensor types = 16 combinations

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 16
ASSERT RESULT SET INCLUDES ('eu-west', 'humidity', 10, 66.64)
ASSERT RESULT SET INCLUDES ('ap-south', 'pressure', 7, 1012.97)
SELECT region, sensor_type, COUNT(*) AS reading_count,
       ROUND(AVG(reading), 2) AS avg_reading
FROM {{zone_name}}.delta_demos.sensor_telemetry
GROUP BY region, sensor_type
ORDER BY region, sensor_type;


-- ============================================================================
-- Query 2: PRE-ZORDER — Multi-column filter on scattered data
-- ============================================================================
-- This query filters on region AND sensor_type AND date range. Without
-- Z-ORDER, these rows are scattered across multiple data files from different
-- batch inserts. The engine must scan all files — no data skipping is possible
-- because file-level min/max stats span all regions and types.

ASSERT ROW_COUNT = 4
SELECT id, device_id, reading, unit, quality_score, recorded_date
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE region = 'eu-west'
  AND sensor_type = 'temperature'
  AND recorded_date BETWEEN '2025-03-01' AND '2025-03-05'
ORDER BY recorded_date;


-- ============================================================================
-- Query 3: PRE-ZORDER — DESCRIBE DETAIL shows file count before optimization
-- ============================================================================
-- DESCRIBE DETAIL reveals the current physical layout. After 3 batch inserts
-- plus an UPDATE (copy-on-write), the table has multiple data files. All
-- queries must scan all of them because data is in insertion order.

ASSERT NO_FAIL IN result
DESCRIBE DETAIL {{zone_name}}.delta_demos.sensor_telemetry;


-- ============================================================================
-- Query 4: OPTIMIZE ZORDER BY (region, sensor_type, recorded_date)
-- ============================================================================
-- This is the key command. It reads ALL data files, interleaves column values
-- using a Z-curve (Morton code), and rewrites the data into new files where
-- rows with similar (region, sensor_type, recorded_date) values are co-located.
--
-- Z-ORDER vs simple sorting:
--   - Sorting by region alone would help region queries but leave sensor_type
--     and date scattered within each region.
--   - Z-ORDER uses a space-filling curve that preserves locality in ALL
--     dimensions simultaneously. The tradeoff: no single dimension is
--     perfectly sorted, but ALL dimensions have reasonable locality.
--
-- After this command, Parquet file-level min/max statistics become much
-- tighter, enabling the engine to skip entire files for filtered queries.

OPTIMIZE {{zone_name}}.delta_demos.sensor_telemetry
ZORDER BY (region, sensor_type, recorded_date);


-- ============================================================================
-- Query 5: POST-ZORDER — DESCRIBE HISTORY shows the OPTIMIZE operation
-- ============================================================================
-- The transaction log records OPTIMIZE ZORDER as a single atomic operation.
-- DESCRIBE HISTORY shows how many files were compacted and the new version.

ASSERT NO_FAIL IN result
DESCRIBE HISTORY {{zone_name}}.delta_demos.sensor_telemetry;


-- ============================================================================
-- Query 6: POST-ZORDER — Same multi-column filter, now with data skipping
-- ============================================================================
-- Re-run the exact same query from Query 2. The result set is identical —
-- Z-ORDER does not change the data, only the physical file layout. After
-- Z-ORDER, rows matching region='eu-west' AND sensor_type='temperature' are
-- co-located in the same file(s). The engine can skip files whose min/max
-- stats exclude these values.

ASSERT ROW_COUNT = 4
SELECT id, device_id, reading, unit, quality_score, recorded_date
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE region = 'eu-west'
  AND sensor_type = 'temperature'
  AND recorded_date BETWEEN '2025-03-01' AND '2025-03-05'
ORDER BY recorded_date;


-- ============================================================================
-- Query 7: POST-ZORDER — Single-column filter still benefits
-- ============================================================================
-- Even though Z-ORDER optimizes across 3 columns, a filter on just ONE column
-- still benefits. A query for region='us-east' can skip files whose min/max
-- stats show no 'us-east' values. Without Z-ORDER, us-east data would be
-- scattered across all batch files.

ASSERT ROW_COUNT = 23
SELECT id, device_id, sensor_type, reading, unit,
       quality_score, recorded_date
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE region = 'us-east'
ORDER BY sensor_type, recorded_date;


-- ============================================================================
-- Query 8: POST-ZORDER — Multi-dimensional grouping shows co-location
-- ============================================================================
-- This aggregation reveals how readings are distributed across the three
-- Z-ORDER dimensions. After optimization, rows sharing the same
-- (region, sensor_type, recorded_date) combination sit in the same files,
-- making GROUP BY queries on these columns much faster.

ASSERT ROW_COUNT = 63
ASSERT RESULT SET INCLUDES ('us-east', 'wind', '2025-03-08', 1, 19.5, 19.5)
SELECT region, sensor_type, recorded_date,
       COUNT(*) AS readings,
       ROUND(MIN(reading), 2) AS min_reading,
       ROUND(MAX(reading), 2) AS max_reading
FROM {{zone_name}}.delta_demos.sensor_telemetry
GROUP BY region, sensor_type, recorded_date
ORDER BY region, sensor_type, recorded_date;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 80
ASSERT ROW_COUNT = 80
SELECT * FROM {{zone_name}}.delta_demos.sensor_telemetry;

-- Verify 4 distinct regions
ASSERT VALUE region_count = 4
SELECT COUNT(DISTINCT region) AS region_count FROM {{zone_name}}.delta_demos.sensor_telemetry;

-- Verify 4 distinct sensor types
ASSERT VALUE sensor_type_count = 4
SELECT COUNT(DISTINCT sensor_type) AS sensor_type_count FROM {{zone_name}}.delta_demos.sensor_telemetry;

-- Verify us-east has 23 readings
ASSERT VALUE us_east_count = 23
SELECT COUNT(*) AS us_east_count FROM {{zone_name}}.delta_demos.sensor_telemetry WHERE region = 'us-east';

-- Verify temperature sensor type has 22 readings
ASSERT VALUE temperature_count = 22
SELECT COUNT(*) AS temperature_count FROM {{zone_name}}.delta_demos.sensor_telemetry WHERE sensor_type = 'temperature';

-- Verify 8 low-quality readings flagged (quality_score = 0)
ASSERT VALUE low_quality_count = 8
SELECT COUNT(*) AS low_quality_count FROM {{zone_name}}.delta_demos.sensor_telemetry WHERE quality_score = 0;

-- Verify average temperature reading is 21.8
ASSERT VALUE avg_temp_reading = 21.8
SELECT ROUND(AVG(reading), 1) AS avg_temp_reading FROM {{zone_name}}.delta_demos.sensor_telemetry WHERE sensor_type = 'temperature';

-- Verify 24 readings in date range 2025-03-01 to 2025-03-03
ASSERT VALUE date_range_count = 24
SELECT COUNT(*) AS date_range_count FROM {{zone_name}}.delta_demos.sensor_telemetry WHERE recorded_date >= '2025-03-01' AND recorded_date <= '2025-03-03';
