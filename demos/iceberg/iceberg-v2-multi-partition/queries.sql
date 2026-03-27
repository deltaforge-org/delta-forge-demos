-- ============================================================================
-- Iceberg V2 Multi-Partition Weather Readings — Queries
-- ============================================================================
-- Demonstrates native Iceberg format-version 2 table reading with
-- multi-column partitioning (region + years(observation_date)). Tests
-- partition pruning, aggregations across partition boundaries, and
-- filtering on both identity and transform partition columns.
-- All queries are read-only.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Total Row Count
-- ============================================================================
-- Verifies that Delta Forge discovered all 15 partitioned Parquet data files
-- via the Iceberg v2 manifest chain (metadata.json -> manifest list ->
-- manifest -> files). 5 regions x 3 years x 30 readings = 450 rows.

ASSERT ROW_COUNT = 450
SELECT * FROM {{zone_name}}.iceberg.weather_readings;


-- ============================================================================
-- Query 2: Region Breakdown
-- ============================================================================
-- Five geographic regions with 90 readings each (3 years x 30 per year).
-- Exercises identity partition column aggregation.

ASSERT ROW_COUNT = 5
ASSERT VALUE reading_count = 90 WHERE region = 'Africa'
ASSERT VALUE reading_count = 90 WHERE region = 'Asia'
ASSERT VALUE reading_count = 90 WHERE region = 'Europe'
ASSERT VALUE reading_count = 90 WHERE region = 'North America'
ASSERT VALUE reading_count = 90 WHERE region = 'South America'
SELECT
    region,
    COUNT(*) AS reading_count
FROM {{zone_name}}.iceberg.weather_readings
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 3: Region + Year Partition Filter
-- ============================================================================
-- Filters to Europe, year 2024 only. With multi-column partitioning this
-- should prune to a single partition (region='Europe', year=2024), reading
-- only 1 of 15 data files. Exercises partition pruning on both dimensions.

ASSERT ROW_COUNT = 30
SELECT
    reading_id,
    station_id,
    region,
    observation_date,
    temperature_c,
    humidity_pct,
    wind_speed_kmh,
    precipitation_mm,
    condition
FROM {{zone_name}}.iceberg.weather_readings
WHERE region = 'Europe'
  AND observation_date >= '2024-01-01'
  AND observation_date < '2025-01-01'
ORDER BY observation_date;


-- ============================================================================
-- Query 4: Per-Station Aggregation
-- ============================================================================
-- 15 stations (3 per region) with average temperature and humidity.
-- Exercises floating-point aggregation across all partitions.

ASSERT ROW_COUNT = 15
ASSERT VALUE avg_temp = 23.84 WHERE station_id = 'WX-AF001'
ASSERT VALUE avg_temp = 12.98 WHERE station_id = 'WX-AS001'
ASSERT VALUE avg_temp = 12.99 WHERE station_id = 'WX-EU001'
ASSERT VALUE avg_temp = 12.46 WHERE station_id = 'WX-NA001'
ASSERT VALUE avg_temp = 24.61 WHERE station_id = 'WX-SA001'
SELECT
    station_id,
    COUNT(*) AS reading_count,
    ROUND(AVG(temperature_c), 2) AS avg_temp,
    ROUND(AVG(humidity_pct), 2) AS avg_humidity
FROM {{zone_name}}.iceberg.weather_readings
GROUP BY station_id
ORDER BY station_id;


-- ============================================================================
-- Query 5: Weather Condition Distribution
-- ============================================================================
-- Five weather conditions across all readings. Exercises string column
-- aggregation and GROUP BY on a non-partition column.

ASSERT ROW_COUNT = 5
ASSERT VALUE cnt = 83 WHERE condition = 'Clear'
ASSERT VALUE cnt = 102 WHERE condition = 'Cloudy'
ASSERT VALUE cnt = 88 WHERE condition = 'Rain'
ASSERT VALUE cnt = 94 WHERE condition = 'Snow'
ASSERT VALUE cnt = 83 WHERE condition = 'Storm'
SELECT
    condition,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg.weather_readings
GROUP BY condition
ORDER BY condition;


-- ============================================================================
-- Query 6: Year-over-Year Comparison
-- ============================================================================
-- Average temperature by year across all regions. Exercises the
-- years(observation_date) transform partition for grouping.

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_temp = 16.65 WHERE obs_year = 2023
ASSERT VALUE avg_temp = 17.73 WHERE obs_year = 2024
ASSERT VALUE avg_temp = 17.18 WHERE obs_year = 2025
SELECT
    YEAR(observation_date) AS obs_year,
    ROUND(AVG(temperature_c), 2) AS avg_temp,
    COUNT(*) AS reading_count
FROM {{zone_name}}.iceberg.weather_readings
GROUP BY YEAR(observation_date)
ORDER BY obs_year;


-- ============================================================================
-- Query 7: Extreme Readings
-- ============================================================================
-- Identifies weather readings with extreme temperatures: above 35C (hot)
-- or below -5C (cold). Exercises predicate pushdown on double column
-- across all partitions.

ASSERT ROW_COUNT = 41
SELECT
    reading_id,
    station_id,
    region,
    observation_date,
    temperature_c,
    condition
FROM {{zone_name}}.iceberg.weather_readings
WHERE temperature_c > 35 OR temperature_c < -5
ORDER BY temperature_c DESC;


-- ============================================================================
-- VERIFY: Grand Totals & Data Integrity
-- ============================================================================
-- Cross-cutting sanity check: total rows, distinct counts, value ranges,
-- and computed aggregates. Verifies the Iceberg v2 multi-partition reader
-- produces correct data values — not just correct row counts.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 450
ASSERT VALUE region_count = 5
ASSERT VALUE station_count = 15
ASSERT VALUE condition_count = 5
ASSERT VALUE extreme_count = 41
ASSERT VALUE null_precip_count = 58
ASSERT VALUE min_temp = -9.9
ASSERT VALUE max_temp = 39.5
ASSERT VALUE min_humidity = 20.9
ASSERT VALUE max_humidity = 99.9
ASSERT VALUE total_wind = 25320.8
ASSERT VALUE avg_precip = 25.01
ASSERT VALUE earliest_date = '2023-01-08'
ASSERT VALUE latest_date = '2025-12-31'
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT station_id) AS station_count,
    COUNT(DISTINCT condition) AS condition_count,
    SUM(CASE WHEN temperature_c > 35 OR temperature_c < -5 THEN 1 ELSE 0 END) AS extreme_count,
    SUM(CASE WHEN precipitation_mm IS NULL THEN 1 ELSE 0 END) AS null_precip_count,
    ROUND(MIN(temperature_c), 2) AS min_temp,
    ROUND(MAX(temperature_c), 2) AS max_temp,
    ROUND(MIN(humidity_pct), 2) AS min_humidity,
    ROUND(MAX(humidity_pct), 2) AS max_humidity,
    ROUND(SUM(wind_speed_kmh), 2) AS total_wind,
    ROUND(AVG(precipitation_mm), 2) AS avg_precip,
    CAST(MIN(observation_date) AS VARCHAR) AS earliest_date,
    CAST(MAX(observation_date) AS VARCHAR) AS latest_date
FROM {{zone_name}}.iceberg.weather_readings;


-- ============================================================================
-- VERIFY: Hottest & Coldest Readings
-- ============================================================================
-- Validates that specific extreme-value rows are read correctly across
-- partition boundaries, confirming data fidelity at the individual row level.

ASSERT ROW_COUNT = 2
ASSERT VALUE temperature_c = 39.5 WHERE reading_id = 439
ASSERT VALUE station_id = 'WX-AF003' WHERE reading_id = 439
ASSERT VALUE region = 'Africa' WHERE reading_id = 439
ASSERT VALUE condition = 'Cloudy' WHERE reading_id = 439
ASSERT VALUE temperature_c = -9.9 WHERE reading_id = 53
ASSERT VALUE station_id = 'WX-NA003' WHERE reading_id = 53
ASSERT VALUE region = 'North America' WHERE reading_id = 53
ASSERT VALUE condition = 'Cloudy' WHERE reading_id = 53
SELECT
    reading_id,
    station_id,
    region,
    observation_date,
    temperature_c,
    condition
FROM {{zone_name}}.iceberg.weather_readings
WHERE reading_id IN (439, 53)
ORDER BY temperature_c DESC;
