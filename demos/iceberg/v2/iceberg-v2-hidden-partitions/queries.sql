-- ============================================================================
-- Iceberg V2 Hidden Partitions — Queries
-- ============================================================================
-- Demonstrates native Iceberg format-version 2 table reading with hidden
-- partition transform: months(pickup_date). The partition column does not
-- appear in the data schema — queries filter on pickup_date and Iceberg
-- transparently prunes partitions. All queries are read-only.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Total Row Count & Schema Spot Checks
-- ============================================================================
-- Verifies that DeltaForge discovered all 6 monthly partitioned Parquet
-- data files via the Iceberg v2 manifest chain. Checks total rows, distinct
-- trip IDs, and fare/distance extremes.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 300
ASSERT VALUE distinct_trips = 300
ASSERT VALUE min_fare = 3.7
ASSERT VALUE max_fare = 85.14
ASSERT VALUE min_distance = 0.5
ASSERT VALUE max_distance = 25.0
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT trip_id) AS distinct_trips,
    ROUND(MIN(fare_amount), 2) AS min_fare,
    ROUND(MAX(fare_amount), 2) AS max_fare,
    ROUND(MIN(distance_miles), 2) AS min_distance,
    ROUND(MAX(distance_miles), 2) AS max_distance
FROM {{zone_name}}.iceberg_demos.trips;


-- ============================================================================
-- Query 2: Monthly Breakdown
-- ============================================================================
-- Six months with exactly 50 trips each (deterministic round-robin).
-- Exercises month extraction and GROUP BY across hidden partitions.

ASSERT ROW_COUNT = 6
ASSERT VALUE trip_count = 50 WHERE pickup_month = 1
ASSERT VALUE trip_count = 50 WHERE pickup_month = 2
ASSERT VALUE trip_count = 50 WHERE pickup_month = 3
ASSERT VALUE trip_count = 50 WHERE pickup_month = 4
ASSERT VALUE trip_count = 50 WHERE pickup_month = 5
ASSERT VALUE trip_count = 50 WHERE pickup_month = 6
SELECT
    MONTH(pickup_date) AS pickup_month,
    COUNT(*) AS trip_count
FROM {{zone_name}}.iceberg_demos.trips
GROUP BY MONTH(pickup_date)
ORDER BY pickup_month;


-- ============================================================================
-- Query 3: Single Month Filter — March 2025 (Partition Pruning)
-- ============================================================================
-- With months(pickup_date) partitioning, Iceberg can prune all non-March
-- partitions. Only 1 of 6 data files needs to be read.

ASSERT ROW_COUNT = 50
ASSERT VALUE trip_id = 57 WHERE pickup_time = '08:49'
ASSERT VALUE city = 'Chicago' WHERE trip_id = 57
ASSERT VALUE driver_id = 'DRV-1042' WHERE trip_id = 57
ASSERT VALUE trip_id = 33 WHERE pickup_time = '23:15'
ASSERT VALUE city = 'San Francisco' WHERE trip_id = 33
ASSERT VALUE fare_amount = 78.36 WHERE trip_id = 225
ASSERT VALUE city = 'Seattle' WHERE trip_id = 225
SELECT
    trip_id,
    driver_id,
    rider_id,
    pickup_date,
    pickup_time,
    dropoff_time,
    distance_miles,
    fare_amount,
    tip_amount,
    payment_type,
    city
FROM {{zone_name}}.iceberg_demos.trips
WHERE pickup_date BETWEEN DATE '2025-03-01' AND DATE '2025-03-31'
ORDER BY pickup_date, pickup_time;


-- ============================================================================
-- Query 4: City Breakdown
-- ============================================================================
-- Five cities with exactly 60 trips each (deterministic round-robin).

ASSERT ROW_COUNT = 5
ASSERT VALUE trip_count = 60 WHERE city = 'Austin'
ASSERT VALUE trip_count = 60 WHERE city = 'Chicago'
ASSERT VALUE trip_count = 60 WHERE city = 'New York'
ASSERT VALUE trip_count = 60 WHERE city = 'San Francisco'
ASSERT VALUE trip_count = 60 WHERE city = 'Seattle'
SELECT
    city,
    COUNT(*) AS trip_count
FROM {{zone_name}}.iceberg_demos.trips
GROUP BY city
ORDER BY city;


-- ============================================================================
-- Query 5: Payment Analysis — Avg Fare & Null Tips
-- ============================================================================
-- Three payment types. Cash rides have ~20% NULL tip_amount.

ASSERT ROW_COUNT = 3
ASSERT VALUE trip_count = 104 WHERE payment_type = 'Cash'
ASSERT VALUE avg_fare = 35.61 WHERE payment_type = 'Cash'
ASSERT VALUE null_tip_count = 14 WHERE payment_type = 'Cash'
ASSERT VALUE trip_count = 108 WHERE payment_type = 'Credit Card'
ASSERT VALUE avg_fare = 36.73 WHERE payment_type = 'Credit Card'
ASSERT VALUE null_tip_count = 0 WHERE payment_type = 'Credit Card'
ASSERT VALUE trip_count = 88 WHERE payment_type = 'Digital Wallet'
ASSERT VALUE avg_fare = 36.6 WHERE payment_type = 'Digital Wallet'
ASSERT VALUE null_tip_count = 0 WHERE payment_type = 'Digital Wallet'
SELECT
    payment_type,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    SUM(CASE WHEN tip_amount IS NULL THEN 1 ELSE 0 END) AS null_tip_count
FROM {{zone_name}}.iceberg_demos.trips
GROUP BY payment_type
ORDER BY payment_type;


-- ============================================================================
-- Query 6: Distance/Fare Correlation — Top Routes by Avg Distance
-- ============================================================================
-- Cities ranked by average trip distance.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_distance = 13.99 WHERE city = 'San Francisco'
ASSERT VALUE avg_distance = 13.26 WHERE city = 'New York'
ASSERT VALUE avg_distance = 12.22 WHERE city = 'Seattle'
ASSERT VALUE avg_distance = 12.11 WHERE city = 'Austin'
ASSERT VALUE avg_distance = 10.87 WHERE city = 'Chicago'
SELECT
    city,
    ROUND(AVG(distance_miles), 2) AS avg_distance,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    COUNT(*) AS trip_count
FROM {{zone_name}}.iceberg_demos.trips
GROUP BY city
ORDER BY avg_distance DESC;


-- ============================================================================
-- Query 7: Driver Performance — Trip Counts
-- ============================================================================
-- 50 distinct drivers. Top driver (DRV-1042) has 12 trips.

ASSERT ROW_COUNT = 50
ASSERT VALUE trip_count = 12 WHERE driver_id = 'DRV-1042'
ASSERT VALUE trip_count = 11 WHERE driver_id = 'DRV-1019'
ASSERT VALUE trip_count = 10 WHERE driver_id = 'DRV-1006'
ASSERT VALUE trip_count = 10 WHERE driver_id = 'DRV-1030'
SELECT
    driver_id,
    COUNT(*) AS trip_count
FROM {{zone_name}}.iceberg_demos.trips
GROUP BY driver_id
ORDER BY trip_count DESC, driver_id;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check: total rows, distinct counts, fare/distance
-- totals, null tips, and March filter count. Validates the full Iceberg
-- hidden-partition reader pipeline.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 300
ASSERT VALUE city_count = 5
ASSERT VALUE payment_type_count = 3
ASSERT VALUE driver_count = 50
ASSERT VALUE total_fare = 10890.01
ASSERT VALUE avg_fare = 36.3
ASSERT VALUE total_distance = 3747.1
ASSERT VALUE null_tip_count = 14
ASSERT VALUE march_rows = 50
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT city) AS city_count,
    COUNT(DISTINCT payment_type) AS payment_type_count,
    COUNT(DISTINCT driver_id) AS driver_count,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(SUM(distance_miles), 1) AS total_distance,
    SUM(CASE WHEN tip_amount IS NULL THEN 1 ELSE 0 END) AS null_tip_count,
    SUM(CASE WHEN pickup_date >= DATE '2025-03-01'
              AND pickup_date <= DATE '2025-03-31' THEN 1 ELSE 0 END) AS march_rows
FROM {{zone_name}}.iceberg_demos.trips;
