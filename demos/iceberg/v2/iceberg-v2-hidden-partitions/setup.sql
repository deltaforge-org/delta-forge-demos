-- ============================================================================
-- Iceberg V2 Hidden Partitions — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg v2 table
-- using hidden partitioning: months(pickup_date).
--
-- Hidden partitioning means the partition column does NOT appear as a
-- separate column in the data schema — Iceberg transparently applies
-- partition pruning when queries filter on pickup_date. Users never need
-- to know that the data is partitioned by month.
--
-- Dataset: 300 ride-share trip records across 6 months (Jan–Jun 2025),
-- 11 columns: trip_id, driver_id, rider_id, pickup_date, pickup_time,
-- dropoff_time, distance_miles, fare_amount, tip_amount, payment_type, city.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v2 table with hidden month partitioning
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- DeltaForge parses metadata.json to discover schema, partition spec, and data files.
-- The partition-spec uses months(pickup_date) — a hidden partition transform.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.trips
USING ICEBERG
LOCATION 'iceberg-v2-hidden-partitions/trips';

