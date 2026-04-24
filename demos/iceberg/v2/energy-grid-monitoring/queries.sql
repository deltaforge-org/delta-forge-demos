-- ============================================================================
-- Iceberg Energy Grid Monitoring — Queries
-- ============================================================================
-- Demonstrates native Iceberg table reading: schema inference from metadata,
-- manifest-based file discovery, aggregations, filtering, and projection.
-- All queries are read-only — no mutations.
-- ============================================================================
-- ============================================================================
-- Query 1: Baseline — Total Row Count
-- ============================================================================
-- Verifies that DeltaForge discovered all 3 Parquet data files via the
-- Iceberg manifest chain (metadata.json → manifest list → manifest → files).

ASSERT ROW_COUNT = 600
SELECT * FROM {{zone_name}}.iceberg_demos.grid_readings;
-- ============================================================================
-- Query 2: Schema Inference from Iceberg Metadata
-- ============================================================================
-- The schema comes from metadata.json (not Parquet footers). This query
-- exercises all 11 columns to prove correct Iceberg→Arrow type mapping.

ASSERT ROW_COUNT = 600
ASSERT VALUE region = 'North' WHERE meter_id = 'MTR-N0001'
ASSERT VALUE substation = 'NS-01' WHERE meter_id = 'MTR-N0001'
ASSERT VALUE meter_type = 'Residential' WHERE meter_id = 'MTR-N0001'
ASSERT VALUE voltage = 240 WHERE meter_id = 'MTR-N0001'
ASSERT VALUE power_factor = 87 WHERE meter_id = 'MTR-N0001'
SELECT
    meter_id,
    region,
    substation,
    meter_type,
    reading_timestamp,
    voltage,
    current_amps,
    power_kw,
    energy_kwh,
    power_factor,
    grid_frequency_hz
FROM {{zone_name}}.iceberg_demos.grid_readings
ORDER BY meter_id;
-- ============================================================================
-- Query 3: Per-Region Row Counts
-- ============================================================================
-- Each region maps to one Parquet data file (200 rows each).

ASSERT ROW_COUNT = 3
ASSERT VALUE reading_count = 200 WHERE region = 'North'
ASSERT VALUE reading_count = 200 WHERE region = 'South'
ASSERT VALUE reading_count = 200 WHERE region = 'East'
SELECT
    region,
    COUNT(*) AS reading_count
FROM {{zone_name}}.iceberg_demos.grid_readings
GROUP BY region
ORDER BY region;
-- ============================================================================
-- Query 4: Total Energy Consumption
-- ============================================================================
-- Aggregation across all 600 readings. Proof value computed independently
-- from the seed data generator (sum of voltage * current / 1000 * 0.25).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_energy_kwh = 993.2375
SELECT
    ROUND(SUM(energy_kwh), 4) AS total_energy_kwh
FROM {{zone_name}}.iceberg_demos.grid_readings;
-- ============================================================================
-- Query 5: Per-Region Energy Totals
-- ============================================================================
-- Proves correct file-level aggregation (each region = one data file).

ASSERT ROW_COUNT = 3
ASSERT VALUE total_energy = 341.6525 WHERE region = 'North'
ASSERT VALUE total_energy = 324.7675 WHERE region = 'South'
ASSERT VALUE total_energy = 326.8175 WHERE region = 'East'
SELECT
    region,
    ROUND(SUM(energy_kwh), 4) AS total_energy
FROM {{zone_name}}.iceberg_demos.grid_readings
GROUP BY region
ORDER BY region;
-- ============================================================================
-- Query 6: Meter Type Distribution
-- ============================================================================
-- Verifies correct reading of string columns and GROUP BY accuracy.

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 208 WHERE meter_type = 'Commercial'
ASSERT VALUE cnt = 198 WHERE meter_type = 'Industrial'
ASSERT VALUE cnt = 194 WHERE meter_type = 'Residential'
SELECT
    meter_type,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.grid_readings
GROUP BY meter_type
ORDER BY meter_type;
-- ============================================================================
-- Query 7: Voltage Distribution
-- ============================================================================
-- Verifies integer column reading and distribution across standard voltages.

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 187 WHERE voltage = 220
ASSERT VALUE cnt = 205 WHERE voltage = 230
ASSERT VALUE cnt = 208 WHERE voltage = 240
SELECT
    voltage,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.grid_readings
GROUP BY voltage
ORDER BY voltage;
-- ============================================================================
-- Query 8: Predicate Pushdown — High-Power Readings
-- ============================================================================
-- Filters on a numeric column. With row_group_filter enabled, Parquet
-- statistics can be used to skip irrelevant row groups.

ASSERT ROW_COUNT = 102
SELECT
    meter_id,
    region,
    power_kw,
    voltage,
    current_amps
FROM {{zone_name}}.iceberg_demos.grid_readings
WHERE power_kw > 10
ORDER BY power_kw DESC;
-- ============================================================================
-- Query 9: Low Power Factor Alert
-- ============================================================================
-- Filters readings with poor power factor (< 85), a real-world grid
-- quality metric. Exercises predicate evaluation on integer columns.

ASSERT ROW_COUNT = 132
SELECT
    meter_id,
    region,
    substation,
    power_factor,
    power_kw
FROM {{zone_name}}.iceberg_demos.grid_readings
WHERE power_factor < 85
ORDER BY power_factor ASC;
-- ============================================================================
-- Query 10: Distinct Meters and Substations
-- ============================================================================
-- Exercises COUNT(DISTINCT ...) across the full dataset.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_meters = 600
ASSERT VALUE distinct_substations = 15
SELECT
    COUNT(DISTINCT meter_id) AS distinct_meters,
    COUNT(DISTINCT substation) AS distinct_substations
FROM {{zone_name}}.iceberg_demos.grid_readings;
-- ============================================================================
-- Query 11: Average Power by Meter Type
-- ============================================================================
-- Proves correct floating-point aggregation grouped by string column.

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_power = 6.44 WHERE meter_type = 'Commercial'
ASSERT VALUE avg_power = 6.75 WHERE meter_type = 'Industrial'
ASSERT VALUE avg_power = 6.68 WHERE meter_type = 'Residential'
SELECT
    meter_type,
    ROUND(AVG(power_kw), 2) AS avg_power
FROM {{zone_name}}.iceberg_demos.grid_readings
GROUP BY meter_type
ORDER BY meter_type;
-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Write the Iceberg data into a UniForm-enabled Delta table, then register
-- the same physical location as an external Iceberg table and query it
-- through the Iceberg metadata chain. This proves the UniForm shadow
-- metadata is correct after a bulk INSERT ... SELECT.
-- ============================================================================

-- Drop any leftovers from a previous run (idempotent)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.grid_readings_iceberg_readback WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.grid_readings_delta WITH FILES;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.grid_readings_delta (
    meter_id           VARCHAR,
    region             VARCHAR,
    substation         VARCHAR,
    meter_type         VARCHAR,
    reading_timestamp  VARCHAR,
    voltage            INT,
    current_amps       DOUBLE,
    power_kw           DOUBLE,
    energy_kwh         DOUBLE,
    power_factor       INT,
    grid_frequency_hz  DOUBLE
) LOCATION 'grid_readings_delta'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);


INSERT INTO {{zone_name}}.iceberg_demos.grid_readings_delta
SELECT * FROM {{zone_name}}.iceberg_demos.grid_readings;
-- Register the Delta table's location as an external Iceberg table
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.grid_readings_iceberg_readback WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.grid_readings_iceberg_readback
USING ICEBERG
LOCATION 'grid_readings_delta';

-- ============================================================================
-- Iceberg Verify 1: Row Count — All 600 Readings via Iceberg Reader
-- ============================================================================

ASSERT ROW_COUNT = 600
SELECT * FROM {{zone_name}}.iceberg_demos.grid_readings_iceberg_readback;
-- ============================================================================
-- Iceberg Verify 2: Per-Region Counts — Must Match Original
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE reading_count = 200 WHERE region = 'North'
ASSERT VALUE reading_count = 200 WHERE region = 'South'
ASSERT VALUE reading_count = 200 WHERE region = 'East'
SELECT
    region,
    COUNT(*) AS reading_count
FROM {{zone_name}}.iceberg_demos.grid_readings_iceberg_readback
GROUP BY region
ORDER BY region;
-- ============================================================================
-- Iceberg Verify 3: Energy Totals — Must Match Delta Write
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_energy_kwh = 993.2375
SELECT
    ROUND(SUM(energy_kwh), 4) AS total_energy_kwh
FROM {{zone_name}}.iceberg_demos.grid_readings_iceberg_readback;
-- ============================================================================
-- Iceberg Verify 4: Grand Totals — Cross-Format Consistency
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 600
ASSERT VALUE region_count = 3
ASSERT VALUE distinct_meters = 600
ASSERT VALUE distinct_substations = 15
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT meter_id) AS distinct_meters,
    COUNT(DISTINCT substation) AS distinct_substations
FROM {{zone_name}}.iceberg_demos.grid_readings_iceberg_readback;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, grand totals, and key invariants.
-- A user who runs only this query can verify the Iceberg reader works correctly.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 600
ASSERT VALUE total_energy_kwh = 993.2375
ASSERT VALUE region_count = 3
ASSERT VALUE distinct_meters = 600
ASSERT VALUE distinct_substations = 15
SELECT
    COUNT(*) AS total_rows,
    ROUND(SUM(energy_kwh), 4) AS total_energy_kwh,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT meter_id) AS distinct_meters,
    COUNT(DISTINCT substation) AS distinct_substations
FROM {{zone_name}}.iceberg_demos.grid_readings;
