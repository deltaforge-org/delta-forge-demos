-- ============================================================================
-- Delta Flexible Ingestion — JSON to Typed Columns — Educational Queries
-- ============================================================================
-- WHAT: Sensor telemetry arrives with a JSON metadata column that holds
--       location, firmware, batch, and alert fields as a single string.
-- WHY:  During initial ingestion, flexible JSON avoids upfront schema design.
--       But as analysts repeatedly query the same JSON fields, extracting them
--       via LIKE on every query is slow and fragile at scale.
-- HOW:  The "schema promotion" pattern uses ALTER TABLE ADD COLUMN to create
--       typed columns, then UPDATE to backfill values from the JSON metadata.
--       New inserts populate both the JSON and the typed column going forward.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Raw telemetry with JSON metadata
-- ============================================================================
-- The metadata column stores heterogeneous JSON — some readings include an
-- "alert" field, others do not. This flexibility is the advantage of JSON,
-- but also its cost: every analytical query must parse the string.

ASSERT ROW_COUNT = 20
SELECT id, sensor_id, reading_type, value, unit, metadata, recorded_at
FROM {{zone_name}}.delta_demos.sensor_telemetry
ORDER BY id;


-- ============================================================================
-- EXPLORE: Sensor and reading type distribution
-- ============================================================================
-- Five sensors across three measurement types. Each sensor reports once per
-- hour, giving 4 readings each over the 4-hour window.

ASSERT ROW_COUNT = 5
ASSERT VALUE reading_count = 4 WHERE sensor_id = 'TEMP-01'
ASSERT VALUE reading_count = 4 WHERE sensor_id = 'HUM-01'
SELECT sensor_id, reading_type, COUNT(*) AS reading_count
FROM {{zone_name}}.delta_demos.sensor_telemetry
GROUP BY sensor_id, reading_type
ORDER BY sensor_id;


-- ============================================================================
-- LEARN: Querying JSON with LIKE — the "before" pattern
-- ============================================================================
-- Before schema promotion, every location filter requires LIKE pattern matching
-- against the raw JSON string. This works but scans every byte of every row.
-- Extracting the alert flag is even worse — it may or may not be present.

ASSERT ROW_COUNT = 8
SELECT id, sensor_id, value, metadata
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE metadata LIKE '%"location":"floor_1_%'
ORDER BY id;

-- Find the 2 alert readings buried in the JSON
ASSERT ROW_COUNT = 2
SELECT id, sensor_id, value, metadata
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE metadata LIKE '%"alert":true%'
ORDER BY id;


-- ============================================================================
-- LEARN: Schema promotion step 1 — ADD COLUMN for location
-- ============================================================================
-- ALTER TABLE ADD COLUMN adds a new typed column. Existing rows get NULL.
-- This is a metadata-only operation — no data files are rewritten.

ALTER TABLE {{zone_name}}.delta_demos.sensor_telemetry ADD COLUMN location VARCHAR;

-- Backfill: extract location from JSON metadata into the typed column.
-- CASE WHEN with LIKE patterns maps each known location value.
UPDATE {{zone_name}}.delta_demos.sensor_telemetry
SET location = CASE
    WHEN metadata LIKE '%"location":"floor_1_east"%' THEN 'floor_1_east'
    WHEN metadata LIKE '%"location":"floor_1_west"%' THEN 'floor_1_west'
    WHEN metadata LIKE '%"location":"boiler_room"%' THEN 'boiler_room'
    WHEN metadata LIKE '%"location":"compressor"%' THEN 'compressor'
    WHEN metadata LIKE '%"location":"clean_room"%' THEN 'clean_room'
END;

-- Verify all 20 rows now have a non-null location
ASSERT VALUE backfilled = 20
SELECT COUNT(*) AS backfilled
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE location IS NOT NULL;


-- ============================================================================
-- LEARN: Schema promotion step 2 — ADD COLUMN for alert_flag
-- ============================================================================
-- The alert field only exists in 2 of 20 JSON payloads. Promoting it to a
-- typed column with a default of 'false' normalizes the sparse data.

ALTER TABLE {{zone_name}}.delta_demos.sensor_telemetry ADD COLUMN alert_flag VARCHAR;

UPDATE {{zone_name}}.delta_demos.sensor_telemetry
SET alert_flag = CASE
    WHEN metadata LIKE '%"alert":true%' THEN 'true'
    ELSE 'false'
END;

-- Verify: 2 alerts, 18 non-alerts
ASSERT VALUE alert_count = 2
SELECT COUNT(*) AS alert_count
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE alert_flag = 'true';

ASSERT VALUE normal_count = 18
SELECT COUNT(*) AS normal_count
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE alert_flag = 'false';


-- ============================================================================
-- LEARN: Querying promoted typed columns — the "after" pattern
-- ============================================================================
-- Now location queries use a simple equality filter on a typed column instead
-- of LIKE against raw JSON. This is cleaner SQL and enables data skipping
-- when the table grows to millions of rows.

ASSERT ROW_COUNT = 4
SELECT id, sensor_id, value, location
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE location = 'boiler_room'
ORDER BY id;


-- ============================================================================
-- LEARN: Analytics on promoted columns — GROUP BY location
-- ============================================================================
-- With location as a typed column, GROUP BY produces clean results without
-- CASE WHEN extraction. This is the payoff of schema promotion.

ASSERT ROW_COUNT = 5
ASSERT VALUE reading_count = 4 WHERE location = 'boiler_room'
ASSERT VALUE reading_count = 4 WHERE location = 'floor_1_east'
SELECT location, COUNT(*) AS reading_count
FROM {{zone_name}}.delta_demos.sensor_telemetry
GROUP BY location
ORDER BY location;


-- ============================================================================
-- LEARN: CTE — firmware version analysis from JSON (not promoted)
-- ============================================================================
-- Not every JSON field needs promotion. Firmware version is queried rarely,
-- so it stays in the JSON metadata. A CTE extracts it on demand using LIKE.

ASSERT ROW_COUNT = 3
ASSERT VALUE sensor_count = 8 WHERE firmware = 'v2.1'
ASSERT VALUE sensor_count = 8 WHERE firmware = 'v3.0'
ASSERT VALUE sensor_count = 4 WHERE firmware = 'v1.8'
WITH firmware_extract AS (
    SELECT sensor_id,
           CASE
               WHEN metadata LIKE '%"firmware":"v2.1"%' THEN 'v2.1'
               WHEN metadata LIKE '%"firmware":"v3.0"%' THEN 'v3.0'
               WHEN metadata LIKE '%"firmware":"v1.8"%' THEN 'v1.8'
           END AS firmware
    FROM {{zone_name}}.delta_demos.sensor_telemetry
)
SELECT firmware, COUNT(*) AS sensor_count
FROM firmware_extract
GROUP BY firmware
ORDER BY firmware;


-- ============================================================================
-- LEARN: Window function — reading trend per sensor
-- ============================================================================
-- LAG shows the previous reading for each sensor, enabling trend detection.
-- A temperature that rises then drops might indicate HVAC cycling.
-- This query combines the promoted location column with window analytics.

ASSERT ROW_COUNT = 8
SELECT sensor_id, location, recorded_at, value,
       LAG(value) OVER (PARTITION BY sensor_id ORDER BY id) AS prev_value
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE reading_type = 'temperature'
ORDER BY sensor_id, id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count
ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.delta_demos.sensor_telemetry;

-- Verify 5 distinct sensors
ASSERT VALUE sensor_count = 5
SELECT COUNT(DISTINCT sensor_id) AS sensor_count FROM {{zone_name}}.delta_demos.sensor_telemetry;

-- Verify 5 distinct locations (from promoted column)
ASSERT VALUE location_count = 5
SELECT COUNT(DISTINCT location) AS location_count FROM {{zone_name}}.delta_demos.sensor_telemetry;

-- Verify 8 temperature readings
ASSERT VALUE temp_count = 8
SELECT COUNT(*) AS temp_count FROM {{zone_name}}.delta_demos.sensor_telemetry WHERE reading_type = 'temperature';

-- Verify 8 pressure readings
ASSERT VALUE press_count = 8
SELECT COUNT(*) AS press_count FROM {{zone_name}}.delta_demos.sensor_telemetry WHERE reading_type = 'pressure';

-- Verify 4 humidity readings
ASSERT VALUE hum_count = 4
SELECT COUNT(*) AS hum_count FROM {{zone_name}}.delta_demos.sensor_telemetry WHERE reading_type = 'humidity';
