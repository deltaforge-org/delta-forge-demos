-- ============================================================================
-- Delta Type Widening — IoT Fleet Counter Overflow — Educational Queries
-- ============================================================================
-- WHAT: Type widening promotes a column's type to a wider one (INT → BIGINT,
--       FLOAT → DOUBLE) via ALTER TABLE ALTER COLUMN TYPE, without rewriting
--       existing Parquet data files.
-- WHY:  IoT device counters accumulate over months. A gateway processing
--       50K events/day reaches 2.1 billion (INT max) in ~4 years. Type
--       widening avoids a costly full-table rewrite when this happens.
-- HOW:  Delta records the widened type in the transaction log metadata.
--       Old Parquet files keep their INT encoding. When read, the engine
--       automatically upcasts INT values to BIGINT — no physical rewrite.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline device telemetry — all values fit in INT
-- ============================================================================
-- The fleet has 25 devices: 12 gateways (GW-*), 6 sensors (SNS-*),
-- 3 cameras (CAM-*), and 4 routers (RTR-*). All event_count and
-- bytes_sent values are well within INT range (max 2,147,483,647).

ASSERT ROW_COUNT = 10
SELECT id, device_id, region, event_count, bytes_sent, avg_latency
FROM {{zone_name}}.delta_demos.device_telemetry
ORDER BY id
LIMIT 10;


-- ============================================================================
-- EXPLORE: How close are we to INT limits?
-- ============================================================================
-- Right now, the largest counter is 225,000 — just 0.01% of INT max.
-- But these are daily snapshots. Cumulative counters grow fast.

ASSERT ROW_COUNT = 1
ASSERT VALUE max_event_count = 225000
ASSERT VALUE max_bytes_sent = 5500000
SELECT MAX(event_count) AS max_event_count,
       MAX(bytes_sent) AS max_bytes_sent
FROM {{zone_name}}.delta_demos.device_telemetry;


-- ============================================================================
-- PHASE 1: Enable type widening and promote columns to BIGINT
-- ============================================================================
-- Before counter accumulation pushes values past INT range, we proactively
-- widen the columns. This is a metadata-only operation — it updates the
-- Delta transaction log schema but does NOT rewrite existing Parquet files.
-- The old files retain their INT encoding; the engine upcasts on read.

ALTER TABLE {{zone_name}}.delta_demos.device_telemetry SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');

ALTER TABLE {{zone_name}}.delta_demos.device_telemetry ALTER COLUMN event_count TYPE BIGINT;

ALTER TABLE {{zone_name}}.delta_demos.device_telemetry ALTER COLUMN bytes_sent TYPE BIGINT;


-- ============================================================================
-- OBSERVE: Schema now shows BIGINT for widened columns
-- ============================================================================
-- The information_schema reflects the new types immediately. Existing data
-- is still stored as INT in Parquet — the engine upcasts transparently.

ASSERT ROW_COUNT = 8
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'delta_demos'
  AND table_name = 'device_telemetry'
ORDER BY ordinal_position;


-- ============================================================================
-- PHASE 2: Simulate 4 years of counter accumulation for gateways
-- ============================================================================
-- Each gateway processes tens of thousands of events daily. Over ~48,000
-- accumulation cycles, cumulative counters grow dramatically. Without
-- type widening, this UPDATE would overflow INT. With BIGINT columns,
-- values safely exceed the 2.1 billion INT limit.

UPDATE {{zone_name}}.delta_demos.device_telemetry
SET event_count = event_count * 48000,
    bytes_sent = bytes_sent * 48000
WHERE device_id LIKE 'GW-%';


-- ============================================================================
-- LEARN: Which gateways now exceed INT range?
-- ============================================================================
-- After accumulation, 6 of 12 gateways have event_count > 2,147,483,647
-- (the INT maximum). These values are only possible because we widened
-- to BIGINT before the UPDATE. The old Parquet files (with INT-encoded
-- values for sensors, cameras, routers) coexist seamlessly with new
-- Parquet files (BIGINT-encoded gateway values).

ASSERT ROW_COUNT = 6
SELECT device_id, region, event_count, bytes_sent
FROM {{zone_name}}.delta_demos.device_telemetry
WHERE device_id LIKE 'GW-%' AND event_count > 2147483647
ORDER BY event_count DESC;


-- ============================================================================
-- OBSERVE: Post-accumulation gateway values
-- ============================================================================
-- All 12 gateways have been scaled. The smallest (GW-SYD-003 at 1.056B)
-- still fits in INT, but the largest (GW-TKY-001 at 3.456B) does not.

ASSERT ROW_COUNT = 12
ASSERT VALUE event_count = 3456000000 WHERE device_id = 'GW-TKY-001'
ASSERT VALUE event_count = 2160000000 WHERE device_id = 'GW-NYC-001'
ASSERT VALUE bytes_sent = 57600000000 WHERE device_id = 'GW-NYC-001'
SELECT device_id, region, event_count, bytes_sent, avg_latency
FROM {{zone_name}}.delta_demos.device_telemetry
WHERE device_id LIKE 'GW-%'
ORDER BY event_count DESC;


-- ============================================================================
-- PHASE 3: Insert 10 high-volume devices that require BIGINT from day one
-- ============================================================================
-- New edge nodes and CDN servers join the fleet. Their counters already
-- exceed INT range. These rows are written to new Parquet files with
-- BIGINT encoding — coexisting with old INT-encoded Parquet files.

ASSERT ROW_COUNT = 10
INSERT INTO {{zone_name}}.delta_demos.device_telemetry
SELECT * FROM (VALUES
    (26, 'GW-NYC-004',     'us-east',  3500000000,  85000000000,   11.20, 'active', '2025-07-15'),
    (27, 'GW-LON-004',     'eu-west',  2800000000,  72000000000,   18.90, 'active', '2025-07-15'),
    (28, 'GW-TKY-004',     'ap-east',  4100000000,  95000000000,   27.30, 'active', '2025-07-15'),
    (29, 'GW-SYD-004',     'ap-south', 1900000000,  48000000000,   39.60, 'active', '2025-07-15'),
    (30, 'EDGE-US-001',    'us-east',  8500000000,  210000000000,  7.80,  'active', '2025-07-15'),
    (31, 'EDGE-EU-001',    'eu-west',  7200000000,  180000000000,  9.50,  'active', '2025-07-15'),
    (32, 'EDGE-AP-001',    'ap-east',  9100000000,  230000000000,  6.20,  'active', '2025-07-15'),
    (33, 'EDGE-AP-002',    'ap-south', 6800000000,  165000000000,  11.40, 'active', '2025-07-15'),
    (34, 'CDN-GLOBAL-001', 'us-east',  15000000000, 500000000000,  2.10,  'active', '2025-07-15'),
    (35, 'CDN-GLOBAL-002', 'eu-west',  12000000000, 420000000000,  3.40,  'active', '2025-07-15')
) AS t(id, device_id, region, event_count, bytes_sent, avg_latency, status, reported_date);


-- ============================================================================
-- LEARN: Mixed-era Parquet files coexist seamlessly
-- ============================================================================
-- The table now spans three eras of Parquet files:
--   Era 1: Original INT-encoded files (sensors, cameras, routers — unchanged)
--   Era 2: BIGINT-encoded files from the gateway UPDATE
--   Era 3: BIGINT-encoded files from the new device INSERT
-- Delta's type widening metadata tells the engine to upcast Era 1 values.

ASSERT ROW_COUNT = 4
ASSERT VALUE device_count = 10 WHERE region = 'us-east'
ASSERT VALUE total_events = 33960590000 WHERE region = 'us-east'
SELECT region,
       COUNT(*) AS device_count,
       SUM(event_count) AS total_events,
       SUM(bytes_sent) AS total_bytes,
       ROUND(AVG(avg_latency), 2) AS avg_latency
FROM {{zone_name}}.delta_demos.device_telemetry
GROUP BY region
ORDER BY total_events DESC;


-- ============================================================================
-- LEARN: Device type analysis — small sensors coexist with massive CDNs
-- ============================================================================
-- The same table holds sensor readings (119K events) alongside CDN counters
-- (15 billion events) — a 126,000x range. Without type widening, the CDN
-- values would have required a full table rewrite or a separate table.

ASSERT ROW_COUNT = 6
ASSERT VALUE max_events = 15000000000 WHERE device_type = 'CDN'
ASSERT VALUE min_events = 119000 WHERE device_type = 'Sensor'
SELECT CASE
         WHEN device_id LIKE 'GW-%'   THEN 'Gateway'
         WHEN device_id LIKE 'SNS-%'  THEN 'Sensor'
         WHEN device_id LIKE 'CAM-%'  THEN 'Camera'
         WHEN device_id LIKE 'RTR-%'  THEN 'Router'
         WHEN device_id LIKE 'EDGE-%' THEN 'Edge Node'
         WHEN device_id LIKE 'CDN-%'  THEN 'CDN'
       END AS device_type,
       COUNT(*) AS device_count,
       MIN(event_count) AS min_events,
       MAX(event_count) AS max_events
FROM {{zone_name}}.delta_demos.device_telemetry
GROUP BY CASE
         WHEN device_id LIKE 'GW-%'   THEN 'Gateway'
         WHEN device_id LIKE 'SNS-%'  THEN 'Sensor'
         WHEN device_id LIKE 'CAM-%'  THEN 'Camera'
         WHEN device_id LIKE 'RTR-%'  THEN 'Router'
         WHEN device_id LIKE 'EDGE-%' THEN 'Edge Node'
         WHEN device_id LIKE 'CDN-%'  THEN 'CDN'
       END
ORDER BY max_events DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 35
ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.delta_demos.device_telemetry;

-- Verify 4 distinct regions
ASSERT VALUE region_count = 4
SELECT COUNT(DISTINCT region) AS region_count FROM {{zone_name}}.delta_demos.device_telemetry;

-- Verify GW-NYC-001 accumulated counter (45000 * 48000)
ASSERT VALUE event_count = 2160000000
SELECT event_count FROM {{zone_name}}.delta_demos.device_telemetry WHERE id = 1;

-- Verify GW-NYC-001 accumulated bytes (1200000 * 48000)
ASSERT VALUE bytes_sent = 57600000000
SELECT bytes_sent FROM {{zone_name}}.delta_demos.device_telemetry WHERE id = 1;

-- Verify CDN-GLOBAL-001 event count (BIGINT insert)
ASSERT VALUE event_count = 15000000000
SELECT event_count FROM {{zone_name}}.delta_demos.device_telemetry WHERE id = 34;

-- Verify CDN-GLOBAL-001 bytes sent (BIGINT insert)
ASSERT VALUE bytes_sent = 500000000000
SELECT bytes_sent FROM {{zone_name}}.delta_demos.device_telemetry WHERE id = 34;

-- Verify max event_count across all devices
ASSERT VALUE max_event_count = 15000000000
SELECT MAX(event_count) AS max_event_count FROM {{zone_name}}.delta_demos.device_telemetry;

-- Verify 15 rows exceed INT max for event_count
ASSERT VALUE exceeds_int_count = 15
SELECT COUNT(*) AS exceeds_int_count FROM {{zone_name}}.delta_demos.device_telemetry WHERE event_count > 2147483647;

-- Verify sensor reading unchanged by gateway UPDATE (id=13)
ASSERT VALUE event_count = 150000
SELECT event_count FROM {{zone_name}}.delta_demos.device_telemetry WHERE id = 13;

-- Verify schema shows BIGINT for widened columns
ASSERT VALUE column_count = 8
SELECT COUNT(*) AS column_count FROM information_schema.columns
WHERE table_schema = 'delta_demos' AND table_name = 'device_telemetry';
