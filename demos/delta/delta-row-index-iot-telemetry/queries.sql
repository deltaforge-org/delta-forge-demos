-- ============================================================================
-- Industrial IoT Telemetry — Composite Row-Level Index
-- ============================================================================
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                   WHAT IS A COMPOSITE INDEX?                         │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │                                                                      │
--  │ A composite index covers several columns at once, in a FIXED order.  │
--  │ For an index on (sensor_id, reading_time) the rows of the index are  │
--  │ sorted FIRST by sensor_id, then by reading_time within each sensor:  │
--  │                                                                      │
--  │     sensor_id         reading_time                                   │
--  │     ──────────────    ────────────                                   │
--  │     TMP-A05           08:00                                          │
--  │     TMP-A05           08:30                                          │
--  │     TMP-A05           09:00                                          │
--  │     TMP-B07           08:00      ← sensor_id changed, time resets    │
--  │     TMP-B07           08:30                                          │
--  │     VIB-A01           08:00                                          │
--  │     ...                                                              │
--  │                                                                      │
--  │ This shape is great for "give me a specific sensor, optionally with  │
--  │ a time window". It's NOT helpful for "give me everything at 09:00    │
--  │ across all sensors" — the rows for 09:00 are scattered across the    │
--  │ structure rather than grouped together.                              │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                THE LEFTMOST-PREFIX RULE                              │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │                                                                      │
--  │ A composite index on (a, b, c) helps queries that filter on:         │
--  │                                                                      │
--  │     ✓ a                  (leftmost column alone)                     │
--  │     ✓ a AND b            (leading prefix)                            │
--  │     ✓ a AND b AND c      (full prefix)                               │
--  │     ✗ b                  (skips the leading column — NO HELP)        │
--  │     ✗ c                  (skips the leading columns — NO HELP)       │
--  │     ✗ b AND c            (skips the leading column — NO HELP)        │
--  │                                                                      │
--  │ "Leftmost prefix" means: predicates must constrain the columns       │
--  │ from the left, in order, with no gaps. The moment you skip a leading │
--  │ column, the index can no longer narrow the search.                   │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │             WHEN TO USE A COMPOSITE INDEX                            │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ ✓ Two or more columns are filtered TOGETHER in the same query        │
--  │ ✓ The columns have a natural lookup order (e.g. sensor first,       │
--  │   then time within that sensor)                                      │
--  │ ✓ Single-column queries on the LEADING column are also common —     │
--  │   the index serves both shapes at once                               │
--  │                                                                      │
--  │ When NOT to use one:                                                 │
--  │ ✗ The columns are usually queried independently — build separate    │
--  │   indexes instead                                                    │
--  │ ✗ The "natural" leading column is low-cardinality (e.g. status):   │
--  │   the leading-column narrowing is weak, hurting overall benefit      │
--  │ ✗ The trailing column is queried on its own — that path won't be   │
--  │   accelerated; you'd need a second index keyed on it                 │
--  └──────────────────────────────────────────────────────────────────────┘
--
-- This demo runs five queries: one with the leading column alone (index
-- helps), one with both columns (index helps fully), one with the
-- TRAILING column alone (leftmost-prefix violated — index does NOT
-- help, but the answer is still correct), and an IN list on the
-- leading column. The contrast makes the rule concrete.
-- ============================================================================


-- ============================================================================
-- BUILD: Create the Composite Index
-- ============================================================================
-- A composite on (sensor_id, reading_time) is one sorted structure where
-- rows are arranged by sensor_id first, then by reading_time within each
-- sensor. That order is what makes the leftmost-prefix rule work:
-- predicates on the leading column (or leading + trailing) hit the
-- index; predicates on the trailing column alone do not.

CREATE INDEX idx_sensor_time
    ON TABLE {{zone_name}}.delta_demos.sensor_telemetry (sensor_id, reading_time)
    WITH (auto_update = true);


-- ============================================================================
-- EXPLORE: Sensor Mix and Quality Distribution
-- ============================================================================
-- 8 sensors × 10 readings each = 80 rows. Two sensor kinds (vibration
-- and temperature), spread across two production lines.

ASSERT ROW_COUNT = 8
ASSERT VALUE reading_count = 10
SELECT sensor_id,
       COUNT(*)        AS reading_count,
       MAX(value)      AS peak_value,
       MIN(value)      AS lowest_value
FROM {{zone_name}}.delta_demos.sensor_telemetry
GROUP BY sensor_id
ORDER BY sensor_id;


-- ============================================================================
-- LEARN: Leading-Column Lookup — Index Helps Fully
-- ============================================================================
-- Pulling VIB-B03's full history. The predicate uses only the leading
-- column (sensor_id), which is exactly what a composite index is built
-- to accelerate.

ASSERT ROW_COUNT = 10
ASSERT VALUE max_value = 0.81
ASSERT VALUE alarm_count = 1
ASSERT VALUE warn_count = 9
SELECT COUNT(*)                                      AS reading_count,
       MAX(value)                                    AS max_value,
       COUNT(*) FILTER (WHERE quality = 'alarm')     AS alarm_count,
       COUNT(*) FILTER (WHERE quality = 'warn')      AS warn_count
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE sensor_id = 'VIB-B03';


-- ============================================================================
-- LEARN: Composite Lookup — Both Columns Used
-- ============================================================================
-- Reconciling TMP-B08's readings against a maintenance window between
-- 09:00 and 13:00. The predicate uses sensor_id (leading) AND
-- reading_time (trailing). The index narrows on sensor_id first, then
-- the trailing range narrows further within that sensor's slice.

ASSERT ROW_COUNT = 4
ASSERT VALUE max_temp = 85.0
ASSERT VALUE alarm_count = 4
SELECT COUNT(*)                                  AS reading_count,
       MAX(value)                                AS max_temp,
       COUNT(*) FILTER (WHERE quality = 'alarm') AS alarm_count
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE sensor_id = 'TMP-B08'
  AND reading_time BETWEEN '2026-04-15T09:00:00' AND '2026-04-15T13:00:00';


-- ============================================================================
-- LEARN: Trailing-Column-Only — Leftmost-Prefix Rule Violated
-- ============================================================================
-- A predicate on reading_time WITHOUT sensor_id does NOT use this
-- index. A composite (sensor_id, reading_time) is sorted by sensor_id
-- first; values of reading_time are scattered across the structure.
-- The query still returns the correct answer — it just falls back to
-- ordinary file pruning. To accelerate this shape you would build a
-- separate index keyed on reading_time first.

ASSERT ROW_COUNT = 8
ASSERT VALUE alarm_count = 2
SELECT COUNT(*)                                  AS reading_count,
       COUNT(*) FILTER (WHERE quality = 'alarm') AS alarm_count
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE reading_time = '2026-04-15T10:00:00';


-- ============================================================================
-- LEARN: IN List on Leading Column
-- ============================================================================
-- IN lists work like several point lookups. The index narrows to the
-- two sensor slices and skips the rest.

ASSERT ROW_COUNT = 20
ASSERT VALUE warn_count = 10
SELECT COUNT(*)                                AS reading_count,
       COUNT(*) FILTER (WHERE quality = 'warn') AS warn_count
FROM {{zone_name}}.delta_demos.sensor_telemetry
WHERE sensor_id IN ('VIB-A01', 'TMP-A06');


-- ============================================================================
-- LEARN: Index Status
-- ============================================================================

DESCRIBE INDEX idx_sensor_time ON TABLE {{zone_name}}.delta_demos.sensor_telemetry;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_readings = 80
ASSERT VALUE distinct_sensors = 8
ASSERT VALUE good_count = 50
ASSERT VALUE warn_count = 22
ASSERT VALUE alarm_count = 8
ASSERT VALUE vibration_sensors = 4
ASSERT VALUE temperature_sensors = 4
SELECT COUNT(*)                                                    AS total_readings,
       COUNT(DISTINCT sensor_id)                                    AS distinct_sensors,
       COUNT(*) FILTER (WHERE quality = 'good')                     AS good_count,
       COUNT(*) FILTER (WHERE quality = 'warn')                     AS warn_count,
       COUNT(*) FILTER (WHERE quality = 'alarm')                    AS alarm_count,
       COUNT(DISTINCT sensor_id) FILTER (WHERE sensor_kind = 'vibration')   AS vibration_sensors,
       COUNT(DISTINCT sensor_id) FILTER (WHERE sensor_kind = 'temperature') AS temperature_sensors
FROM {{zone_name}}.delta_demos.sensor_telemetry;
