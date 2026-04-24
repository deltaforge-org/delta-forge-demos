-- ============================================================================
-- Iceberg UniForm Partition Transforms — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH PARTITION TRANSFORMS
-- --------------------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- When a Delta table is PARTITIONED BY (CAST(event_timestamp AS DATE)),
-- the Iceberg metadata includes a partition spec with a day() transform
-- on event_timestamp. This means:
--   - Delta reads use the generated date partition column for pruning
--   - External Iceberg engines use the day(event_timestamp) transform
--   - Both produce identical partition boundaries (one per calendar day)
--
-- INSERTs into existing days route to the correct partition. INSERTs for
-- new days create new partitions automatically. DELETEs within a single
-- day only rewrite that day's data files.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running, verify the partition spec in the Iceberg metadata:
--   python3 verify_iceberg_metadata.py <table_data_path>/app_events -v
-- ============================================================================
-- ============================================================================
-- EXPLORE: Baseline — All 36 Events
-- ============================================================================

ASSERT ROW_COUNT = 36
SELECT * FROM {{zone_name}}.iceberg_demos.app_events ORDER BY event_id;
-- ============================================================================
-- Query 1: Events Per Day
-- ============================================================================
-- Each day has exactly 6 events. Iceberg partition pruning can skip
-- entire data files for single-day queries.

ASSERT ROW_COUNT = 6
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-01'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-02'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-03'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-04'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-05'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-06'
SELECT
    CAST(event_timestamp AS DATE) AS event_date,
    COUNT(*) AS event_count,
    SUM(payload_size) AS total_payload
FROM {{zone_name}}.iceberg_demos.app_events
GROUP BY CAST(event_timestamp AS DATE)
ORDER BY event_date;
-- ============================================================================
-- Query 2: Events Per Type
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE type_count = 6 WHERE event_type = 'click'
ASSERT VALUE type_count = 6 WHERE event_type = 'error'
ASSERT VALUE type_count = 6 WHERE event_type = 'login'
ASSERT VALUE type_count = 6 WHERE event_type = 'logout'
ASSERT VALUE type_count = 6 WHERE event_type = 'page_view'
ASSERT VALUE type_count = 6 WHERE event_type = 'purchase'
SELECT
    event_type,
    COUNT(*) AS type_count
FROM {{zone_name}}.iceberg_demos.app_events
GROUP BY event_type
ORDER BY event_type;
-- ============================================================================
-- Query 3: Severity Distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sev_count = 5 WHERE severity = 'error'
ASSERT VALUE sev_count = 30 WHERE severity = 'info'
ASSERT VALUE sev_count = 1 WHERE severity = 'warning'
SELECT
    severity,
    COUNT(*) AS sev_count
FROM {{zone_name}}.iceberg_demos.app_events
GROUP BY severity
ORDER BY severity;
-- ============================================================================
-- Query 4: Single-Day Partition Read — 2024-03-01 Only
-- ============================================================================
-- With partition pruning, Iceberg engines only read 2024-03-01 data files.

ASSERT ROW_COUNT = 6
SELECT
    event_id, user_id, event_type, event_timestamp, payload_size, source_app, severity
FROM {{zone_name}}.iceberg_demos.app_events
WHERE CAST(event_timestamp AS DATE) = '2024-03-01'
ORDER BY event_id;
-- ============================================================================
-- LEARN: INSERT Into Existing Day Partitions (Version 2 / Snapshot 2)
-- ============================================================================
-- Add 3 events to existing days. Proves partition routing works — each
-- event lands in the correct day partition without creating duplicates.

INSERT INTO {{zone_name}}.iceberg_demos.app_events VALUES
    (37, 'usr_131', 'click',     TIMESTAMP '2024-03-01 20:00:00', 384,  'web-app',    'info'),
    (38, 'usr_132', 'purchase',  TIMESTAMP '2024-03-03 21:15:00', 640,  'mobile-app', 'info'),
    (39, 'usr_133', 'login',     TIMESTAMP '2024-03-05 06:30:00', 128,  'web-app',    'info');
-- ============================================================================
-- Query 5: Per-Day Counts After Insert Into Existing Days
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-01'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-02'
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-03'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-04'
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-05'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-06'
SELECT
    CAST(event_timestamp AS DATE) AS event_date,
    COUNT(*) AS event_count
FROM {{zone_name}}.iceberg_demos.app_events
GROUP BY CAST(event_timestamp AS DATE)
ORDER BY event_date;
-- ============================================================================
-- LEARN: INSERT New Day Partition — 2024-03-07 (Version 3 / Snapshot 3)
-- ============================================================================
-- Adding events for a new day creates a new partition automatically.
-- The Iceberg manifest list gains a new manifest entry for 2024-03-07.

INSERT INTO {{zone_name}}.iceberg_demos.app_events VALUES
    (40, 'usr_134', 'login',     TIMESTAMP '2024-03-07 08:00:00', 128,  'web-app',    'info'),
    (41, 'usr_135', 'click',     TIMESTAMP '2024-03-07 09:30:00', 256,  'mobile-app', 'info'),
    (42, 'usr_136', 'purchase',  TIMESTAMP '2024-03-07 12:00:00', 960,  'web-app',    'info'),
    (43, 'usr_137', 'error',     TIMESTAMP '2024-03-07 14:45:00', 1536, 'api-server', 'warning'),
    (44, 'usr_138', 'page_view', TIMESTAMP '2024-03-07 16:00:00', 192,  'web-app',    'info'),
    (45, 'usr_134', 'logout',    TIMESTAMP '2024-03-07 18:30:00', 64,   'mobile-app', 'info');
-- ============================================================================
-- Query 6: Per-Day Counts After New Partition
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-01'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-02'
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-03'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-04'
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-05'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-06'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-07'
SELECT
    CAST(event_timestamp AS DATE) AS event_date,
    COUNT(*) AS event_count
FROM {{zone_name}}.iceberg_demos.app_events
GROUP BY CAST(event_timestamp AS DATE)
ORDER BY event_date;
-- ============================================================================
-- LEARN: UPDATE — Escalate High-Payload Errors to Critical (Version 4)
-- ============================================================================
-- Change severity to 'critical' for all api-server events with
-- payload_size > 1500. This touches data files across multiple partitions.

UPDATE {{zone_name}}.iceberg_demos.app_events
SET severity = 'critical'
WHERE source_app = 'api-server' AND payload_size > 1500;
-- ============================================================================
-- Query 7: Severity Distribution After Update
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE sev_count = 5 WHERE severity = 'critical'
ASSERT VALUE sev_count = 1 WHERE severity = 'error'
ASSERT VALUE sev_count = 38 WHERE severity = 'info'
ASSERT VALUE sev_count = 1 WHERE severity = 'warning'
SELECT
    severity,
    COUNT(*) AS sev_count
FROM {{zone_name}}.iceberg_demos.app_events
GROUP BY severity
ORDER BY severity;
-- ============================================================================
-- LEARN: DELETE — Remove Error Events From 2024-03-01 (Version 5)
-- ============================================================================
-- Partition-scoped delete: only the 2024-03-01 partition is rewritten.
-- The Iceberg manifest for other days remains unchanged.

DELETE FROM {{zone_name}}.iceberg_demos.app_events
WHERE CAST(event_timestamp AS DATE) = '2024-03-01' AND event_type = 'error';
-- ============================================================================
-- Query 8: Per-Day Counts After Delete
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-01'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-02'
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-03'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-04'
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-05'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-06'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-07'
SELECT
    CAST(event_timestamp AS DATE) AS event_date,
    COUNT(*) AS event_count
FROM {{zone_name}}.iceberg_demos.app_events
GROUP BY CAST(event_timestamp AS DATE)
ORDER BY event_date;
-- ============================================================================
-- Query 9: Time Travel — Pre-Mutation vs Post-Mutation
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_count = 36
ASSERT VALUE v1_payload = 19520
ASSERT VALUE final_count = 44
ASSERT VALUE final_payload = 22784
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.app_events VERSION AS OF 1) AS v1_count,
    (SELECT SUM(payload_size) FROM {{zone_name}}.iceberg_demos.app_events VERSION AS OF 1) AS v1_payload,
    COUNT(*) AS final_count,
    SUM(payload_size) AS final_payload
FROM {{zone_name}}.iceberg_demos.app_events;
-- ============================================================================
-- Query 10: Version History
-- ============================================================================
-- 5 versions: seed, insert existing days, insert new day, update severity,
-- delete error events.

ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.app_events;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Grand totals across all partitions after all mutations.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_events = 44
ASSERT VALUE total_days = 7
ASSERT VALUE total_types = 6
ASSERT VALUE total_payload = 22784
ASSERT VALUE error_type_count = 6
ASSERT VALUE critical_sev_count = 5
ASSERT VALUE march_07_count = 6
SELECT
    COUNT(*) AS total_events,
    COUNT(DISTINCT CAST(event_timestamp AS DATE)) AS total_days,
    COUNT(DISTINCT event_type) AS total_types,
    SUM(payload_size) AS total_payload,
    COUNT(*) FILTER (WHERE event_type = 'error') AS error_type_count,
    COUNT(*) FILTER (WHERE severity = 'critical') AS critical_sev_count,
    COUNT(*) FILTER (WHERE CAST(event_timestamp AS DATE) = '2024-03-07') AS march_07_count
FROM {{zone_name}}.iceberg_demos.app_events;
-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents day-partitioned data after
-- partition routing, new partition creation, cross-partition UPDATE,
-- and partition-scoped DELETE.
--
-- The Iceberg partition spec should show a day(event_timestamp) transform.
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.app_events_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.app_events_iceberg
USING ICEBERG
LOCATION 'app_events';

-- ============================================================================
-- Iceberg Verify 1: Row Count + Spot-Check Individual Rows
-- ============================================================================
-- Verify specific rows survived the full mutation cycle (INSERT, UPDATE,
-- DELETE) and are visible through the Iceberg metadata chain.

ASSERT ROW_COUNT = 44
ASSERT VALUE event_type = 'click' WHERE event_id = 1
ASSERT VALUE source_app = 'web-app' WHERE event_id = 1
ASSERT VALUE severity = 'info' WHERE event_id = 1
ASSERT VALUE user_id = 'usr_134' WHERE event_id = 40
ASSERT VALUE event_type = 'login' WHERE event_id = 40
ASSERT VALUE source_app = 'web-app' WHERE event_id = 40
SELECT * FROM {{zone_name}}.iceberg_demos.app_events_iceberg ORDER BY event_id;
-- ============================================================================
-- Iceberg Verify 2: Per-Day Counts — Reflect All Mutations
-- ============================================================================
-- After INSERT (existing days), INSERT (new day 2024-03-07), UPDATE
-- (severity escalation), and DELETE (error events from 2024-03-01),
-- per-day counts must match the Delta final state exactly.

ASSERT ROW_COUNT = 7
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-01'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-02'
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-03'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-04'
ASSERT VALUE event_count = 7 WHERE event_date = '2024-03-05'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-06'
ASSERT VALUE event_count = 6 WHERE event_date = '2024-03-07'
SELECT
    CAST(event_timestamp AS DATE) AS event_date,
    COUNT(*) AS event_count
FROM {{zone_name}}.iceberg_demos.app_events_iceberg
GROUP BY CAST(event_timestamp AS DATE)
ORDER BY event_date;
-- ============================================================================
-- Iceberg Verify 3: Severity Distribution — Reflects UPDATE + DELETE
-- ============================================================================
-- The UPDATE escalated high-payload api-server errors to 'critical'.
-- The DELETE removed the error event from 2024-03-01.
-- Must match the Delta severity distribution exactly.

ASSERT ROW_COUNT = 3
ASSERT VALUE sev_count = 5 WHERE severity = 'critical'
ASSERT VALUE sev_count = 38 WHERE severity = 'info'
ASSERT VALUE sev_count = 1 WHERE severity = 'warning'
SELECT
    severity,
    COUNT(*) AS sev_count
FROM {{zone_name}}.iceberg_demos.app_events_iceberg
GROUP BY severity
ORDER BY severity;
-- ============================================================================
-- Iceberg Verify 4: Grand Totals — Must Match Delta Final State
-- ============================================================================
-- Cross-cutting aggregate check: total events, distinct days, distinct
-- event types, total payload, and filtered counts must all agree with
-- the Delta table's final state.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_events = 44
ASSERT VALUE total_days = 7
ASSERT VALUE total_types = 6
ASSERT VALUE total_payload = 22784
ASSERT VALUE error_type_count = 6
ASSERT VALUE critical_sev_count = 5
ASSERT VALUE march_07_count = 6
SELECT
    COUNT(*) AS total_events,
    COUNT(DISTINCT CAST(event_timestamp AS DATE)) AS total_days,
    COUNT(DISTINCT event_type) AS total_types,
    SUM(payload_size) AS total_payload,
    COUNT(*) FILTER (WHERE event_type = 'error') AS error_type_count,
    COUNT(*) FILTER (WHERE severity = 'critical') AS critical_sev_count,
    COUNT(*) FILTER (WHERE CAST(event_timestamp AS DATE) = '2024-03-07') AS march_07_count
FROM {{zone_name}}.iceberg_demos.app_events_iceberg;
