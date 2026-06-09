-- ============================================================================
-- Delta Partitions & Deletion Vectors — Educational Queries
-- ============================================================================
-- WHAT: Deletion vectors (DVs) are lightweight sidecar files that mark
--       individual rows as deleted without rewriting the entire Parquet
--       data file.
-- WHY:  Traditional Delta deletes must rewrite the full data file to
--       exclude deleted rows, which is expensive for large files with
--       few deletions. DVs make DELETE nearly instant by writing only a
--       small bitmap file alongside the original data file.
-- HOW:  When a DELETE runs, Delta writes a .bin deletion vector file
--       that records which row indices in the original Parquet file are
--       deleted. Readers apply the DV as a filter. OPTIMIZE later merges
--       the DVs back into compacted data files.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Event Distribution Across Partitions
-- ============================================================================
-- The cloud_events table is partitioned by region (us-east, us-west,
-- eu-west). Each partition's data files live in their own directory.
-- Let's see the starting state: 30 events per region, 90 total.

ASSERT ROW_COUNT = 3
SELECT region,
       COUNT(*) AS event_count,
       COUNT(DISTINCT service) AS services,
       COUNT(*) FILTER (WHERE severity = 'error') AS errors,
       COUNT(*) FILTER (WHERE severity = 'warning') AS warnings,
       COUNT(*) FILTER (WHERE severity = 'info') AS info
FROM {{zone_name}}.delta_demos.cloud_events
GROUP BY region
ORDER BY region;


-- ============================================================================
-- STEP 1: DELETE — Remove 5 low-severity info events per region (triggers DVs)
-- ============================================================================
-- These deletes target low-latency info events that add noise. Instead of
-- rewriting entire Parquet files, Delta writes small deletion vector (.bin)
-- sidecar files that mark the specific row positions as deleted. The original
-- data files remain untouched — only a lightweight bitmap is written.

-- us-east: remove ids 2, 11, 18, 25, 29 (low-latency info events)
ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.cloud_events
WHERE id IN (2, 11, 18, 25, 29);

-- us-west: remove ids 32, 42, 43, 54, 59 (low-latency info events)
ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.cloud_events
WHERE id IN (32, 42, 43, 54, 59);

-- eu-west: remove ids 68, 70, 78, 84, 89 (low-latency info events)
ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.cloud_events
WHERE id IN (68, 70, 78, 84, 89);


-- ============================================================================
-- LEARN: Confirm Deletion Vectors Worked
-- ============================================================================
-- 15 rows were deleted (5 per region). The count below should return 0,
-- confirming that the deletion vectors are filtering out those rows even
-- though the original Parquet files still physically contain them.

ASSERT VALUE deleted_events_found = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS deleted_events_found
FROM {{zone_name}}.delta_demos.cloud_events
WHERE id IN (2, 11, 18, 25, 29, 32, 42, 43, 54, 59, 68, 70, 78, 84, 89);


-- Check the per-region counts after deletion: 25 per region, 75 total.
ASSERT ROW_COUNT = 3
SELECT region,
       COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.cloud_events
GROUP BY region
ORDER BY region;


-- ============================================================================
-- STEP 2: UPDATE — Escalate severity for high-latency events (> 500ms)
-- ============================================================================
-- Events with high latency indicate real problems. We escalate their severity:
--   - error events with latency > 500ms become 'critical'
--   - warning events with latency > 500ms become 'error'
--
-- Under the hood, each UPDATE generates deletion vectors (marking the old row
-- versions as deleted) plus new data files containing the updated severity.

ASSERT ROW_COUNT = 11
UPDATE {{zone_name}}.delta_demos.cloud_events
SET severity = 'critical'
WHERE latency_ms > 500 AND severity = 'error';

ASSERT ROW_COUNT = 6
UPDATE {{zone_name}}.delta_demos.cloud_events
SET severity = 'error'
WHERE latency_ms > 500 AND severity = 'warning';


-- ============================================================================
-- LEARN: See the Escalated Events
-- ============================================================================
-- Let's verify the severity escalation. Events that were 'error' with
-- latency > 500ms should now be 'critical'. Events that were 'warning'
-- with latency > 500ms should now be 'error'.
-- All 11 escalated events should have latency > 500ms

ASSERT ROW_COUNT = 11
SELECT id, service, region, severity, latency_ms, message
FROM {{zone_name}}.delta_demos.cloud_events
WHERE severity = 'critical' AND id <= 90
ORDER BY latency_ms DESC;

-- Verify: no critical events with low latency (all must have latency > 500ms)
ASSERT VALUE misescalated = 0
SELECT COUNT(*) AS misescalated
FROM {{zone_name}}.delta_demos.cloud_events
WHERE severity = 'critical' AND id <= 90 AND latency_ms <= 500;


-- ============================================================================
-- STEP 3: INSERT — Add 10 critical events to us-east
-- ============================================================================
-- A critical incident wave hits the us-east region. These new rows are
-- written as new data files only in the region=us-east/ partition directory.
-- Other partitions (us-west, eu-west) are completely unaffected.

ASSERT ROW_COUNT = 10
INSERT INTO {{zone_name}}.delta_demos.cloud_events
SELECT * FROM (VALUES
    (91,  'api-gateway',  'us-east', 'critical', 'API gateway overloaded',          1800, '2024-06-01 11:00:00'),
    (92,  'auth-service', 'us-east', 'critical', 'Auth service unresponsive',       2500, '2024-06-01 11:05:00'),
    (93,  'data-pipeline','us-east', 'critical', 'Pipeline data loss detected',     3000, '2024-06-01 11:10:00'),
    (94,  'web-server',   'us-east', 'critical', 'All backends down',               5000, '2024-06-01 11:15:00'),
    (95,  'database',     'us-east', 'critical', 'Database failover triggered',     1500, '2024-06-01 11:20:00'),
    (96,  'cache-layer',  'us-east', 'critical', 'Cache cluster partitioned',       2200, '2024-06-01 11:25:00'),
    (97,  'api-gateway',  'us-east', 'critical', 'Circuit breaker open',            1000, '2024-06-01 11:30:00'),
    (98,  'auth-service', 'us-east', 'critical', 'Credential store unavailable',    2800, '2024-06-01 11:35:00'),
    (99,  'data-pipeline','us-east', 'critical', 'Sink write failures',             1600, '2024-06-01 11:40:00'),
    (100, 'web-server',   'us-east', 'critical', 'Load balancer failover',          900,  '2024-06-01 11:45:00')
) AS t(id, service, region, severity, message, latency_ms, event_time);


-- ============================================================================
-- EXPLORE: Critical Incident Wave in us-east
-- ============================================================================
-- Verify the 10 new critical events landed in the us-east partition:

ASSERT ROW_COUNT = 10
SELECT id, service, severity, latency_ms, message
FROM {{zone_name}}.delta_demos.cloud_events
WHERE id BETWEEN 91 AND 100
ORDER BY id;


-- ============================================================================
-- STEP 4: OPTIMIZE — Compact files and merge deletion vectors
-- ============================================================================
-- OPTIMIZE does two things per partition:
--   1. Merges small data files into larger, optimally-sized files
--   2. Applies pending deletion vectors by physically removing deleted
--      rows from the compacted files
--
-- After OPTIMIZE, deletion vector sidecar files are no longer needed
-- because deleted rows have been physically excluded. This improves
-- read performance since readers no longer need to apply DV filters.

OPTIMIZE {{zone_name}}.delta_demos.cloud_events;


-- ============================================================================
-- LEARN: Post-OPTIMIZE — Service-Level Summary
-- ============================================================================
-- After compaction, let's look at the final state of the table grouped
-- by service. The data is now stored in fewer, larger files with no
-- pending deletion vectors.

ASSERT ROW_COUNT = 6
ASSERT VALUE avg_latency_ms = 586 WHERE service = 'data-pipeline'
ASSERT VALUE avg_latency_ms = 409 WHERE service = 'auth-service'
ASSERT VALUE max_latency_ms = 5000 WHERE service = 'web-server'
-- Non-deterministic: cache-layer AVG(latency_ms) = 638.5; ROUND(.5) yields 638 (banker's) or 639 (half-up) depending on SQL engine
ASSERT WARNING VALUE avg_latency_ms BETWEEN 638 AND 639 WHERE service = 'cache-layer'
SELECT service,
       COUNT(*) AS events,
       ROUND(AVG(latency_ms), 0) AS avg_latency_ms,
       MAX(latency_ms) AS max_latency_ms,
       COUNT(DISTINCT region) AS regions
FROM {{zone_name}}.delta_demos.cloud_events
GROUP BY service
ORDER BY avg_latency_ms DESC;


-- ============================================================================
-- EXPLORE: Final Distribution Across Partitions
-- ============================================================================
-- us-east: 30 original - 5 deleted + 10 critical = 35
-- us-west: 30 original - 5 deleted = 25
-- eu-west: 30 original - 5 deleted = 25

ASSERT ROW_COUNT = 3
ASSERT VALUE event_count = 35 WHERE region = 'us-east'
ASSERT VALUE critical = 13 WHERE region = 'us-east'
ASSERT VALUE critical = 4 WHERE region = 'us-west'
ASSERT VALUE critical = 4 WHERE region = 'eu-west'
SELECT region,
       COUNT(*) AS event_count,
       COUNT(DISTINCT service) AS services,
       COUNT(*) FILTER (WHERE severity = 'critical') AS critical,
       COUNT(*) FILTER (WHERE severity = 'error') AS errors,
       COUNT(*) FILTER (WHERE severity = 'warning') AS warnings,
       COUNT(*) FILTER (WHERE severity = 'info') AS info
FROM {{zone_name}}.delta_demos.cloud_events
GROUP BY region
ORDER BY region;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 90 - 15 deleted + 10 inserted = 85
ASSERT ROW_COUNT = 85
SELECT * FROM {{zone_name}}.delta_demos.cloud_events;

-- Verify us_east_count: 30 - 5 deleted + 10 critical = 35
ASSERT VALUE cnt = 35
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.cloud_events WHERE region = 'us-east';

-- Verify us_west_count: 30 - 5 deleted = 25
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.cloud_events WHERE region = 'us-west';

-- Verify eu_west_count: 30 - 5 deleted = 25
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.cloud_events WHERE region = 'eu-west';

-- Verify deleted_events_gone: 15 low-severity info events removed via DVs
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.cloud_events WHERE id IN (2, 11, 18, 25, 29, 32, 42, 43, 54, 59, 68, 70, 78, 84, 89);

-- Verify new_critical_count: 10 critical incident events inserted
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.cloud_events WHERE id BETWEEN 91 AND 100;

-- Verify escalated_severity: id=5 error escalated to critical (latency > 500ms)
ASSERT VALUE severity = 'critical'
SELECT severity FROM {{zone_name}}.delta_demos.cloud_events WHERE id = 5;

-- Verify region_count: 3 distinct regions
ASSERT VALUE cnt = 3
SELECT COUNT(DISTINCT region) AS cnt FROM {{zone_name}}.delta_demos.cloud_events;
