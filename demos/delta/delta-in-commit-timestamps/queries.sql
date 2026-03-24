-- ============================================================================
-- Delta In-Commit Timestamps — Educational Queries
-- ============================================================================
-- WHAT: In-commit timestamps embed a monotonically increasing timestamp
--       directly into each Delta transaction log commit entry.
-- WHY:  Without this feature, Delta derives version timestamps from file
--       modification times, which can be unreliable when multiple writers
--       have clock skew. This breaks TIMESTAMP AS OF time travel queries.
-- HOW:  When 'delta.enableInCommitTimestamps' = 'true', each commit action
--       in the _delta_log JSON file includes a reliable timestamp field.
--       This guarantees monotonicity regardless of writer clock differences.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Verify In-Commit Timestamps Are Enabled
-- ============================================================================
-- The first step is confirming the table property is active. SHOW TBLPROPERTIES
-- reads the Delta metadata configuration. The key property is
-- delta.enableInCommitTimestamps — when 'true', every commit written to
-- the _delta_log includes a reliable, monotonically increasing timestamp.

ASSERT VALUE value = 'true' WHERE key = 'delta.enableInCommitTimestamps'
SHOW TBLPROPERTIES {{zone_name}}.delta_demos.release_tracker;


-- ============================================================================
-- LEARN: Inspect the Version Timeline with DESCRIBE HISTORY
-- ============================================================================
-- DESCRIBE HISTORY reveals the full commit history of the table. Each row
-- is a version with its commit timestamp, operation type, and metrics.
-- Because in-commit timestamps are enabled, these timestamps are reliable
-- and monotonically increasing — critical for TIMESTAMP AS OF queries.
--
-- The setup created 6 versions:
--   V0: CREATE TABLE (schema definition, ICT property set)
--   V1: INSERT 15 production releases
--   V2: INSERT 10 staging releases
--   V3: INSERT 5 development releases
--   V4: UPDATE 3 releases → rolled_back
--   V5: UPDATE 2 releases → failed

ASSERT ROW_COUNT = 6
DESCRIBE HISTORY {{zone_name}}.delta_demos.release_tracker;


-- ============================================================================
-- EXPLORE: Time Travel to Production-Only Snapshot (Version 1)
-- ============================================================================
-- VERSION AS OF lets us query the table at any historical version.
-- At V1, only the first INSERT batch (15 production releases) existed.
-- The reliability of this query depends on accurate commit timestamps —
-- if timestamps were derived from unreliable file modification times,
-- version resolution could return wrong results.

ASSERT ROW_COUNT = 15
ASSERT VALUE app_count = 5
SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT app_name) AS app_count,
       COUNT(DISTINCT environment) AS env_count
FROM {{zone_name}}.delta_demos.release_tracker VERSION AS OF 1;


-- ============================================================================
-- LEARN: Pre-Update State — All Releases Successful (Version 3)
-- ============================================================================
-- At V3, all 30 releases had been inserted but no status updates applied.
-- Every release was still 'success'. This is the last "clean" version
-- before rollbacks and failures were recorded in V4 and V5.

ASSERT ROW_COUNT = 3
ASSERT VALUE release_count = 15 WHERE environment = 'production'
ASSERT VALUE release_count = 10 WHERE environment = 'staging'
ASSERT VALUE release_count = 5 WHERE environment = 'development'
SELECT environment,
       COUNT(*) AS release_count,
       COUNT(*) FILTER (WHERE status = 'success') AS all_success
FROM {{zone_name}}.delta_demos.release_tracker VERSION AS OF 3
GROUP BY environment
ORDER BY environment;


-- ============================================================================
-- EXPLORE: Before vs After — Affected Releases Across Versions
-- ============================================================================
-- These 5 releases (ids 3, 9, 12, 20, 22) were updated in V4 and V5.
-- At V3 they were all 'success'. Now they show rolled_back or failed.
-- With in-commit timestamps, you can pinpoint exactly when each status
-- change was committed — not when the deployer claimed it happened
-- (deployed_at), but when the Delta commit was actually written.

-- V3: All 5 affected releases were still successful
ASSERT ROW_COUNT = 5
ASSERT VALUE status = 'success' WHERE id = 3
ASSERT VALUE status = 'success' WHERE id = 12
SELECT id, app_name, environment, status
FROM {{zone_name}}.delta_demos.release_tracker VERSION AS OF 3
WHERE id IN (3, 9, 12, 20, 22)
ORDER BY id;

-- Current: Same releases now reflect their true outcomes
ASSERT ROW_COUNT = 5
ASSERT VALUE status = 'rolled_back' WHERE id = 3
ASSERT VALUE status = 'failed' WHERE id = 12
SELECT id, app_name, environment, status
FROM {{zone_name}}.delta_demos.release_tracker
WHERE id IN (3, 9, 12, 20, 22)
ORDER BY id;


-- ============================================================================
-- LEARN: Current State — Deployment Outcomes Summary
-- ============================================================================
-- The final state (V5) shows the complete picture after all updates.
-- 25 successful, 3 rolled back, 2 failed across all environments.

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 2 WHERE status = 'failed'
ASSERT VALUE cnt = 3 WHERE status = 'rolled_back'
ASSERT VALUE cnt = 25 WHERE status = 'success'
SELECT status,
       COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.release_tracker
GROUP BY status
ORDER BY status;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 30 releases across all versions
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.release_tracker;

-- Verify production_count: 15 production releases
ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.release_tracker WHERE environment = 'production';

-- Verify staging_count: 10 staging releases
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.release_tracker WHERE environment = 'staging';

-- Verify development_count: 5 development releases
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.release_tracker WHERE environment = 'development';

-- Verify rolled_back_count: 3 rolled back (ids 3, 9, 22)
ASSERT VALUE cnt = 3
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.release_tracker WHERE status = 'rolled_back';

-- Verify failed_count: 2 failed (ids 12, 20)
ASSERT VALUE cnt = 2
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.release_tracker WHERE status = 'failed';

-- Verify success_count: 25 successful
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.release_tracker WHERE status = 'success';

-- Verify distinct_apps: 5 different applications
ASSERT VALUE cnt = 5
SELECT COUNT(DISTINCT app_name) AS cnt FROM {{zone_name}}.delta_demos.release_tracker;

-- Verify version_count: 6 versions in history (V0-V5)
ASSERT ROW_COUNT = 6
DESCRIBE HISTORY {{zone_name}}.delta_demos.release_tracker;
