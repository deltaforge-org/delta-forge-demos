-- ============================================================================
-- Delta Time Travel — VACUUM Retention Boundary — Educational Queries
-- ============================================================================
-- WHAT: VACUUM removes old Parquet files to reclaim storage. But those files
--       are exactly what time travel reads. After VACUUM, old VERSION AS OF
--       queries fail silently — the delta log still lists the versions, but
--       the files are gone.
-- WHY:  This is the #1 production gotcha with Delta Lake. Teams run VACUUM
--       for storage savings, then discover weeks later that their audit
--       queries are broken. Understanding the retention boundary is critical.
-- HOW:  We verify time travel works, run VACUUM RETAIN 0 HOURS (aggressive),
--       then confirm only the latest version survives.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Current State (Version 4 — Latest)
-- ============================================================================
-- The activity_log has been through 5 versions. Let's see the current state.

ASSERT VALUE cnt = 7 WHERE action = 'click'
ASSERT VALUE cnt = 6 WHERE action = 'purchase'
ASSERT VALUE cnt = 5 WHERE action = 'view'
ASSERT ROW_COUNT = 3
SELECT action,
       COUNT(*) AS cnt,
       ROUND(AVG(duration_secs), 1) AS avg_duration
FROM {{zone_name}}.delta_demos.activity_log
GROUP BY action
ORDER BY action;


-- ============================================================================
-- LEARN: Pre-VACUUM — Time Travel Works
-- ============================================================================
-- Before VACUUM, every historical version is queryable. Let's prove it by
-- checking V1 (the original 15 records before any mutations).

ASSERT VALUE v1_count = 15
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS v1_count
FROM {{zone_name}}.delta_demos.activity_log VERSION AS OF 1;


-- ============================================================================
-- LEARN: Pre-VACUUM — Full Version Progression
-- ============================================================================
-- Each version tells a story. V1→V2: durations shifted. V2→V3: bounces
-- removed. V3→V4: new users joined.

ASSERT VALUE rows = 15 WHERE version = 'V1'
ASSERT VALUE rows = 15 WHERE version = 'V2'
ASSERT VALUE rows = 13 WHERE version = 'V3'
ASSERT VALUE rows = 18 WHERE version = 'V4'
ASSERT ROW_COUNT = 4
SELECT 'V1' AS version,
       (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.activity_log VERSION AS OF 1) AS rows
UNION ALL
SELECT 'V2',
       (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.activity_log VERSION AS OF 2)
UNION ALL
SELECT 'V3',
       (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.activity_log VERSION AS OF 3)
UNION ALL
SELECT 'V4',
       (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.activity_log VERSION AS OF 4);


-- ============================================================================
-- ACTION: VACUUM RETAIN 0 HOURS — The Destructive Operation
-- ============================================================================
-- VACUUM RETAIN 0 HOURS removes ALL Parquet files not referenced by the
-- latest version. This is the most aggressive setting — in production,
-- the default is 168 hours (7 days). After this command:
--   - Files for V0, V1, V2, V3 are deleted from disk
--   - Only V4's Parquet files survive
--   - The _delta_log/ JSON entries for all versions still exist (metadata)
--   - VERSION AS OF 1..3 will fail with file-not-found errors

VACUUM {{zone_name}}.delta_demos.activity_log RETAIN 0 HOURS;


-- ============================================================================
-- LEARN: Post-VACUUM — Current Data Survives
-- ============================================================================
-- The latest version (V4) is always preserved by VACUUM. Current queries
-- work exactly as before — no data loss for the active snapshot.

ASSERT VALUE current_count = 18
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS current_count
FROM {{zone_name}}.delta_demos.activity_log;


-- ============================================================================
-- LEARN: Post-VACUUM — Latest Version Still Queryable
-- ============================================================================
-- VERSION AS OF 4 works because its files weren't vacuumed. The latest
-- version is always safe.

ASSERT VALUE v4_count = 18
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS v4_count
FROM {{zone_name}}.delta_demos.activity_log VERSION AS OF 4;


-- ============================================================================
-- LEARN: Post-VACUUM — DESCRIBE HISTORY Shows Ghost Entries
-- ============================================================================
-- The delta log metadata still records all 5 versions (0-4). DESCRIBE
-- HISTORY shows them all — but the Parquet files for V0-V3 no longer
-- exist on disk. These are "ghost entries": the log says the version
-- happened, but you can't read its data.
--
-- If you tried: SELECT * FROM activity_log VERSION AS OF 1
-- You would get: ERROR — referenced Parquet file not found
--
-- This is the VACUUM retention boundary: the log remembers everything,
-- but the data only exists for versions within the retention window.

-- Non-deterministic: DESCRIBE HISTORY may include extra internal versions
ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.delta_demos.activity_log;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify current data is intact (18 rows)
ASSERT ROW_COUNT = 18
SELECT * FROM {{zone_name}}.delta_demos.activity_log;

-- Verify 3 actions exist
ASSERT VALUE action_count = 3
SELECT COUNT(DISTINCT action) AS action_count FROM {{zone_name}}.delta_demos.activity_log;

-- Verify 8 distinct users
ASSERT VALUE user_count = 8
SELECT COUNT(DISTINCT user_id) AS user_count FROM {{zone_name}}.delta_demos.activity_log;

-- Verify latest version is queryable
ASSERT VALUE v4_rows = 18
SELECT COUNT(*) AS v4_rows FROM {{zone_name}}.delta_demos.activity_log VERSION AS OF 4;
