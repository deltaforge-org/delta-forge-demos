-- ============================================================================
-- Delta Column Default Values — Educational Queries
-- ============================================================================
-- WHAT: Column defaults automatically populate missing fields during INSERT,
--       using COALESCE(column, default_value) in CTEs when CREATE TABLE
--       does not support the DEFAULT keyword.
-- WHY:  NULLs propagating into analytics pipelines cause errors in COUNT,
--       SUM, and JOIN operations. Defaults ensure every row has a meaningful
--       value for every column, making downstream queries predictable.
-- HOW:  A CTE wraps the raw VALUES with COALESCE expressions that replace
--       NULLs before the data reaches the Delta table. The transaction log
--       stores only the final non-NULL values — no record of the original
--       NULLs is kept in the data files.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Inspect the Baseline Data
-- ============================================================================
-- The setup script inserted 25 rows with all columns explicitly provided.
-- Let's inspect a sample before adding rows that use defaults.

ASSERT ROW_COUNT = 10
SELECT id, action, user_name, severity, retry_count, notes
FROM {{zone_name}}.delta_demos.audit_log
ORDER BY id
LIMIT 10;


-- ============================================================================
-- LEARN: What Happens Without Defaults — The Anti-Pattern
-- ============================================================================
-- An application pushes 5 audit entries without applying defaults.
-- Some rows have user_name, severity, retry_count, or notes missing.
-- Without COALESCE, NULLs flow directly into the Delta table.

ASSERT ROW_COUNT = 5
INSERT INTO {{zone_name}}.delta_demos.audit_log
SELECT * FROM (VALUES
    (100, 'test.ping',   NULL,    NULL,      NULL, 0, '2024-01-10 01:00:00', NULL),
    (101, 'test.health', 'admin', NULL,      NULL, 0, '2024-01-10 02:00:00', NULL),
    (102, 'test.metric', NULL,    'error',   NULL, 0, '2024-01-10 03:00:00', NULL),
    (103, 'test.alert',  NULL,    NULL,      NULL, 0, '2024-01-10 04:00:00', NULL),
    (104, 'test.sync',   'jdoe',  'warning', NULL, 0, '2024-01-10 05:00:00', NULL)
) AS t(id, action, user_name, severity, retry_count, is_archived, created_at, notes);


-- ============================================================================
-- EXPLORE: NULL Contamination — Silent Data Corruption
-- ============================================================================
-- NULLs are invisible poison in analytics pipelines. COUNT(*) says 30 rows,
-- but COUNT(column) silently gives different numbers for each column.
-- Downstream dashboards and reports produce wrong numbers without any error.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 30
ASSERT VALUE users_counted = 27
ASSERT VALUE severities_counted = 27
ASSERT VALUE retries_counted = 25
SELECT
    COUNT(*)           AS total_rows,
    COUNT(user_name)   AS users_counted,
    COUNT(severity)    AS severities_counted,
    COUNT(retry_count) AS retries_counted
FROM {{zone_name}}.delta_demos.audit_log;

-- GROUP BY creates a phantom NULL bucket that breaks category reports.
-- We had 4 severity levels; now there are 5 groups — the 5th is NULL.
ASSERT ROW_COUNT = 5
SELECT severity,
       COUNT(*) AS entry_count
FROM {{zone_name}}.delta_demos.audit_log
GROUP BY severity
ORDER BY entry_count DESC;


-- ============================================================================
-- LEARN: Cleaning Up NULL Contamination
-- ============================================================================
-- The only fix is to DELETE the corrupted rows and re-insert with defaults.
-- In production this means downtime, reprocessing, and data reconciliation.
-- Prevention (applying defaults at insert time) is far cheaper than cure.

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.audit_log WHERE id >= 100;

-- Confirm baseline restored — 25 clean rows remain
ASSERT VALUE clean_count = 25
SELECT COUNT(*) AS clean_count FROM {{zone_name}}.delta_demos.audit_log;


-- ============================================================================
-- LEARN: The COALESCE Default Pattern — Batch 2 (ids 26-35)
-- ============================================================================
-- Now let's do it right. A CTE wraps raw VALUES with COALESCE so NULLs
-- are replaced by defaults BEFORE the INSERT reaches the table:
--
--   COALESCE(user_name, 'system')  AS user_name
--   COALESCE(severity, 'info')     AS severity
--   COALESCE(retry_count, 0)       AS retry_count
--   COALESCE(notes, 'N/A')         AS notes
--
-- NULL columns replaced by defaults in this batch:
--   user_name   NULL -> 'system'  (ids 26,28,30,32,34 -> 5 rows)
--   severity    NULL -> 'info'    (ids 26,27,29,31,33 -> 5 rows)
--   retry_count NULL -> 0         (all 10 rows)
--   notes       NULL -> 'N/A'     (all 10 rows)
-- ============================================================================
ASSERT ROW_COUNT = 10
INSERT INTO {{zone_name}}.delta_demos.audit_log
WITH raw_entries AS (
    SELECT * FROM (VALUES
        (26, 'auto.sync',       NULL,     NULL,      NULL, 0, '2024-01-06 01:00:00', NULL),
        (27, 'user.login',      'jdoe',   NULL,      NULL, 0, '2024-01-06 08:00:00', NULL),
        (28, 'cron.cleanup',    NULL,     'warning',  NULL, 0, '2024-01-06 02:00:00', NULL),
        (29, 'cache.refresh',   'admin',  NULL,       NULL, 0, '2024-01-06 03:00:00', NULL),
        (30, 'log.rotate',      NULL,     'error',    NULL, 0, '2024-01-06 04:00:00', NULL),
        (31, 'metric.collect',  'msmith', NULL,       NULL, 0, '2024-01-07 01:00:00', NULL),
        (32, 'auto.backup',     NULL,     'critical', NULL, 0, '2024-01-07 02:00:00', NULL),
        (33, 'index.rebuild',   'analyst',NULL,       NULL, 0, '2024-01-07 03:00:00', NULL),
        (34, 'health.ping',     NULL,     'warning',  NULL, 0, '2024-01-07 04:00:00', NULL),
        (35, 'queue.drain',     'jdoe',   'error',    NULL, 0, '2024-01-07 05:00:00', NULL)
    ) AS t(id, action, user_name, severity, retry_count, is_archived, created_at, notes)
),
with_defaults AS (
    SELECT
        id,
        action,
        COALESCE(user_name, 'system')  AS user_name,
        COALESCE(severity, 'info')     AS severity,
        COALESCE(retry_count, 0)       AS retry_count,
        is_archived,
        created_at,
        COALESCE(notes, 'N/A')         AS notes
    FROM raw_entries
)
SELECT * FROM with_defaults;


-- ============================================================================
-- EXPLORE: Confirm Defaults Applied in Batch 2
-- ============================================================================
-- Let's examine batch 2 entries and confirm no NULLs leaked through.
-- The user_source and notes_source columns show whether each value was
-- defaulted or explicitly provided.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 10
SELECT id, action, user_name, severity, retry_count, notes,
       CASE WHEN user_name = 'system' THEN 'defaulted' ELSE 'explicit' END AS user_source,
       CASE WHEN notes = 'N/A' THEN 'defaulted' ELSE 'explicit' END AS notes_source
FROM {{zone_name}}.delta_demos.audit_log
WHERE id BETWEEN 26 AND 35
ORDER BY id;

-- Verify: no NULLs leaked through COALESCE in batch 2
ASSERT VALUE null_leak_count = 0
SELECT COUNT(*) AS null_leak_count
FROM {{zone_name}}.delta_demos.audit_log
WHERE id BETWEEN 26 AND 35
  AND (user_name IS NULL OR severity IS NULL OR retry_count IS NULL OR notes IS NULL);


-- ============================================================================
-- LEARN: Mixed Explicit and Default Values — Batch 3 (ids 36-45)
-- ============================================================================
-- This batch mixes rows where some columns are explicit and others rely on
-- COALESCE defaults. This is the most realistic pattern — some rows arrive
-- with full data, others have gaps filled by defaults.
--
-- NULL columns replaced by defaults in this batch:
--   user_name   NULL -> 'system' (ids 36,38,40,42,44 -> 5 rows)
--   severity    NULL -> 'info'   (ids 36,38,40 -> 3 rows)
--   retry_count NULL -> 0        (ids 36,37,38,39,40 -> 5 rows)
--   notes       NULL -> 'N/A'    (ids 36,38,40,42,44 -> 5 rows)
-- ============================================================================
ASSERT ROW_COUNT = 10
INSERT INTO {{zone_name}}.delta_demos.audit_log
WITH raw_entries AS (
    SELECT * FROM (VALUES
        (36, 'auto.deploy',     NULL,     NULL,      NULL, 0, '2024-01-08 01:00:00', NULL),
        (37, 'user.login',      'admin',  'warning', NULL, 0, '2024-01-08 08:00:00', 'Unusual login time'),
        (38, 'service.restart', NULL,     NULL,      NULL, 0, '2024-01-08 02:00:00', NULL),
        (39, 'data.sync',       'jdoe',   'warning', NULL, 0, '2024-01-08 09:00:00', 'Sync delay detected'),
        (40, 'cert.renew',      NULL,     NULL,      NULL, 0, '2024-01-08 03:00:00', NULL),
        (41, 'api.throttle',    'msmith', 'error',   1,   0, '2024-01-09 08:00:00', 'Rate limit exceeded'),
        (42, 'disk.alert',      NULL,     'critical',2,   0, '2024-01-09 02:00:00', NULL),
        (43, 'user.logout',     'analyst','error',   1,   0, '2024-01-09 17:00:00', 'Forced logout timeout'),
        (44, 'memory.warning',  NULL,     'critical',3,   0, '2024-01-09 03:00:00', NULL),
        (45, 'report.export',   'jdoe',   'error',   1,   0, '2024-01-09 10:00:00', 'Export format error')
    ) AS t(id, action, user_name, severity, retry_count, is_archived, created_at, notes)
),
with_defaults AS (
    SELECT
        id,
        action,
        COALESCE(user_name, 'system')  AS user_name,
        COALESCE(severity, 'info')     AS severity,
        COALESCE(retry_count, 0)       AS retry_count,
        is_archived,
        created_at,
        COALESCE(notes, 'N/A')         AS notes
    FROM raw_entries
)
SELECT * FROM with_defaults;


-- ============================================================================
-- LEARN: Default Value Distribution Across All Batches
-- ============================================================================
-- Three batches were inserted with different mixes of explicit and default
-- values. This query shows how many rows used the default vs. an explicit
-- value for each defaultable column — demonstrating that the COALESCE
-- pattern works consistently across batches.

ASSERT ROW_COUNT = 4
SELECT
    'user_name' AS column_name,
    SUM(CASE WHEN user_name = 'system' THEN 1 ELSE 0 END) AS default_count,
    SUM(CASE WHEN user_name != 'system' THEN 1 ELSE 0 END) AS explicit_count
FROM {{zone_name}}.delta_demos.audit_log
UNION ALL
SELECT
    'severity',
    SUM(CASE WHEN severity = 'info' THEN 1 ELSE 0 END),
    SUM(CASE WHEN severity != 'info' THEN 1 ELSE 0 END)
FROM {{zone_name}}.delta_demos.audit_log
UNION ALL
SELECT
    'retry_count',
    SUM(CASE WHEN retry_count = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN retry_count != 0 THEN 1 ELSE 0 END)
FROM {{zone_name}}.delta_demos.audit_log
UNION ALL
SELECT
    'notes',
    SUM(CASE WHEN notes = 'N/A' THEN 1 ELSE 0 END),
    SUM(CASE WHEN notes != 'N/A' THEN 1 ELSE 0 END)
FROM {{zone_name}}.delta_demos.audit_log;


-- ============================================================================
-- LEARN: UPDATE — Archive Old Entries (id <= 10)
-- ============================================================================
-- The is_archived column defaults to 0 at insert time. This UPDATE
-- selectively marks the 10 oldest entries as archived, demonstrating
-- how defaults and UPDATEs work together: rows start with the default
-- value and are later modified based on business logic.

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.audit_log
SET is_archived = 1
WHERE id <= 10;


-- ============================================================================
-- EXPLORE: Archived vs. Active Entries
-- ============================================================================
-- After the UPDATE, let's confirm the archive status. Only ids 1-10 should
-- be archived; the remaining 35 entries stay active with the default value.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 2
ASSERT VALUE entry_count = 10 WHERE status = 'Archived'
ASSERT VALUE entry_count = 35 WHERE status = 'Active'
SELECT
    CASE WHEN is_archived = 1 THEN 'Archived' ELSE 'Active' END AS status,
    COUNT(*) AS entry_count,
    MIN(id) AS min_id,
    MAX(id) AS max_id
FROM {{zone_name}}.delta_demos.audit_log
GROUP BY is_archived
ORDER BY is_archived;


-- ============================================================================
-- EXPLORE: Severity Breakdown
-- ============================================================================
-- The 'info' severity is both the default and the most common explicit value.
-- Higher severities (warning, error, critical) were always set explicitly,
-- never defaulted — showing that defaults capture the "normal" case while
-- exceptional cases are intentionally specified.

ASSERT ROW_COUNT = 4
SELECT severity,
       COUNT(*) AS entry_count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM {{zone_name}}.delta_demos.audit_log
GROUP BY severity
ORDER BY entry_count DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count
ASSERT ROW_COUNT = 45
SELECT * FROM {{zone_name}}.delta_demos.audit_log;

-- Verify system user count (defaulted user_name)
ASSERT VALUE system_user_count = 15
SELECT COUNT(*) AS system_user_count FROM {{zone_name}}.delta_demos.audit_log WHERE user_name = 'system';

-- Verify info severity count (defaulted severity)
ASSERT VALUE info_severity_count = 18
SELECT COUNT(*) AS info_severity_count FROM {{zone_name}}.delta_demos.audit_log WHERE severity = 'info';

-- Verify zero retry count (defaulted retry_count)
ASSERT VALUE zero_retry_count = 26
SELECT COUNT(*) AS zero_retry_count FROM {{zone_name}}.delta_demos.audit_log WHERE retry_count = 0;

-- Verify archived count
ASSERT VALUE archived_count = 10
SELECT COUNT(*) AS archived_count FROM {{zone_name}}.delta_demos.audit_log WHERE is_archived = 1;

-- Verify explicit user count
ASSERT VALUE explicit_user_count = 30
SELECT COUNT(*) AS explicit_user_count FROM {{zone_name}}.delta_demos.audit_log WHERE user_name != 'system';

-- Verify warning severity count
ASSERT VALUE warning_count = 9
SELECT COUNT(*) AS warning_count FROM {{zone_name}}.delta_demos.audit_log WHERE severity = 'warning';

-- Verify notes default count
ASSERT VALUE notes_default_count = 20
SELECT COUNT(*) AS notes_default_count FROM {{zone_name}}.delta_demos.audit_log WHERE notes = 'N/A';

-- Verify no NULLs in any defaultable column (the COALESCE guarantee)
ASSERT VALUE null_count = 0
SELECT COUNT(*) AS null_count FROM {{zone_name}}.delta_demos.audit_log
WHERE user_name IS NULL OR severity IS NULL OR retry_count IS NULL OR notes IS NULL;
