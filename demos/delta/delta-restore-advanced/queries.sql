-- ============================================================================
-- Delta RESTORE — Advanced Recovery Scenarios — Educational Queries
-- ============================================================================
-- WHAT: RESTORE TO VERSION rolls an entire Delta table back to a previous
--       transaction log version, undoing all intermediate commits.
-- WHY:  When a bad deployment, accidental bulk delete, or corrupt data
--       load reaches production, RESTORE provides instant recovery without
--       needing traditional backup/restore infrastructure.
-- HOW:  RESTORE does not delete data files. It writes a new commit to the
--       transaction log that re-adds the data files from the target version
--       and removes the files added by later versions. The old files are
--       still on disk (for VACUUM retention), making RESTORE very fast.
-- ============================================================================
--
-- Version history we will build:
--   V0: CREATE table                         (setup → delta version 0)
--   V1: INSERT 35 config settings            (setup → delta version 1)
--   V2: UPDATE 10 database settings          (queries → delta version 2)
--   V3: INSERT 10 + UPDATE 5 — deployment    (queries → delta versions 3-6)
--   V4: DELETE is_active = 0 — cleanup       (queries → delta version 7)
--   V5: INSPECT via VERSION AS OF 2          (read-only, no version change)
--   V6: RESTORE TO VERSION 2                 (queries → delta version 8)
--   V7: UPDATE 3 — mark restored_admin       (queries → delta version 9)
-- ============================================================================


-- ============================================================================
-- Baseline: Observe V0 State
-- ============================================================================
-- Setup created the table with 35 config settings across 5 categories.
-- Let's see the starting point before we begin making changes:

ASSERT ROW_COUNT = 5
SELECT category,
       COUNT(*) AS setting_count,
       COUNT(*) FILTER (WHERE is_active = 1) AS active,
       COUNT(*) FILTER (WHERE is_active = 0) AS inactive
FROM {{zone_name}}.delta_demos.config_settings
GROUP BY category
ORDER BY category;


-- ============================================================================
-- V1: UPDATE 10 database settings — new connection pool and timeout values
-- ============================================================================
-- Tuning database settings after load testing revealed bottlenecks.
-- This changes pool sizes, timeouts, host, port, db name, and legacy settings.

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.config_settings
SET value = CASE key
        WHEN 'db.pool.min'        THEN '10'
        WHEN 'db.pool.max'        THEN '50'
        WHEN 'db.timeout.connect' THEN '3000'
        WHEN 'db.timeout.read'    THEN '15000'
        WHEN 'db.ssl.enabled'     THEN 'strict'
        WHEN 'db.host'            THEN 'prod-db-cluster.internal'
        WHEN 'db.port'            THEN '5433'
        WHEN 'db.name'            THEN 'app_production_v2'
        WHEN 'db.legacy.driver'   THEN 'jdbc-v2'
        WHEN 'db.legacy.pool'     THEN '5'
    END,
    updated_by = 'dba_team',
    updated_at = '2025-02-01'
WHERE category = 'database';

-- Observe V1: Verify database settings were tuned
ASSERT ROW_COUNT = 10
SELECT id, key, value, updated_by, updated_at
FROM {{zone_name}}.delta_demos.config_settings
WHERE category = 'database'
ORDER BY id;


-- ============================================================================
-- V2: INSERT 10 new settings + UPDATE 5 existing — config deployment
-- ============================================================================
-- A deployment adds new monitoring/feature settings and deactivates some old ones.

ASSERT ROW_COUNT = 10
INSERT INTO {{zone_name}}.delta_demos.config_settings VALUES
    (36, 'monitor.enabled',       'true',             'logging',  1, 'deploy_bot', '2025-03-01'),
    (37, 'monitor.interval',      '60',               'logging',  1, 'deploy_bot', '2025-03-01'),
    (38, 'monitor.alerting',      'pagerduty',        'logging',  1, 'deploy_bot', '2025-03-01'),
    (39, 'feature.darkmode',      'true',             'api',      1, 'deploy_bot', '2025-03-01'),
    (40, 'feature.beta.signup',   'true',             'api',      1, 'deploy_bot', '2025-03-01'),
    (41, 'cache.cluster.enabled', 'true',             'cache',    1, 'deploy_bot', '2025-03-01'),
    (42, 'cache.cluster.nodes',   '3',                'cache',    1, 'deploy_bot', '2025-03-01'),
    (43, 'auth.sso.enabled',      'true',             'auth',     1, 'deploy_bot', '2025-03-01'),
    (44, 'api.graphql.enabled',   'true',             'api',      1, 'deploy_bot', '2025-03-01'),
    (45, 'db.replica.host',       'replica-01.internal', 'database', 1, 'deploy_bot', '2025-03-01');

-- Deactivate 3 old settings and change 2 values as part of deployment
ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.config_settings
SET is_active = 0,
    updated_by = 'deploy_bot',
    updated_at = '2025-03-01'
WHERE id IN (13, 14, 26);

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.config_settings
SET value = '2000',
    updated_by = 'deploy_bot',
    updated_at = '2025-03-01'
WHERE id = 31;

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.config_settings
SET value = '60000',
    updated_by = 'deploy_bot',
    updated_at = '2025-03-01'
WHERE id = 32;

-- Observe V2: Table now has 45 rows — 10 new settings added, 3 deactivated
ASSERT ROW_COUNT = 5
SELECT category,
       COUNT(*) AS setting_count,
       COUNT(*) FILTER (WHERE is_active = 1) AS active,
       COUNT(*) FILTER (WHERE is_active = 0) AS inactive
FROM {{zone_name}}.delta_demos.config_settings
GROUP BY category
ORDER BY category;


-- ============================================================================
-- V3: DELETE all is_active = 0 — dangerous bulk cleanup!
-- ============================================================================
-- Someone runs a cleanup script that removes ALL inactive settings.
-- This deletes 8 rows: ids 9, 10, 17, 23, 24 (original inactive)
--                       + ids 13, 14, 26 (deactivated in V2)

ASSERT ROW_COUNT = 8
DELETE FROM {{zone_name}}.delta_demos.config_settings
WHERE is_active = 0;

-- Observe V3: 8 inactive rows are gone — only 37 active rows remain
ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 37
ASSERT VALUE active = 37
ASSERT VALUE inactive = 0
SELECT COUNT(*) AS total_rows,
       COUNT(*) FILTER (WHERE is_active = 1) AS active,
       COUNT(*) FILTER (WHERE is_active = 0) AS inactive
FROM {{zone_name}}.delta_demos.config_settings;


-- ============================================================================
-- INSPECT: Use VERSION AS OF to verify the restore target
-- ============================================================================
-- Before committing to a RESTORE, inspect the target version to confirm
-- it contains the state you want. VERSION AS OF lets you query any
-- historical version without changing the table. This is the real-world
-- workflow: inspect → decide → restore.
--
-- Delta version 2 = the state after our V1 database tuning UPDATE.
-- (V0=CREATE, V1=INSERT in setup, V2=UPDATE in first query block above.)

-- Peek at version 2: should have 35 rows with original inactive settings intact
ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 35
ASSERT VALUE inactive = 5
SELECT COUNT(*) AS total_rows,
       COUNT(*) FILTER (WHERE is_active = 1) AS active,
       COUNT(*) FILTER (WHERE is_active = 0) AS inactive
FROM {{zone_name}}.delta_demos.config_settings VERSION AS OF 2;

-- Confirm V2 has the database tuning we want to preserve
ASSERT VALUE value = '50' WHERE key = 'db.pool.max'
ASSERT VALUE updated_by = 'dba_team' WHERE key = 'db.pool.max'
ASSERT ROW_COUNT = 10
SELECT key, value, updated_by
FROM {{zone_name}}.delta_demos.config_settings VERSION AS OF 2
WHERE category = 'database'
ORDER BY key;

-- Compare with current state (post-delete): only 37 rows, 0 inactive
ASSERT ROW_COUNT = 1
ASSERT VALUE current_rows = 37
ASSERT VALUE version2_rows = 35
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.config_settings) AS current_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.config_settings VERSION AS OF 2) AS version2_rows;


-- ============================================================================
-- V4: RESTORE TO VERSION 2 — undo the deployment and cleanup
-- ============================================================================
-- Now that we have confirmed version 2 is the correct target, execute RESTORE.
-- This undoes V2 inserts, V2 updates, and V3 deletes.
--
-- RESTORE does NOT delete any files or rewind the log.
-- Instead, it creates a NEW commit that:
--   1. Adds back the data files that existed at version 2
--   2. Removes the data files that were added in later versions
--
-- The transaction log grows forward — RESTORE is just another commit that
-- happens to reproduce an earlier state. You could even RESTORE the RESTORE.

RESTORE {{zone_name}}.delta_demos.config_settings TO VERSION 2;

-- Observe V4: Back to 35 rows — V2's inserts are gone, V3's deletes are undone
ASSERT ROW_COUNT = 5
SELECT category,
       COUNT(*) AS setting_count,
       COUNT(*) FILTER (WHERE is_active = 1) AS active,
       COUNT(*) FILTER (WHERE is_active = 0) AS inactive
FROM {{zone_name}}.delta_demos.config_settings
GROUP BY category
ORDER BY category;

-- Verify V2's inserted settings (ids 36-45) are gone:
ASSERT ROW_COUNT = 1
ASSERT VALUE v2_inserts_remaining = 0
SELECT COUNT(*) AS v2_inserts_remaining
FROM {{zone_name}}.delta_demos.config_settings
WHERE id >= 36;

-- Verify V1's database tuning is still intact (RESTORE preserves V1 changes):
ASSERT ROW_COUNT = 10
ASSERT VALUE updated_by = 'dba_team' WHERE key = 'db.pool.max'
ASSERT VALUE value = '50' WHERE key = 'db.pool.max'
SELECT id, key, value, updated_by
FROM {{zone_name}}.delta_demos.config_settings
WHERE category = 'database'
ORDER BY id;

-- Verify the deleted inactive rows were recovered:
ASSERT ROW_COUNT = 5
SELECT id, key, value, category, updated_by
FROM {{zone_name}}.delta_demos.config_settings
WHERE is_active = 0
ORDER BY category, id;


-- ============================================================================
-- V5: UPDATE — mark 3 settings as reviewed by restored_admin
-- ============================================================================
-- After restore, an admin reviews and marks key settings.
-- This proves that RESTORE does not freeze the table — it simply sets a
-- new baseline, and you can continue making changes on top of it.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.config_settings
SET updated_by = 'restored_admin',
    updated_at = '2025-03-15'
WHERE id IN (1, 18, 31);

-- Observe V5: Confirm the post-restore writes took effect
ASSERT ROW_COUNT = 3
SELECT id, key, value, category, updated_by, updated_at
FROM {{zone_name}}.delta_demos.config_settings
WHERE updated_by = 'restored_admin'
ORDER BY id;


-- ============================================================================
-- EXPLORE: Full Configuration Landscape
-- ============================================================================
-- All settings organized by category to confirm the complete restored state:

ASSERT ROW_COUNT = 35
SELECT id, key, value, category,
       CASE WHEN is_active = 1 THEN 'active' ELSE 'inactive' END AS status,
       updated_by
FROM {{zone_name}}.delta_demos.config_settings
ORDER BY category, key;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: restored to 35 original rows
ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.delta_demos.config_settings;

-- Verify v2_inserts_gone: V2 inserted rows (ids 36-45) undone by RESTORE
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.config_settings WHERE id >= 36;

-- Verify v1_updates_intact: V1 database tuning preserved (db.pool.max = 50)
ASSERT VALUE value = '50'
SELECT value FROM {{zone_name}}.delta_demos.config_settings WHERE id = 5;

-- Verify deleted_rows_recovered: 5 inactive rows recovered after RESTORE
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.config_settings WHERE is_active = 0;

-- Verify restored_admin_count: 3 settings marked by restored_admin in V5
ASSERT VALUE cnt = 3
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.config_settings WHERE updated_by = 'restored_admin';

-- Verify category_count: all 5 categories present
ASSERT VALUE cnt = 5
SELECT COUNT(DISTINCT category) AS cnt FROM {{zone_name}}.delta_demos.config_settings;

-- Verify all_original_ids: all 35 original ids (1-35) present
ASSERT VALUE cnt = 35
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.config_settings WHERE id BETWEEN 1 AND 35;

-- Verify auth_settings_intact: 7 auth settings unchanged
ASSERT VALUE cnt = 7
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.config_settings WHERE category = 'auth';
