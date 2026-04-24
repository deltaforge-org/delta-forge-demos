-- ============================================================================
-- Delta Column Default Values — Setup Script
-- ============================================================================
-- Creates the audit_log table and inserts a baseline batch of 25 rows
-- with all columns explicitly provided (no defaults needed).
--
-- The queries.sql script then demonstrates CTE/COALESCE default patterns
-- for subsequent inserts and an UPDATE to archive old entries.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: audit_log
-- ============================================================================
-- Column defaults (conceptual, applied manually via COALESCE in queries.sql):
--   user_name   DEFAULT 'system'
--   severity    DEFAULT 'info'
--   retry_count DEFAULT 0
--   is_archived DEFAULT 0
--   notes       DEFAULT 'N/A'
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.audit_log (
    id           INT,
    action       VARCHAR,
    user_name    VARCHAR,
    severity     VARCHAR,
    retry_count  INT,
    is_archived  INT,
    created_at   VARCHAR,
    notes        VARCHAR
) LOCATION 'audit_log';


-- ============================================================================
-- STEP 2: Insert 25 rows — all columns explicitly provided (baseline data)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.audit_log
SELECT * FROM (VALUES
    (1,  'user.login',       'admin',   'info',     0, 0, '2024-01-01 08:00:00', 'Admin morning login'),
    (2,  'user.login',       'jdoe',    'info',     0, 0, '2024-01-01 08:15:00', 'Regular user login'),
    (3,  'config.change',    'system',  'warning',  0, 0, '2024-01-01 09:00:00', 'Config auto-update triggered'),
    (4,  'data.export',      'msmith',  'error',    2, 0, '2024-01-01 09:30:00', 'Export failed, retried'),
    (5,  'report.generate',  'analyst', 'info',     0, 0, '2024-01-01 10:00:00', 'N/A'),
    (6,  'user.logout',      'admin',   'info',     0, 0, '2024-01-01 10:30:00', 'Session ended normally'),
    (7,  'file.upload',      'jdoe',    'warning',  1, 0, '2024-01-02 08:00:00', 'Large file warning'),
    (8,  'backup.run',       'system',  'error',    3, 0, '2024-01-02 02:00:00', 'Backup timeout, retried'),
    (9,  'user.login',       'msmith',  'info',     0, 0, '2024-01-02 08:30:00', 'Morning login'),
    (10, 'cache.clear',      'analyst', 'info',     0, 0, '2024-01-02 09:00:00', 'N/A'),
    (11, 'schema.migrate',   'admin',   'warning',  1, 0, '2024-01-03 01:00:00', 'Schema v2 migration'),
    (12, 'data.import',      'jdoe',    'error',    2, 0, '2024-01-03 08:00:00', 'Import validation error'),
    (13, 'cron.execute',     'system',  'critical', 1, 0, '2024-01-03 03:00:00', 'Cron job timeout'),
    (14, 'api.call',         'msmith',  'critical', 3, 0, '2024-01-03 09:00:00', 'External API down'),
    (15, 'report.generate',  'analyst', 'info',     0, 0, '2024-01-03 10:00:00', 'N/A'),
    (16, 'permission.change','admin',   'warning',  1, 0, '2024-01-04 08:00:00', 'Role updated for jdoe'),
    (17, 'data.delete',      'jdoe',    'error',    1, 0, '2024-01-04 09:00:00', 'Soft delete triggered'),
    (18, 'health.check',     'system',  'critical', 2, 0, '2024-01-04 02:00:00', 'Service degraded'),
    (19, 'audit.review',     'msmith',  'critical', 1, 0, '2024-01-04 10:00:00', 'Compliance flag raised'),
    (20, 'dashboard.load',   'analyst', 'info',     0, 0, '2024-01-04 11:00:00', 'N/A'),
    (21, 'user.create',      'admin',   'info',     0, 0, '2024-01-05 08:00:00', 'New user onboarded'),
    (22, 'file.download',    'jdoe',    'warning',  1, 0, '2024-01-05 09:00:00', 'Bandwidth limit near'),
    (23, 'batch.process',    'system',  'error',    2, 0, '2024-01-05 03:00:00', 'Batch partial failure'),
    (24, 'notification.send','msmith',  'critical', 1, 0, '2024-01-05 10:00:00', 'Email gateway error'),
    (25, 'session.cleanup',  'analyst', 'info',     0, 0, '2024-01-05 23:00:00', 'N/A')
) AS t(id, action, user_name, severity, retry_count, is_archived, created_at, notes);

