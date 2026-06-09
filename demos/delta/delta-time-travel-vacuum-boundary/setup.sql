-- ============================================================================
-- Delta Time Travel — VACUUM Retention Boundary — Setup Script
-- ============================================================================
-- A web analytics team tracks user activity across their platform. The
-- activity_log goes through 5 versions before VACUUM cleans up old files.
--
-- Version History:
--   V0: CREATE TABLE (empty)
--   V1: INSERT 15 activity records (5 users x 3 actions)
--   V2: UPDATE — add 10 to all durations (timezone correction)
--   V3: DELETE — remove bounce records (user 1 & 2 view events)
--   V4: INSERT — 5 new activity records (users 6-8)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- VERSION 0+1: CREATE TABLE + INSERT 15 activity records
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.activity_log (
    user_id        INT,
    action         VARCHAR,
    page           VARCHAR,
    duration_secs  INT,
    logged_at      VARCHAR
) LOCATION 'delta-time-travel-vacuum-boundary/activity_log';


INSERT INTO {{zone_name}}.delta_demos.activity_log VALUES
    -- User 1
    (1, 'view',     '/home',       3,  '2026-03-20 08:01:00'),
    (1, 'click',    '/products',   22, '2026-03-20 08:02:30'),
    (1, 'purchase', '/checkout',   45, '2026-03-20 08:05:00'),
    -- User 2
    (2, 'view',     '/home',       7,  '2026-03-20 09:10:00'),
    (2, 'click',    '/pricing',    18, '2026-03-20 09:12:00'),
    (2, 'purchase', '/checkout',   52, '2026-03-20 09:15:00'),
    -- User 3
    (3, 'view',     '/docs',       12, '2026-03-20 10:00:00'),
    (3, 'click',    '/docs/api',   35, '2026-03-20 10:03:00'),
    (3, 'purchase', '/checkout',   60, '2026-03-20 10:08:00'),
    -- User 4
    (4, 'view',     '/blog',       2,  '2026-03-20 11:30:00'),
    (4, 'click',    '/blog/post-1',28, '2026-03-20 11:32:00'),
    (4, 'purchase', '/checkout',   38, '2026-03-20 11:40:00'),
    -- User 5
    (5, 'view',     '/home',       9,  '2026-03-20 14:00:00'),
    (5, 'click',    '/features',   15, '2026-03-20 14:02:00'),
    (5, 'purchase', '/checkout',   41, '2026-03-20 14:10:00');


-- ============================================================================
-- VERSION 2: UPDATE — timezone correction (all durations += 10)
-- ============================================================================
UPDATE {{zone_name}}.delta_demos.activity_log
SET duration_secs = duration_secs + 10;


-- ============================================================================
-- VERSION 3: DELETE — remove bounce view events for users 1 & 2
-- ============================================================================
DELETE FROM {{zone_name}}.delta_demos.activity_log
WHERE user_id IN (1, 2) AND action = 'view';


-- ============================================================================
-- VERSION 4: INSERT — 5 new activity records
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.activity_log VALUES
    (6, 'view',     '/home',     25, '2026-03-22 16:00:00'),
    (6, 'click',    '/signup',   33, '2026-03-22 16:02:00'),
    (7, 'view',     '/pricing',  14, '2026-03-22 17:30:00'),
    (7, 'purchase', '/checkout', 48, '2026-03-22 17:35:00'),
    (8, 'click',    '/docs',     19, '2026-03-22 18:00:00');
