-- ============================================================================
-- Delta RESTORE — Advanced Recovery Scenarios — Setup Script
-- ============================================================================
-- Creates the config_settings table with initial V0 baseline data.
-- The version-building operations (V1–V5) are in queries.sql for
-- interactive exploration.
--
-- Tables created:
--   1. config_settings — 35 configuration entries across 5 categories
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- V0: CREATE + INSERT 35 config settings
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.config_settings (
    id          INT,
    key         VARCHAR,
    value       VARCHAR,
    category    VARCHAR,
    is_active   INT,
    updated_by  VARCHAR,
    updated_at  VARCHAR
) LOCATION 'delta-restore-advanced/config_settings';


INSERT INTO {{zone_name}}.delta_demos.config_settings VALUES
    -- database (10 entries: ids 1-10)
    (1,  'db.host',               'prod-db-01.internal',  'database', 1, 'deploy_bot',  '2025-01-15'),
    (2,  'db.port',               '5432',                 'database', 1, 'deploy_bot',  '2025-01-15'),
    (3,  'db.name',               'app_production',       'database', 1, 'deploy_bot',  '2025-01-15'),
    (4,  'db.pool.min',           '5',                    'database', 1, 'deploy_bot',  '2025-01-15'),
    (5,  'db.pool.max',           '20',                   'database', 1, 'deploy_bot',  '2025-01-15'),
    (6,  'db.timeout.connect',    '5000',                 'database', 1, 'deploy_bot',  '2025-01-15'),
    (7,  'db.timeout.read',       '30000',                'database', 1, 'deploy_bot',  '2025-01-15'),
    (8,  'db.ssl.enabled',        'true',                 'database', 1, 'deploy_bot',  '2025-01-15'),
    (9,  'db.legacy.driver',      'jdbc-v1',              'database', 0, 'admin',       '2024-06-01'),
    (10, 'db.legacy.pool',        '10',                   'database', 0, 'admin',       '2024-06-01'),
    -- cache (7 entries: ids 11-17)
    (11, 'cache.provider',        'redis',                'cache',    1, 'deploy_bot',  '2025-01-15'),
    (12, 'cache.host',            'redis-01.internal',    'cache',    1, 'deploy_bot',  '2025-01-15'),
    (13, 'cache.port',            '6379',                 'cache',    1, 'deploy_bot',  '2025-01-15'),
    (14, 'cache.ttl.default',     '3600',                 'cache',    1, 'deploy_bot',  '2025-01-15'),
    (15, 'cache.ttl.session',     '1800',                 'cache',    1, 'deploy_bot',  '2025-01-15'),
    (16, 'cache.max.memory',      '512mb',                'cache',    1, 'deploy_bot',  '2025-01-15'),
    (17, 'cache.old.endpoint',    'memcached-01',         'cache',    0, 'admin',       '2024-03-10'),
    -- auth (7 entries: ids 18-24)
    (18, 'auth.provider',         'oauth2',               'auth',     1, 'security_team', '2025-01-15'),
    (19, 'auth.token.expiry',     '3600',                 'auth',     1, 'security_team', '2025-01-15'),
    (20, 'auth.refresh.expiry',   '86400',                'auth',     1, 'security_team', '2025-01-15'),
    (21, 'auth.mfa.enabled',      'true',                 'auth',     1, 'security_team', '2025-01-15'),
    (22, 'auth.password.min',     '12',                   'auth',     1, 'security_team', '2025-01-15'),
    (23, 'auth.legacy.ldap',      'ldap://old-server',    'auth',     0, 'admin',         '2024-04-20'),
    (24, 'auth.legacy.radius',    'radius://old-auth',    'auth',     0, 'admin',         '2024-04-20'),
    -- logging (6 entries: ids 25-30)
    (25, 'log.level',             'INFO',                 'logging',  1, 'ops_team',    '2025-01-15'),
    (26, 'log.output',            'stdout',               'logging',  1, 'ops_team',    '2025-01-15'),
    (27, 'log.format',            'json',                 'logging',  1, 'ops_team',    '2025-01-15'),
    (28, 'log.rotation.days',     '30',                   'logging',  1, 'ops_team',    '2025-01-15'),
    (29, 'log.max.size',          '100mb',                'logging',  1, 'ops_team',    '2025-01-15'),
    (30, 'log.sentry.enabled',    'true',                 'logging',  1, 'ops_team',    '2025-01-15'),
    -- api (5 entries: ids 31-35)
    (31, 'api.rate.limit',        '1000',                 'api',      1, 'deploy_bot',  '2025-01-15'),
    (32, 'api.timeout',           '30000',                'api',      1, 'deploy_bot',  '2025-01-15'),
    (33, 'api.cors.origins',      '*',                    'api',      1, 'deploy_bot',  '2025-01-15'),
    (34, 'api.version',           'v2',                   'api',      1, 'deploy_bot',  '2025-01-15'),
    (35, 'api.docs.enabled',      'true',                 'api',      1, 'deploy_bot',  '2025-01-15');

