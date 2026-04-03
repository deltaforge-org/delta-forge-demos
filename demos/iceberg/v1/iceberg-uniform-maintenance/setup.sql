-- ============================================================================
-- Iceberg UniForm OPTIMIZE & VACUUM Maintenance — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table and seeds it with 40 application log
-- entries across 4 services (auth-service, api-gateway, payment-service,
-- notification-service) and 5 log levels (DEBUG, INFO, WARN, ERROR, FATAL).
--
-- Additional batches are inserted in queries.sql to create file fragmentation,
-- then OPTIMIZE compacts and VACUUM cleans up obsolete files — all while
-- verifying Iceberg metadata consistency.
--
-- Schema: log_id INT, service_name VARCHAR, log_level VARCHAR,
--         message VARCHAR, response_time_ms INT, endpoint VARCHAR,
--         log_date VARCHAR
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create Delta table with UniForm enabled
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.app_logs (
    log_id            INT,
    service_name      VARCHAR,
    log_level         VARCHAR,
    message           VARCHAR,
    response_time_ms  INT,
    endpoint          VARCHAR,
    log_date          VARCHAR
) LOCATION '{{data_path}}/app_logs'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.app_logs TO USER {{current_user}};

-- STEP 3: Seed 40 log entries (Batch 1)
INSERT INTO {{zone_name}}.iceberg_demos.app_logs VALUES
    (1,  'auth-service',         'INFO',  'User login successful',              45,   '/api/auth/login',        '2025-01-15'),
    (2,  'api-gateway',          'DEBUG', 'Request routed to backend',          12,   '/api/v1/users',          '2025-01-15'),
    (3,  'payment-service',      'INFO',  'Payment processed',                  230,  '/api/payments/charge',   '2025-01-15'),
    (4,  'notification-service', 'WARN',  'Email delivery delayed',             890,  '/api/notify/email',      '2025-01-15'),
    (5,  'auth-service',         'ERROR', 'Token validation failed',            15,   '/api/auth/validate',     '2025-01-15'),
    (6,  'api-gateway',          'INFO',  'Rate limit applied',                 8,    '/api/v1/orders',         '2025-01-15'),
    (7,  'payment-service',      'DEBUG', 'Idempotency key checked',            5,    '/api/payments/charge',   '2025-01-15'),
    (8,  'notification-service', 'INFO',  'Push notification sent',             120,  '/api/notify/push',       '2025-01-15'),
    (9,  'auth-service',         'DEBUG', 'Session cache hit',                  3,    '/api/auth/session',      '2025-01-15'),
    (10, 'api-gateway',          'WARN',  'Upstream timeout exceeded',          5010, '/api/v1/products',       '2025-01-15'),
    (11, 'payment-service',      'ERROR', 'Card declined',                      180,  '/api/payments/charge',   '2025-01-16'),
    (12, 'notification-service', 'DEBUG', 'Template cache refreshed',           25,   '/api/notify/template',   '2025-01-16'),
    (13, 'auth-service',         'INFO',  'Password reset initiated',           67,   '/api/auth/reset',        '2025-01-16'),
    (14, 'api-gateway',          'INFO',  'Health check passed',                2,    '/api/health',            '2025-01-16'),
    (15, 'payment-service',      'WARN',  'Retry attempt on gateway timeout',   3200, '/api/payments/charge',   '2025-01-16'),
    (16, 'notification-service', 'ERROR', 'SMS provider unreachable',           10000,'/api/notify/sms',        '2025-01-16'),
    (17, 'auth-service',         'FATAL', 'Database connection pool exhausted', 0,    '/api/auth/login',        '2025-01-16'),
    (18, 'api-gateway',          'DEBUG', 'Circuit breaker tripped',            1,    '/api/v1/inventory',      '2025-01-16'),
    (19, 'payment-service',      'INFO',  'Refund processed successfully',      145,  '/api/payments/refund',   '2025-01-16'),
    (20, 'notification-service', 'INFO',  'Webhook delivered',                  88,   '/api/notify/webhook',    '2025-01-16'),
    (21, 'auth-service',         'INFO',  'MFA challenge sent',                 34,   '/api/auth/mfa',          '2025-01-17'),
    (22, 'api-gateway',          'ERROR', 'SSL certificate expired',            0,    '/api/v1/users',          '2025-01-17'),
    (23, 'payment-service',      'INFO',  'Subscription renewed',              110,  '/api/payments/subscribe', '2025-01-17'),
    (24, 'notification-service', 'WARN',  'Queue depth exceeds threshold',      0,    '/api/notify/queue',      '2025-01-17'),
    (25, 'auth-service',         'DEBUG', 'OAuth token refreshed',              22,   '/api/auth/oauth',        '2025-01-17'),
    (26, 'api-gateway',          'INFO',  'New API version deployed',           0,    '/api/v2/users',          '2025-01-17'),
    (27, 'payment-service',      'FATAL', 'Ledger inconsistency detected',     0,    '/api/payments/reconcile','2025-01-17'),
    (28, 'notification-service', 'DEBUG', 'Preferences cache invalidated',      15,   '/api/notify/prefs',      '2025-01-17'),
    (29, 'auth-service',         'WARN',  'Brute force attempt detected',       0,    '/api/auth/login',        '2025-01-17'),
    (30, 'api-gateway',          'DEBUG', 'Request logging sampled',            1,    '/api/v1/analytics',      '2025-01-17'),
    (31, 'payment-service',      'INFO',  'Daily settlement completed',         450,  '/api/payments/settle',   '2025-01-18'),
    (32, 'notification-service', 'INFO',  'Batch emails dispatched',            2100, '/api/notify/batch',      '2025-01-18'),
    (33, 'auth-service',         'INFO',  'API key rotated',                    55,   '/api/auth/keys',         '2025-01-18'),
    (34, 'api-gateway',          'WARN',  'Deprecated endpoint accessed',       3,    '/api/v0/legacy',         '2025-01-18'),
    (35, 'payment-service',      'DEBUG', 'Tax calculation cached',             8,    '/api/payments/tax',      '2025-01-18'),
    (36, 'notification-service', 'ERROR', 'Template rendering failed',          0,    '/api/notify/render',     '2025-01-18'),
    (37, 'auth-service',         'DEBUG', 'LDAP sync completed',               1200, '/api/auth/ldap',         '2025-01-18'),
    (38, 'api-gateway',          'INFO',  'GraphQL query optimized',            78,   '/api/graphql',           '2025-01-18'),
    (39, 'payment-service',      'WARN',  'Currency conversion rate stale',     0,    '/api/payments/fx',       '2025-01-18'),
    (40, 'notification-service', 'INFO',  'Digest email scheduled',            0,    '/api/notify/digest',     '2025-01-18');
