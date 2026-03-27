-- ============================================================================
-- Iceberg UniForm OPTIMIZE & VACUUM Maintenance — Queries
-- ============================================================================
-- HOW UNIFORM WORKS
-- -----------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically by the post-commit hook.
-- Each DML operation creates both a new Delta version and a new Iceberg
-- snapshot. The final Iceberg read-back proves metadata consistency after
-- the full maintenance lifecycle: INSERT → OPTIMIZE → DELETE → VACUUM.
--
-- WHAT THIS DEMO SHOWS
-- --------------------
-- 1. Baseline queries on 40 seed rows
-- 2. Two additional INSERT batches to create file fragmentation
-- 3. OPTIMIZE to compact small files into fewer, larger ones
-- 4. Post-OPTIMIZE verification: data integrity preserved
-- 5. DELETE old DEBUG logs
-- 6. VACUUM to remove obsolete files
-- 7. Post-VACUUM verification: data still accessible
-- 8. Cross-format verification via Iceberg read-back
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline State (40 seed rows)
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.iceberg_demos.app_logs ORDER BY log_id;


-- ============================================================================
-- Query 1: Per-Service Log Counts — Baseline
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE log_count = 10 WHERE service_name = 'api-gateway'
ASSERT VALUE log_count = 10 WHERE service_name = 'auth-service'
ASSERT VALUE log_count = 10 WHERE service_name = 'notification-service'
ASSERT VALUE log_count = 10 WHERE service_name = 'payment-service'
SELECT
    service_name,
    COUNT(*) AS log_count
FROM {{zone_name}}.iceberg_demos.app_logs
GROUP BY service_name
ORDER BY service_name;


-- ============================================================================
-- Query 2: Per-Level Log Counts — Baseline
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE log_count = 10 WHERE log_level = 'DEBUG'
ASSERT VALUE log_count = 16 WHERE log_level = 'INFO'
ASSERT VALUE log_count = 7 WHERE log_level = 'WARN'
ASSERT VALUE log_count = 5 WHERE log_level = 'ERROR'
ASSERT VALUE log_count = 2 WHERE log_level = 'FATAL'
SELECT
    log_level,
    COUNT(*) AS log_count
FROM {{zone_name}}.iceberg_demos.app_logs
GROUP BY log_level
ORDER BY log_level;


-- ============================================================================
-- LEARN: INSERT Batch 2 — Create File Fragmentation (20 more rows)
-- ============================================================================
-- Each INSERT creates a new data file. Multiple small files degrade query
-- performance — exactly the problem OPTIMIZE solves.

INSERT INTO {{zone_name}}.iceberg_demos.app_logs VALUES
    (41, 'auth-service',         'INFO',  'SSO login successful',               38,   '/api/auth/sso',         '2025-01-19'),
    (42, 'api-gateway',          'DEBUG', 'Load balancer health check',         2,    '/api/health/lb',        '2025-01-19'),
    (43, 'payment-service',      'ERROR', 'Duplicate transaction detected',     95,   '/api/payments/charge',  '2025-01-19'),
    (44, 'notification-service', 'INFO',  'In-app notification delivered',      42,   '/api/notify/inapp',     '2025-01-19'),
    (45, 'auth-service',         'WARN',  'Session expiry approaching',         0,    '/api/auth/session',     '2025-01-19'),
    (46, 'api-gateway',          'INFO',  'API throttle adjusted',              5,    '/api/v1/config',        '2025-01-19'),
    (47, 'payment-service',      'DEBUG', 'Webhook signature verified',         12,   '/api/payments/webhook', '2025-01-19'),
    (48, 'notification-service', 'FATAL', 'Message broker connection lost',     0,    '/api/notify/broker',    '2025-01-19'),
    (49, 'auth-service',         'INFO',  'User account locked',               28,   '/api/auth/lock',        '2025-01-19'),
    (50, 'api-gateway',          'ERROR', 'Request body too large',             0,    '/api/v1/upload',        '2025-01-19'),
    (51, 'payment-service',      'INFO',  'Chargeback initiated',              200,  '/api/payments/dispute',  '2025-01-20'),
    (52, 'notification-service', 'DEBUG', 'Rate limiter reset',                1,    '/api/notify/ratelimit', '2025-01-20'),
    (53, 'auth-service',         'DEBUG', 'Permission cache rebuilt',           350,  '/api/auth/permissions', '2025-01-20'),
    (54, 'api-gateway',          'WARN',  'Memory usage above 80%',            0,    '/api/metrics',          '2025-01-20'),
    (55, 'payment-service',      'INFO',  'PCI compliance scan passed',        0,    '/api/payments/pci',     '2025-01-20'),
    (56, 'notification-service', 'WARN',  'Delivery receipt timeout',          15000,'/api/notify/receipt',   '2025-01-20'),
    (57, 'auth-service',         'ERROR', 'SAML assertion invalid',            0,    '/api/auth/saml',        '2025-01-20'),
    (58, 'api-gateway',          'INFO',  'Cache purge completed',             150,  '/api/cache/purge',      '2025-01-20'),
    (59, 'payment-service',      'DEBUG', 'Exchange rate updated',             5,    '/api/payments/fx',      '2025-01-20'),
    (60, 'notification-service', 'INFO',  'Unsubscribe processed',            30,   '/api/notify/unsub',     '2025-01-20');


-- ============================================================================
-- LEARN: INSERT Batch 3 — More Fragmentation (20 more rows)
-- ============================================================================
-- A third data file is created. We now have at least 3 separate Parquet
-- files that OPTIMIZE will compact into fewer files.

INSERT INTO {{zone_name}}.iceberg_demos.app_logs VALUES
    (61, 'auth-service',         'INFO',  'Two-factor enabled',                42,   '/api/auth/2fa',         '2025-01-21'),
    (62, 'api-gateway',          'DEBUG', 'Connection pool expanded',          8,    '/api/v1/pool',          '2025-01-21'),
    (63, 'payment-service',      'WARN',  'Payment gateway latency high',     4500, '/api/payments/charge',  '2025-01-21'),
    (64, 'notification-service', 'INFO',  'Campaign email sent',              180,  '/api/notify/campaign',   '2025-01-21'),
    (65, 'auth-service',         'ERROR', 'Certificate pinning failure',       0,    '/api/auth/cert',        '2025-01-21'),
    (66, 'api-gateway',          'INFO',  'Canary deployment routed',         15,   '/api/v2/canary',        '2025-01-21'),
    (67, 'payment-service',      'INFO',  'Payout batch completed',           600,  '/api/payments/payout',  '2025-01-21'),
    (68, 'notification-service', 'DEBUG', 'Template version updated',         20,   '/api/notify/template',  '2025-01-21'),
    (69, 'auth-service',         'DEBUG', 'Audit log flushed',               100,  '/api/auth/audit',       '2025-01-21'),
    (70, 'notification-service', 'ERROR', 'Push certificate expired',         0,    '/api/notify/push',      '2025-01-21'),
    (71, 'api-gateway',          'WARN',  'DNS resolution slow',             2000, '/api/v1/dns',           '2025-01-22'),
    (72, 'payment-service',      'DEBUG', 'Fraud score calculated',          45,   '/api/payments/fraud',   '2025-01-22'),
    (73, 'auth-service',         'INFO',  'Service account created',         88,   '/api/auth/service',     '2025-01-22'),
    (74, 'notification-service', 'WARN',  'Attachment size limit reached',    0,    '/api/notify/attach',    '2025-01-22'),
    (75, 'api-gateway',          'INFO',  'Rate limit rule updated',         10,   '/api/v1/ratelimit',     '2025-01-22'),
    (76, 'payment-service',      'INFO',  'Monthly invoice generated',       300,  '/api/payments/invoice',  '2025-01-22'),
    (77, 'auth-service',         'WARN',  'Weak password detected',          0,    '/api/auth/password',    '2025-01-22'),
    (78, 'notification-service', 'INFO',  'Slack integration active',        55,   '/api/notify/slack',     '2025-01-22'),
    (79, 'api-gateway',          'DEBUG', 'Metrics endpoint scraped',        3,    '/api/metrics/scrape',   '2025-01-22'),
    (80, 'payment-service',      'FATAL', 'Double-spend detected',          0,    '/api/payments/verify',  '2025-01-22');


-- ============================================================================
-- Query 3: Pre-OPTIMIZE Row Count — All 80 Rows Present
-- ============================================================================

ASSERT ROW_COUNT = 80
SELECT * FROM {{zone_name}}.iceberg_demos.app_logs ORDER BY log_id;


-- ============================================================================
-- Query 4: Pre-OPTIMIZE Aggregates — Snapshot Before Compaction
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_logs = 80
ASSERT VALUE total_response_ms = 48046
ASSERT VALUE avg_response_ms = 600.58
SELECT
    COUNT(*) AS total_logs,
    SUM(response_time_ms) AS total_response_ms,
    ROUND(AVG(response_time_ms), 2) AS avg_response_ms
FROM {{zone_name}}.iceberg_demos.app_logs;


-- ============================================================================
-- LEARN: OPTIMIZE — Compact Small Files
-- ============================================================================
-- OPTIMIZE rewrites small Parquet files into fewer, larger files for better
-- read performance. The logical data is unchanged — only the physical layout
-- improves. The Iceberg metadata must reflect the compacted manifests.

OPTIMIZE {{zone_name}}.iceberg_demos.app_logs;


-- ============================================================================
-- Query 5: Post-OPTIMIZE Row Count — Data Integrity Preserved
-- ============================================================================
-- After compaction, all 80 rows must still be present and queryable.

ASSERT ROW_COUNT = 80
SELECT * FROM {{zone_name}}.iceberg_demos.app_logs ORDER BY log_id;


-- ============================================================================
-- Query 6: Post-OPTIMIZE Aggregates — Values Unchanged
-- ============================================================================
-- Compaction must not alter any data values. Same totals as pre-OPTIMIZE.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_logs = 80
ASSERT VALUE total_response_ms = 48046
ASSERT VALUE avg_response_ms = 600.58
SELECT
    COUNT(*) AS total_logs,
    SUM(response_time_ms) AS total_response_ms,
    ROUND(AVG(response_time_ms), 2) AS avg_response_ms
FROM {{zone_name}}.iceberg_demos.app_logs;


-- ============================================================================
-- Query 7: Post-OPTIMIZE Per-Service Counts — Still Balanced
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE log_count = 20 WHERE service_name = 'api-gateway'
ASSERT VALUE log_count = 20 WHERE service_name = 'auth-service'
ASSERT VALUE log_count = 20 WHERE service_name = 'notification-service'
ASSERT VALUE log_count = 20 WHERE service_name = 'payment-service'
SELECT
    service_name,
    COUNT(*) AS log_count
FROM {{zone_name}}.iceberg_demos.app_logs
GROUP BY service_name
ORDER BY service_name;


-- ============================================================================
-- LEARN: DELETE — Remove Old DEBUG Logs
-- ============================================================================
-- In production, DEBUG logs are useful for troubleshooting but expensive to
-- store long-term. This deletes all 20 DEBUG-level log entries.

DELETE FROM {{zone_name}}.iceberg_demos.app_logs WHERE log_level = 'DEBUG';


-- ============================================================================
-- Query 8: Post-Delete Row Count — 20 DEBUG Rows Removed
-- ============================================================================

ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.iceberg_demos.app_logs ORDER BY log_id;


-- ============================================================================
-- Query 9: Post-Delete Level Counts — No More DEBUG
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE log_count = 10 WHERE log_level = 'ERROR'
ASSERT VALUE log_count = 4 WHERE log_level = 'FATAL'
ASSERT VALUE log_count = 32 WHERE log_level = 'INFO'
ASSERT VALUE log_count = 14 WHERE log_level = 'WARN'
SELECT
    log_level,
    COUNT(*) AS log_count
FROM {{zone_name}}.iceberg_demos.app_logs
GROUP BY log_level
ORDER BY log_level;


-- ============================================================================
-- Query 10: Post-Delete Per-Service Counts
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE log_count = 14 WHERE service_name = 'api-gateway'
ASSERT VALUE log_count = 15 WHERE service_name = 'auth-service'
ASSERT VALUE log_count = 16 WHERE service_name = 'notification-service'
ASSERT VALUE log_count = 15 WHERE service_name = 'payment-service'
SELECT
    service_name,
    COUNT(*) AS log_count
FROM {{zone_name}}.iceberg_demos.app_logs
GROUP BY service_name
ORDER BY service_name;


-- ============================================================================
-- LEARN: VACUUM — Remove Obsolete Files
-- ============================================================================
-- VACUUM removes data files no longer referenced by the current Delta log.
-- After this operation, time travel to pre-VACUUM versions will fail because
-- the underlying Parquet files have been physically deleted.
--
-- NOTE: RETAIN 0 HOURS removes all obsolete files immediately. In production,
-- use a longer retention period to allow concurrent readers to finish.

VACUUM {{zone_name}}.iceberg_demos.app_logs RETAIN 0 HOURS;


-- ============================================================================
-- Query 11: Post-VACUUM Row Count — Current Data Still Accessible
-- ============================================================================
-- The current snapshot (60 rows, no DEBUG) must remain fully readable.

ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.iceberg_demos.app_logs ORDER BY log_id;


-- ============================================================================
-- Query 12: Post-VACUUM Aggregates
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_logs = 60
ASSERT VALUE total_response_ms = 46208
ASSERT VALUE avg_response_ms = 770.13
SELECT
    COUNT(*) AS total_logs,
    SUM(response_time_ms) AS total_response_ms,
    ROUND(AVG(response_time_ms), 2) AS avg_response_ms
FROM {{zone_name}}.iceberg_demos.app_logs;


-- ============================================================================
-- Query 13: Post-VACUUM — No DEBUG Rows Exist
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.iceberg_demos.app_logs WHERE log_level = 'DEBUG';


-- ============================================================================
-- VERIFY: Cross-Cutting Sanity Check
-- ============================================================================
-- Confirms the final state after the full maintenance lifecycle:
-- INSERT (40) → INSERT (20) → INSERT (20) → OPTIMIZE → DELETE DEBUG → VACUUM

ASSERT ROW_COUNT = 1
ASSERT VALUE total_logs = 60
ASSERT VALUE service_count = 4
ASSERT VALUE level_count = 4
ASSERT VALUE avg_response_ms = 770.13
ASSERT VALUE error_and_fatal = 14
SELECT
    COUNT(*) AS total_logs,
    COUNT(DISTINCT service_name) AS service_count,
    COUNT(DISTINCT log_level) AS level_count,
    ROUND(AVG(response_time_ms), 2) AS avg_response_ms,
    COUNT(*) FILTER (WHERE log_level IN ('ERROR', 'FATAL')) AS error_and_fatal
FROM {{zone_name}}.iceberg_demos.app_logs;


-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata remains consistent after OPTIMIZE, DELETE, and VACUUM.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.app_logs_iceberg
USING ICEBERG
LOCATION '{{data_path}}/app_logs';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.app_logs_iceberg TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg_demos.app_logs_iceberg;


-- ============================================================================
-- Iceberg Verify 1: Row Count — 60 Logs After Full Maintenance Lifecycle
-- ============================================================================

ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.iceberg_demos.app_logs_iceberg ORDER BY log_id;


-- ============================================================================
-- Iceberg Verify 2: No DEBUG Logs Visible Through Iceberg
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.iceberg_demos.app_logs_iceberg WHERE log_level = 'DEBUG';


-- ============================================================================
-- Iceberg Verify 3: Per-Level Counts — Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE log_count = 10 WHERE log_level = 'ERROR'
ASSERT VALUE log_count = 4 WHERE log_level = 'FATAL'
ASSERT VALUE log_count = 32 WHERE log_level = 'INFO'
ASSERT VALUE log_count = 14 WHERE log_level = 'WARN'
SELECT
    log_level,
    COUNT(*) AS log_count
FROM {{zone_name}}.iceberg_demos.app_logs_iceberg
GROUP BY log_level
ORDER BY log_level;


-- ============================================================================
-- Iceberg Verify 4: Grand Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_logs = 60
ASSERT VALUE total_response_ms = 46208
ASSERT VALUE avg_response_ms = 770.13
SELECT
    COUNT(*) AS total_logs,
    SUM(response_time_ms) AS total_response_ms,
    ROUND(AVG(response_time_ms), 2) AS avg_response_ms
FROM {{zone_name}}.iceberg_demos.app_logs_iceberg;
