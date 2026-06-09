-- ============================================================================
-- Delta Partitions & Deletion Vectors — Setup Script
-- ============================================================================
-- Creates a partitioned Delta table and loads baseline monitoring data.
--
-- Tables created:
--   1. cloud_events — 90 rows, partitioned by region (us-east, us-west, eu-west)
--
-- The queries.sql script then demonstrates DELETE (triggering DVs), UPDATE
-- (severity escalation), INSERT (critical events), and OPTIMIZE (compaction).
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: cloud_events — Cloud infrastructure monitoring events
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.cloud_events (
    id          INT,
    service     VARCHAR,
    region      VARCHAR,
    severity    VARCHAR,
    message     VARCHAR,
    latency_ms  INT,
    event_time  VARCHAR
) LOCATION 'delta-partitions-dv/cloud_events'
PARTITIONED BY (region)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
);


-- Region 1: us-east (30 events)
INSERT INTO {{zone_name}}.delta_demos.cloud_events VALUES
    (1,  'api-gateway',  'us-east', 'info',     'Request processed successfully',    45,  '2024-06-01 08:00:00'),
    (2,  'api-gateway',  'us-east', 'info',     'Health check passed',               12,  '2024-06-01 08:05:00'),
    (3,  'auth-service', 'us-east', 'warning',  'Token refresh delayed',             320, '2024-06-01 08:10:00'),
    (4,  'auth-service', 'us-east', 'info',     'User authenticated',                85,  '2024-06-01 08:15:00'),
    (5,  'data-pipeline','us-east', 'error',    'Pipeline stage timeout',            850, '2024-06-01 08:20:00'),
    (6,  'data-pipeline','us-east', 'info',     'Batch processing completed',        200, '2024-06-01 08:25:00'),
    (7,  'web-server',   'us-east', 'info',     'Static content served',             15,  '2024-06-01 08:30:00'),
    (8,  'web-server',   'us-east', 'warning',  'Slow response detected',            620, '2024-06-01 08:35:00'),
    (9,  'database',     'us-east', 'info',     'Query executed',                    30,  '2024-06-01 08:40:00'),
    (10, 'database',     'us-east', 'error',    'Connection pool exhausted',         900, '2024-06-01 08:45:00'),
    (11, 'cache-layer',  'us-east', 'info',     'Cache hit ratio 95%',               5,   '2024-06-01 08:50:00'),
    (12, 'cache-layer',  'us-east', 'info',     'Cache invalidation completed',      25,  '2024-06-01 08:55:00'),
    (13, 'api-gateway',  'us-east', 'warning',  'Rate limit approaching',            180, '2024-06-01 09:00:00'),
    (14, 'auth-service', 'us-east', 'info',     'Session created',                   60,  '2024-06-01 09:05:00'),
    (15, 'data-pipeline','us-east', 'info',     'ETL job started',                   100, '2024-06-01 09:10:00'),
    (16, 'web-server',   'us-east', 'error',    'SSL certificate expiring',          10,  '2024-06-01 09:15:00'),
    (17, 'database',     'us-east', 'warning',  'Slow query detected',               750, '2024-06-01 09:20:00'),
    (18, 'cache-layer',  'us-east', 'info',     'Memory usage normal',               8,   '2024-06-01 09:25:00'),
    (19, 'api-gateway',  'us-east', 'info',     'API version check passed',          20,  '2024-06-01 09:30:00'),
    (20, 'auth-service', 'us-east', 'error',    'Failed login attempt',              150, '2024-06-01 09:35:00'),
    (21, 'data-pipeline','us-east', 'warning',  'Data skew detected',                400, '2024-06-01 09:40:00'),
    (22, 'web-server',   'us-east', 'info',     'CDN cache refreshed',               35,  '2024-06-01 09:45:00'),
    (23, 'database',     'us-east', 'info',     'Index rebuild completed',           250, '2024-06-01 09:50:00'),
    (24, 'cache-layer',  'us-east', 'warning',  'Eviction rate high',                55,  '2024-06-01 09:55:00'),
    (25, 'api-gateway',  'us-east', 'info',     'Upstream health OK',                18,  '2024-06-01 10:00:00'),
    (26, 'auth-service', 'us-east', 'info',     'MFA verified',                      90,  '2024-06-01 10:05:00'),
    (27, 'data-pipeline','us-east', 'info',     'Checkpoint saved',                  70,  '2024-06-01 10:10:00'),
    (28, 'web-server',   'us-east', 'info',     'Request routed',                    22,  '2024-06-01 10:15:00'),
    (29, 'database',     'us-east', 'info',     'Replication lag 0ms',               3,   '2024-06-01 10:20:00'),
    (30, 'cache-layer',  'us-east', 'error',    'Redis connection lost',             1200,'2024-06-01 10:25:00');

-- Region 2: us-west (30 events)
INSERT INTO {{zone_name}}.delta_demos.cloud_events VALUES
    (31, 'api-gateway',  'us-west', 'info',     'Request processed successfully',    50,  '2024-06-01 08:00:00'),
    (32, 'api-gateway',  'us-west', 'info',     'Health check passed',               14,  '2024-06-01 08:05:00'),
    (33, 'auth-service', 'us-west', 'info',     'User authenticated',                75,  '2024-06-01 08:10:00'),
    (34, 'auth-service', 'us-west', 'warning',  'Certificate renewal pending',       110, '2024-06-01 08:15:00'),
    (35, 'data-pipeline','us-west', 'error',    'Source connection failed',           950, '2024-06-01 08:20:00'),
    (36, 'data-pipeline','us-west', 'info',     'Incremental load completed',        180, '2024-06-01 08:25:00'),
    (37, 'web-server',   'us-west', 'info',     'Static content served',             18,  '2024-06-01 08:30:00'),
    (38, 'web-server',   'us-west', 'warning',  'High memory usage',                 40,  '2024-06-01 08:35:00'),
    (39, 'database',     'us-west', 'info',     'Backup completed',                  25,  '2024-06-01 08:40:00'),
    (40, 'database',     'us-west', 'error',    'Deadlock detected',                 800, '2024-06-01 08:45:00'),
    (41, 'cache-layer',  'us-west', 'info',     'Cache warmed up',                   300, '2024-06-01 08:50:00'),
    (42, 'cache-layer',  'us-west', 'info',     'TTL policy applied',                10,  '2024-06-01 08:55:00'),
    (43, 'api-gateway',  'us-west', 'info',     'Load balancer healthy',             8,   '2024-06-01 09:00:00'),
    (44, 'auth-service', 'us-west', 'warning',  'Password policy violation',         65,  '2024-06-01 09:05:00'),
    (45, 'data-pipeline','us-west', 'info',     'Schema validation passed',          55,  '2024-06-01 09:10:00'),
    (46, 'web-server',   'us-west', 'info',     'Gzip compression enabled',          12,  '2024-06-01 09:15:00'),
    (47, 'database',     'us-west', 'warning',  'Table fragmentation high',          680, '2024-06-01 09:20:00'),
    (48, 'cache-layer',  'us-west', 'info',     'Cluster rebalanced',               150, '2024-06-01 09:25:00'),
    (49, 'api-gateway',  'us-west', 'error',    'Gateway timeout',                   1500,'2024-06-01 09:30:00'),
    (50, 'auth-service', 'us-west', 'info',     'Token issued',                      40,  '2024-06-01 09:35:00'),
    (51, 'data-pipeline','us-west', 'info',     'Watermark advanced',                80,  '2024-06-01 09:40:00'),
    (52, 'web-server',   'us-west', 'error',    'HTTP 503 returned',                 700, '2024-06-01 09:45:00'),
    (53, 'database',     'us-west', 'info',     'Statistics updated',                120, '2024-06-01 09:50:00'),
    (54, 'cache-layer',  'us-west', 'info',     'Hit ratio 92%',                     6,   '2024-06-01 09:55:00'),
    (55, 'api-gateway',  'us-west', 'info',     'Circuit breaker reset',             30,  '2024-06-01 10:00:00'),
    (56, 'auth-service', 'us-west', 'info',     'LDAP sync completed',              200, '2024-06-01 10:05:00'),
    (57, 'data-pipeline','us-west', 'warning',  'Late data detected',                350, '2024-06-01 10:10:00'),
    (58, 'web-server',   'us-west', 'info',     'Session affinity set',              15,  '2024-06-01 10:15:00'),
    (59, 'database',     'us-west', 'info',     'Connection pool healthy',           7,   '2024-06-01 10:20:00'),
    (60, 'cache-layer',  'us-west', 'warning',  'Memory pressure rising',            45,  '2024-06-01 10:25:00');

-- Region 3: eu-west (30 events)
INSERT INTO {{zone_name}}.delta_demos.cloud_events VALUES
    (61, 'api-gateway',  'eu-west', 'info',     'Request processed successfully',    55,  '2024-06-01 08:00:00'),
    (62, 'api-gateway',  'eu-west', 'warning',  'Latency spike detected',            520, '2024-06-01 08:05:00'),
    (63, 'auth-service', 'eu-west', 'info',     'OAuth flow completed',              95,  '2024-06-01 08:10:00'),
    (64, 'auth-service', 'eu-west', 'info',     'SAML assertion validated',          110, '2024-06-01 08:15:00'),
    (65, 'data-pipeline','eu-west', 'error',    'Partition write failed',            1100,'2024-06-01 08:20:00'),
    (66, 'data-pipeline','eu-west', 'info',     'Compaction triggered',              220, '2024-06-01 08:25:00'),
    (67, 'web-server',   'eu-west', 'info',     'TLS handshake OK',                  28,  '2024-06-01 08:30:00'),
    (68, 'web-server',   'eu-west', 'info',     'Keepalive connection reused',       8,   '2024-06-01 08:35:00'),
    (69, 'database',     'eu-west', 'warning',  'Vacuum running long',               600, '2024-06-01 08:40:00'),
    (70, 'database',     'eu-west', 'info',     'WAL archived',                      15,  '2024-06-01 08:45:00'),
    (71, 'cache-layer',  'eu-west', 'error',    'Cluster node unreachable',          2000,'2024-06-01 08:50:00'),
    (72, 'cache-layer',  'eu-west', 'info',     'Failover completed',               350, '2024-06-01 08:55:00'),
    (73, 'api-gateway',  'eu-west', 'info',     'Retry succeeded',                   180, '2024-06-01 09:00:00'),
    (74, 'auth-service', 'eu-west', 'info',     'Role assignment updated',           50,  '2024-06-01 09:05:00'),
    (75, 'data-pipeline','eu-west', 'info',     'Merge operation completed',         160, '2024-06-01 09:10:00'),
    (76, 'web-server',   'eu-west', 'warning',  'Request queue growing',             420, '2024-06-01 09:15:00'),
    (77, 'database',     'eu-west', 'info',     'Autovacuum started',                20,  '2024-06-01 09:20:00'),
    (78, 'cache-layer',  'eu-west', 'info',     'LRU eviction normal',              12,  '2024-06-01 09:25:00'),
    (79, 'api-gateway',  'eu-west', 'error',    'Backend unhealthy',                 900, '2024-06-01 09:30:00'),
    (80, 'auth-service', 'eu-west', 'warning',  'Brute force attempt blocked',       130, '2024-06-01 09:35:00'),
    (81, 'data-pipeline','eu-west', 'info',     'Sink write succeeded',              90,  '2024-06-01 09:40:00'),
    (82, 'web-server',   'eu-west', 'info',     'Compression ratio 85%',             10,  '2024-06-01 09:45:00'),
    (83, 'database',     'eu-west', 'error',    'Replication slot behind',           750, '2024-06-01 09:50:00'),
    (84, 'cache-layer',  'eu-west', 'info',     'Key distribution balanced',         5,   '2024-06-01 09:55:00'),
    (85, 'api-gateway',  'eu-west', 'info',     'Canary deployment passed',          35,  '2024-06-01 10:00:00'),
    (86, 'auth-service', 'eu-west', 'info',     'Certificate rotated',              70,  '2024-06-01 10:05:00'),
    (87, 'data-pipeline','eu-west', 'warning',  'Backpressure applied',              550, '2024-06-01 10:10:00'),
    (88, 'web-server',   'eu-west', 'info',     'Upstream DNS resolved',             22,  '2024-06-01 10:15:00'),
    (89, 'database',     'eu-west', 'info',     'Checkpoint completed',              40,  '2024-06-01 10:20:00'),
    (90, 'cache-layer',  'eu-west', 'info',     'Snapshot persisted',                60,  '2024-06-01 10:25:00');

