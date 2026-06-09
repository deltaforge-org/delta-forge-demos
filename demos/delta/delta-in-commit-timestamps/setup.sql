-- ============================================================================
-- Delta In-Commit Timestamps — Reliable Version Timing — Setup Script
-- ============================================================================
-- Demonstrates in-commit timestamps for reliable version timing.
-- Each DML operation creates a new Delta version with a monotonically
-- increasing commit timestamp embedded in the transaction log.
--
-- Table created:
--   1. release_tracker — 30 release records across 3 environments
--
-- Version history (6 versions):
--   V0: CREATE TABLE with delta.enableInCommitTimestamps = true
--   V1: INSERT 15 production releases
--   V2: INSERT 10 staging releases
--   V3: INSERT 5 development releases
--   V4: UPDATE 3 releases → rolled_back
--   V5: UPDATE 2 releases → failed
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: release_tracker — deployment tracking with in-commit timestamps
-- ============================================================================
-- The key property is delta.enableInCommitTimestamps = true, which embeds
-- a reliable timestamp into each commit in the _delta_log. Without this,
-- Delta derives timestamps from file modification times, which break
-- TIMESTAMP AS OF queries when writers have clock skew.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.release_tracker (
    id                  INT,
    app_name            VARCHAR,
    environment         VARCHAR,
    version_tag         VARCHAR,
    deployer            VARCHAR,
    status              VARCHAR,
    duration_sec        INT,
    deployed_at         VARCHAR,
    commit_hash         VARCHAR
) LOCATION 'delta-in-commit-timestamps/release_tracker'
TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true');


-- ============================================================================
-- V1: INSERT 15 production releases (5 apps × 3 releases each)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.release_tracker VALUES
    (1,  'api-gateway',      'production', 'v2.1.0', 'alice',   'success', 120, '2025-06-01 08:00:00', 'a1b2c3d4'),
    (2,  'api-gateway',      'production', 'v2.1.1', 'bob',     'success', 95,  '2025-06-05 10:30:00', 'b2c3d4e5'),
    (3,  'api-gateway',      'production', 'v2.2.0', 'alice',   'success', 140, '2025-06-10 14:00:00', 'c3d4e5f6'),
    (4,  'user-service',     'production', 'v3.0.0', 'bob',     'success', 200, '2025-06-02 11:00:00', 'd4e5f6g7'),
    (5,  'user-service',     'production', 'v3.0.1', 'alice',   'success', 110, '2025-06-06 16:45:00', 'e5f6g7h8'),
    (6,  'user-service',     'production', 'v3.1.0', 'charlie', 'success', 180, '2025-06-11 08:30:00', 'f6g7h8i9'),
    (7,  'payment-engine',   'production', 'v1.5.0', 'charlie', 'success', 300, '2025-06-01 20:00:00', 'g7h8i9j0'),
    (8,  'payment-engine',   'production', 'v1.5.1', 'alice',   'success', 150, '2025-06-07 07:30:00', 'h8i9j0k1'),
    (9,  'payment-engine',   'production', 'v1.6.0', 'bob',     'success', 280, '2025-06-12 19:00:00', 'i9j0k1l2'),
    (10, 'notification-hub', 'production', 'v4.0.0', 'alice',   'success', 160, '2025-06-03 15:30:00', 'j0k1l2m3'),
    (11, 'notification-hub', 'production', 'v4.0.1', 'bob',     'success', 75,  '2025-06-08 09:00:00', 'k1l2m3n4'),
    (12, 'notification-hub', 'production', 'v4.1.0', 'charlie', 'success', 190, '2025-06-13 12:15:00', 'l2m3n4o5'),
    (13, 'search-indexer',   'production', 'v2.0.0', 'bob',     'success', 240, '2025-06-04 18:00:00', 'm3n4o5p6'),
    (14, 'search-indexer',   'production', 'v2.0.1', 'charlie', 'success', 100, '2025-06-09 11:30:00', 'n4o5p6q7'),
    (15, 'search-indexer',   'production', 'v2.1.0', 'alice',   'success', 210, '2025-06-14 16:00:00', 'o5p6q7r8');


-- ============================================================================
-- V2: INSERT 10 staging releases (5 apps × 2 release candidates each)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.release_tracker VALUES
    (16, 'api-gateway',      'staging', 'v2.3.0-rc1', 'alice',   'success', 90,  '2025-06-14 08:00:00', 'p6q7r8s9'),
    (17, 'api-gateway',      'staging', 'v2.3.0-rc2', 'bob',     'success', 85,  '2025-06-15 09:00:00', 'q7r8s9t0'),
    (18, 'user-service',     'staging', 'v3.2.0-rc1', 'charlie', 'success', 170, '2025-06-14 11:30:00', 'r8s9t0u1'),
    (19, 'user-service',     'staging', 'v3.2.0-rc2', 'alice',   'success', 155, '2025-06-15 13:00:00', 's9t0u1v2'),
    (20, 'payment-engine',   'staging', 'v1.7.0-rc1', 'bob',     'success', 260, '2025-06-14 15:00:00', 't0u1v2w3'),
    (21, 'payment-engine',   'staging', 'v1.7.0-rc2', 'charlie', 'success', 240, '2025-06-15 16:30:00', 'u1v2w3x4'),
    (22, 'notification-hub', 'staging', 'v4.2.0-rc1', 'alice',   'success', 140, '2025-06-14 17:00:00', 'v2w3x4y5'),
    (23, 'notification-hub', 'staging', 'v4.2.0-rc2', 'bob',     'success', 125, '2025-06-15 18:00:00', 'w3x4y5z6'),
    (24, 'search-indexer',   'staging', 'v2.2.0-rc1', 'charlie', 'success', 200, '2025-06-14 19:30:00', 'x4y5z6a7'),
    (25, 'search-indexer',   'staging', 'v2.2.0-rc2', 'alice',   'success', 185, '2025-06-15 20:00:00', 'y5z6a7b8');


-- ============================================================================
-- V3: INSERT 5 development releases (5 apps × 1 each)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.release_tracker VALUES
    (26, 'api-gateway',      'development', 'v2.4.0-dev', 'alice',   'success', 60,  '2025-06-15 07:00:00', 'z6a7b8c9'),
    (27, 'user-service',     'development', 'v3.3.0-dev', 'charlie', 'success', 130, '2025-06-15 08:00:00', 'a7b8c9d0'),
    (28, 'payment-engine',   'development', 'v1.8.0-dev', 'bob',     'success', 210, '2025-06-15 09:00:00', 'b8c9d0e1'),
    (29, 'notification-hub', 'development', 'v4.3.0-dev', 'alice',   'success', 100, '2025-06-15 10:00:00', 'c9d0e1f2'),
    (30, 'search-indexer',   'development', 'v2.3.0-dev', 'charlie', 'success', 150, '2025-06-15 11:00:00', 'd0e1f2g3');


-- ============================================================================
-- V4: UPDATE — 3 releases rolled back
-- ============================================================================
UPDATE {{zone_name}}.delta_demos.release_tracker
SET status = 'rolled_back'
WHERE id IN (3, 9, 22);


-- ============================================================================
-- V5: UPDATE — 2 releases failed
-- ============================================================================
UPDATE {{zone_name}}.delta_demos.release_tracker
SET status = 'failed'
WHERE id IN (12, 20);
