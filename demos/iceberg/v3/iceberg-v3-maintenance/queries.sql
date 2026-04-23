-- ============================================================================
-- Iceberg V3 UniForm — SaaS Billing OPTIMIZE & VACUUM — Queries
-- ============================================================================
-- Lifecycle: seed (30) → batch 2 (15) → batch 3 (15) → OPTIMIZE → DELETE
-- trial/suspended → VACUUM → Iceberg read-back.
--
-- Each INSERT creates a separate data file, causing fragmentation.
-- OPTIMIZE compacts them. DELETE removes inactive accounts. VACUUM cleans
-- up orphaned files. The Iceberg V3 metadata must survive all operations.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — 30 Subscriptions
-- ============================================================================

ASSERT ROW_COUNT = 30
ASSERT VALUE company = 'Acme Corp' WHERE sub_id = 1
ASSERT VALUE mrr = 499.99 WHERE sub_id = 1
SELECT * FROM {{zone_name}}.iceberg_demos.subscriptions ORDER BY sub_id;


-- ============================================================================
-- Query 2: Baseline Per-Plan Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sub_count = 10 WHERE plan_tier = 'enterprise'
ASSERT VALUE sub_count = 10 WHERE plan_tier = 'pro'
ASSERT VALUE sub_count = 10 WHERE plan_tier = 'startup'
SELECT
    plan_tier,
    COUNT(*) AS sub_count,
    ROUND(SUM(mrr), 2) AS total_mrr
FROM {{zone_name}}.iceberg_demos.subscriptions
GROUP BY plan_tier
ORDER BY plan_tier;


-- ============================================================================
-- Query 3: Baseline Status Counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sub_count = 25 WHERE status = 'active'
ASSERT VALUE sub_count = 2 WHERE status = 'suspended'
ASSERT VALUE sub_count = 3 WHERE status = 'trial'
SELECT
    status,
    COUNT(*) AS sub_count
FROM {{zone_name}}.iceberg_demos.subscriptions
GROUP BY status
ORDER BY status;


-- ============================================================================
-- INSERT Batch 2: 15 More Subscriptions (Creates File Fragmentation)
-- ============================================================================

INSERT INTO {{zone_name}}.iceberg_demos.subscriptions VALUES
    (31, 'tenant-031', 'EchoBase',          'startup',    'monthly', 49.99,  'active',    '2024-03-03'),
    (32, 'tenant-032', 'FluxEngine',        'enterprise', 'annual',  499.99, 'active',    '2024-03-05'),
    (33, 'tenant-033', 'GlyphDB',           'pro',        'monthly', 149.99, 'trial',     '2024-03-07'),
    (34, 'tenant-034', 'HaloStack',         'startup',    'annual',  399.99, 'active',    '2024-03-09'),
    (35, 'tenant-035', 'IndexForge',        'enterprise', 'monthly', 599.99, 'active',    '2024-03-11'),
    (36, 'tenant-036', 'JiveOps',           'pro',        'annual',  1199.99,'active',    '2024-03-13'),
    (37, 'tenant-037', 'KernelSync',        'startup',    'monthly', 49.99,  'active',    '2024-03-15'),
    (38, 'tenant-038', 'LogicLayer',        'enterprise', 'annual',  499.99, 'suspended', '2024-03-17'),
    (39, 'tenant-039', 'MatrixFlow',        'pro',        'monthly', 149.99, 'active',    '2024-03-19'),
    (40, 'tenant-040', 'NexusGrid',         'startup',    'monthly', 49.99,  'active',    '2024-03-21'),
    (41, 'tenant-041', 'OrbitDB',           'enterprise', 'monthly', 599.99, 'active',    '2024-03-23'),
    (42, 'tenant-042', 'PrismIO',           'pro',        'monthly', 149.99, 'active',    '2024-03-25'),
    (43, 'tenant-043', 'QuasarNet',         'startup',    'annual',  399.99, 'active',    '2024-03-27'),
    (44, 'tenant-044', 'RiftEngine',        'enterprise', 'annual',  499.99, 'trial',     '2024-03-29'),
    (45, 'tenant-045', 'SynapseDB',         'pro',        'annual',  1199.99,'active',    '2024-03-31');


-- ============================================================================
-- INSERT Batch 3: 15 More Subscriptions (More Fragmentation)
-- ============================================================================

INSERT INTO {{zone_name}}.iceberg_demos.subscriptions VALUES
    (46, 'tenant-046', 'TensorOps',         'startup',    'monthly', 49.99,  'active',    '2024-04-02'),
    (47, 'tenant-047', 'UnityCloud',        'enterprise', 'annual',  499.99, 'active',    '2024-04-04'),
    (48, 'tenant-048', 'VectorDB',          'pro',        'monthly', 149.99, 'active',    '2024-04-06'),
    (49, 'tenant-049', 'WaveOps',           'startup',    'monthly', 49.99,  'trial',     '2024-04-08'),
    (50, 'tenant-050', 'XStreamIO',         'enterprise', 'monthly', 599.99, 'active',    '2024-04-10'),
    (51, 'tenant-051', 'YottaBase',         'pro',        'annual',  1199.99,'active',    '2024-04-12'),
    (52, 'tenant-052', 'ZetaForge',         'startup',    'annual',  399.99, 'active',    '2024-04-14'),
    (53, 'tenant-053', 'ArcticDB',          'enterprise', 'annual',  499.99, 'active',    '2024-04-16'),
    (54, 'tenant-054', 'BloomOps',          'pro',        'monthly', 149.99, 'suspended', '2024-04-18'),
    (55, 'tenant-055', 'CatalystIO',        'startup',    'monthly', 49.99,  'active',    '2024-04-20'),
    (56, 'tenant-056', 'DeepCore',          'enterprise', 'monthly', 599.99, 'active',    '2024-04-22'),
    (57, 'tenant-057', 'EmberStack',        'pro',        'monthly', 149.99, 'active',    '2024-04-24'),
    (58, 'tenant-058', 'FrostByte',         'startup',    'annual',  399.99, 'active',    '2024-04-26'),
    (59, 'tenant-059', 'GalaxyDB',          'enterprise', 'annual',  499.99, 'active',    '2024-04-28'),
    (60, 'tenant-060', 'HorizonOps',        'pro',        'annual',  1199.99,'active',    '2024-04-30');


-- ============================================================================
-- Query 4: Pre-OPTIMIZE — All 60 Rows Present
-- ============================================================================

ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.iceberg_demos.subscriptions ORDER BY sub_id;


-- ============================================================================
-- Query 5: Pre-OPTIMIZE Aggregates
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_subs = 60
ASSERT VALUE total_mrr = 24499.4
SELECT
    COUNT(*) AS total_subs,
    ROUND(SUM(mrr), 2) AS total_mrr
FROM {{zone_name}}.iceberg_demos.subscriptions;


-- ============================================================================
-- OPTIMIZE — Compact Fragmented Files
-- ============================================================================
-- Three INSERT batches created 3+ data files. OPTIMIZE rewrites them into
-- fewer files. V3 metadata must reflect the compacted manifests.

OPTIMIZE {{zone_name}}.iceberg_demos.subscriptions;


-- ============================================================================
-- Query 6: Post-OPTIMIZE — Data Integrity Preserved
-- ============================================================================

ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.iceberg_demos.subscriptions ORDER BY sub_id;


-- ============================================================================
-- Query 7: Post-OPTIMIZE Aggregates — Unchanged
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_subs = 60
ASSERT VALUE total_mrr = 24499.4
SELECT
    COUNT(*) AS total_subs,
    ROUND(SUM(mrr), 2) AS total_mrr
FROM {{zone_name}}.iceberg_demos.subscriptions;


-- ============================================================================
-- Query 8: Post-OPTIMIZE Per-Plan — Still Balanced
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sub_count = 20 WHERE plan_tier = 'enterprise'
ASSERT VALUE sub_count = 20 WHERE plan_tier = 'pro'
ASSERT VALUE sub_count = 20 WHERE plan_tier = 'startup'
SELECT
    plan_tier,
    COUNT(*) AS sub_count,
    ROUND(SUM(mrr), 2) AS total_mrr
FROM {{zone_name}}.iceberg_demos.subscriptions
GROUP BY plan_tier
ORDER BY plan_tier;


-- ============================================================================
-- DELETE — Remove Trial and Suspended Accounts
-- ============================================================================
-- In production, churned and trial accounts are periodically purged.

DELETE FROM {{zone_name}}.iceberg_demos.subscriptions
WHERE status IN ('trial', 'suspended');


-- ============================================================================
-- Query 9: Post-Delete — 50 Active Subscriptions Remain
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.iceberg_demos.subscriptions ORDER BY sub_id;


-- ============================================================================
-- Query 10: Post-Delete Status — Only Active
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE sub_count = 50 WHERE status = 'active'
SELECT
    status,
    COUNT(*) AS sub_count
FROM {{zone_name}}.iceberg_demos.subscriptions
GROUP BY status
ORDER BY status;


-- ============================================================================
-- Query 11: Post-Delete Per-Plan Counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sub_count = 17 WHERE plan_tier = 'enterprise'
ASSERT VALUE sub_count = 17 WHERE plan_tier = 'pro'
ASSERT VALUE sub_count = 16 WHERE plan_tier = 'startup'
SELECT
    plan_tier,
    COUNT(*) AS sub_count,
    ROUND(SUM(mrr), 2) AS total_mrr
FROM {{zone_name}}.iceberg_demos.subscriptions
GROUP BY plan_tier
ORDER BY plan_tier;


-- ============================================================================
-- VACUUM — Remove Obsolete Files
-- ============================================================================

VACUUM {{zone_name}}.iceberg_demos.subscriptions RETAIN 0 HOURS;


-- ============================================================================
-- Query 12: Post-VACUUM — Data Still Accessible
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.iceberg_demos.subscriptions ORDER BY sub_id;


-- ============================================================================
-- Query 13: Post-VACUUM Aggregates
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_subs = 50
ASSERT VALUE total_mrr = 22349.5
ASSERT VALUE avg_mrr = 446.99
SELECT
    COUNT(*) AS total_subs,
    ROUND(SUM(mrr), 2) AS total_mrr,
    ROUND(AVG(mrr), 2) AS avg_mrr
FROM {{zone_name}}.iceberg_demos.subscriptions;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_subs = 50
ASSERT VALUE total_mrr = 22349.5
ASSERT VALUE plan_count = 3
ASSERT VALUE enterprise_count = 17
ASSERT VALUE pro_count = 17
ASSERT VALUE startup_count = 16
SELECT
    COUNT(*) AS total_subs,
    ROUND(SUM(mrr), 2) AS total_mrr,
    COUNT(DISTINCT plan_tier) AS plan_count,
    SUM(CASE WHEN plan_tier = 'enterprise' THEN 1 ELSE 0 END) AS enterprise_count,
    SUM(CASE WHEN plan_tier = 'pro' THEN 1 ELSE 0 END) AS pro_count,
    SUM(CASE WHEN plan_tier = 'startup' THEN 1 ELSE 0 END) AS startup_count
FROM {{zone_name}}.iceberg_demos.subscriptions;


-- ============================================================================
-- ICEBERG V3 READ-BACK VERIFICATION
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.subscriptions_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.subscriptions_iceberg
USING ICEBERG
LOCATION 'subscriptions';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.subscriptions_iceberg TO USER {{current_user}};


-- ============================================================================
-- Iceberg Verify 1: Row Count — 50 Active Subscriptions
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.iceberg_demos.subscriptions_iceberg ORDER BY sub_id;


-- ============================================================================
-- Iceberg Verify 2: Spot-Check Seed Row — Enterprise Tenant
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE company = 'Acme Corp' WHERE sub_id = 1
ASSERT VALUE plan_tier = 'enterprise' WHERE sub_id = 1
ASSERT VALUE mrr = 499.99 WHERE sub_id = 1
ASSERT VALUE status = 'active' WHERE sub_id = 1
ASSERT VALUE billing_cycle = 'annual' WHERE sub_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.subscriptions_iceberg
WHERE sub_id = 1;


-- ============================================================================
-- Iceberg Verify 3: Spot-Check Batch 2 Row — Survived OPTIMIZE + VACUUM
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE company = 'SynapseDB' WHERE sub_id = 45
ASSERT VALUE plan_tier = 'pro' WHERE sub_id = 45
ASSERT VALUE mrr = 1199.99 WHERE sub_id = 45
ASSERT VALUE billing_cycle = 'annual' WHERE sub_id = 45
SELECT *
FROM {{zone_name}}.iceberg_demos.subscriptions_iceberg
WHERE sub_id = 45;


-- ============================================================================
-- Iceberg Verify 4: Spot-Check Batch 3 Row — Latest Insert Survived
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE company = 'HorizonOps' WHERE sub_id = 60
ASSERT VALUE plan_tier = 'pro' WHERE sub_id = 60
ASSERT VALUE mrr = 1199.99 WHERE sub_id = 60
SELECT *
FROM {{zone_name}}.iceberg_demos.subscriptions_iceberg
WHERE sub_id = 60;


-- ============================================================================
-- Iceberg Verify 5: Deleted Rows Are Gone
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.subscriptions_iceberg
WHERE status IN ('trial', 'suspended');


-- ============================================================================
-- Iceberg Verify 6: Per-Plan Aggregates Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sub_count = 17 WHERE plan_tier = 'enterprise'
ASSERT VALUE sub_count = 17 WHERE plan_tier = 'pro'
ASSERT VALUE sub_count = 16 WHERE plan_tier = 'startup'
SELECT
    plan_tier,
    COUNT(*) AS sub_count,
    ROUND(SUM(mrr), 2) AS total_mrr
FROM {{zone_name}}.iceberg_demos.subscriptions_iceberg
GROUP BY plan_tier
ORDER BY plan_tier;


-- ============================================================================
-- Iceberg Verify 7: Grand Totals Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_subs = 50
ASSERT VALUE total_mrr = 22349.5
ASSERT VALUE avg_mrr = 446.99
SELECT
    COUNT(*) AS total_subs,
    ROUND(SUM(mrr), 2) AS total_mrr,
    ROUND(AVG(mrr), 2) AS avg_mrr
FROM {{zone_name}}.iceberg_demos.subscriptions_iceberg;
