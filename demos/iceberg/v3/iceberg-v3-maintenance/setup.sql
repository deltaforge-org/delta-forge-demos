-- ============================================================================
-- Iceberg V3 UniForm — SaaS Billing OPTIMIZE & VACUUM — Setup
-- ============================================================================
-- Creates a Delta table with UniForm V3 for tracking SaaS subscription
-- billing. Seeds 30 tenants across 3 plan tiers (enterprise, pro, startup).
-- Additional INSERT batches and maintenance ops happen in queries.sql.
--
-- Dataset: 30 subscriptions, 3 plans, 3 billing cycles (monthly/annual),
-- 3 statuses (active/trial/suspended).
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm V3
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.subscriptions (
    sub_id        INT,
    tenant_id     VARCHAR,
    company       VARCHAR,
    plan_tier     VARCHAR,
    billing_cycle VARCHAR,
    mrr           DOUBLE,
    status        VARCHAR,
    start_date    VARCHAR
) LOCATION '{{data_path}}/subscriptions'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '3',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.subscriptions TO USER {{current_user}};

-- STEP 3: Seed 30 subscriptions (Version 1)
INSERT INTO {{zone_name}}.iceberg_demos.subscriptions VALUES
    (1,  'tenant-001', 'Acme Corp',        'enterprise', 'annual',  499.99, 'active',    '2024-01-01'),
    (2,  'tenant-002', 'Beta Labs',         'startup',    'monthly', 49.99,  'active',    '2024-01-03'),
    (3,  'tenant-003', 'CloudNine Inc',     'enterprise', 'annual',  499.99, 'active',    '2024-01-05'),
    (4,  'tenant-004', 'DataDriven LLC',    'pro',        'monthly', 149.99, 'active',    '2024-01-07'),
    (5,  'tenant-005', 'ElastiScale',       'startup',    'annual',  399.99, 'active',    '2024-01-09'),
    (6,  'tenant-006', 'FogCompute',        'pro',        'monthly', 149.99, 'active',    '2024-01-11'),
    (7,  'tenant-007', 'GridOps',           'enterprise', 'annual',  499.99, 'active',    '2024-01-13'),
    (8,  'tenant-008', 'HyperSync',         'startup',    'monthly', 49.99,  'trial',     '2024-01-15'),
    (9,  'tenant-009', 'InfraStack',        'pro',        'annual',  1199.99,'active',    '2024-01-17'),
    (10, 'tenant-010', 'JetBridge',         'enterprise', 'monthly', 599.99, 'active',    '2024-01-19'),
    (11, 'tenant-011', 'KubeForge',         'startup',    'monthly', 49.99,  'active',    '2024-01-21'),
    (12, 'tenant-012', 'LightFlow',         'pro',        'monthly', 149.99, 'suspended', '2024-01-23'),
    (13, 'tenant-013', 'MeshNet',           'enterprise', 'annual',  499.99, 'active',    '2024-01-25'),
    (14, 'tenant-014', 'NodePulse',         'startup',    'annual',  399.99, 'active',    '2024-01-27'),
    (15, 'tenant-015', 'OrcaDB',            'pro',        'monthly', 149.99, 'active',    '2024-01-29'),
    (16, 'tenant-016', 'PipelineIQ',        'enterprise', 'annual',  499.99, 'trial',     '2024-02-01'),
    (17, 'tenant-017', 'QueryStar',         'startup',    'monthly', 49.99,  'active',    '2024-02-03'),
    (18, 'tenant-018', 'ReplicaSet',        'pro',        'annual',  1199.99,'active',    '2024-02-05'),
    (19, 'tenant-019', 'StreamVault',       'enterprise', 'monthly', 599.99, 'active',    '2024-02-07'),
    (20, 'tenant-020', 'TerraOps',          'startup',    'monthly', 49.99,  'suspended', '2024-02-09'),
    (21, 'tenant-021', 'UniStack',          'pro',        'monthly', 149.99, 'active',    '2024-02-11'),
    (22, 'tenant-022', 'VortexAI',          'enterprise', 'annual',  499.99, 'active',    '2024-02-13'),
    (23, 'tenant-023', 'WarpDrive',         'startup',    'annual',  399.99, 'active',    '2024-02-15'),
    (24, 'tenant-024', 'XenonCloud',        'pro',        'monthly', 149.99, 'active',    '2024-02-17'),
    (25, 'tenant-025', 'YieldMetrics',      'enterprise', 'annual',  499.99, 'active',    '2024-02-19'),
    (26, 'tenant-026', 'ZeroLatency',       'startup',    'monthly', 49.99,  'trial',     '2024-02-21'),
    (27, 'tenant-027', 'AlphaEdge',         'pro',        'annual',  1199.99,'active',    '2024-02-23'),
    (28, 'tenant-028', 'BrightSignal',      'enterprise', 'monthly', 599.99, 'active',    '2024-02-25'),
    (29, 'tenant-029', 'CoreMetrics',       'startup',    'monthly', 49.99,  'active',    '2024-02-27'),
    (30, 'tenant-030', 'DeltaForge SaaS',   'pro',        'monthly', 149.99, 'active',    '2024-03-01');
