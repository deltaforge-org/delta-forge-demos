-- ============================================================================
-- Cross-Partition Updates — Setup Script
-- ============================================================================
-- Creates a partitioned Delta table and loads SaaS subscription billing data.
--
-- Tables created:
--   1. subscriptions — 60 rows, partitioned by region (americas, europe, asia-pacific)
--
-- The queries.sql script then demonstrates cross-partition UPDATE (DVs in
-- every partition), partition-aligned UPDATE (DVs in one partition), and
-- OPTIMIZE (compaction merging DVs back into data files).
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: subscriptions — SaaS billing platform subscriptions
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.subscriptions (
    id           INT,
    customer     VARCHAR,
    region       VARCHAR,
    plan         VARCHAR,
    monthly_fee  DECIMAL(10,2),
    usage_gb     INT,
    signup_date  VARCHAR,
    status       VARCHAR
) LOCATION 'subscriptions'
PARTITIONED BY (region)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
);


-- Region 1: americas (20 rows)
INSERT INTO {{zone_name}}.delta_demos.subscriptions VALUES
    ( 1, 'Acme Corp',          'americas', 'starter',      29.99,  50,   '2024-01-15', 'active'),
    ( 2, 'Beta Industries',    'americas', 'professional', 99.99,  250,  '2024-02-01', 'active'),
    ( 3, 'Cloud Nine LLC',     'americas', 'enterprise',   299.99, 2000, '2023-06-10', 'active'),
    ( 4, 'DataStream Inc',     'americas', 'starter',      39.99,  150,  '2024-03-20', 'trial'),
    ( 5, 'Eagle Software',     'americas', 'professional', 129.99, 400,  '2023-11-05', 'active'),
    ( 6, 'FreshBooks Co',      'americas', 'starter',      49.99,  120,  '2024-04-12', 'active'),
    ( 7, 'GridPower Systems',  'americas', 'enterprise',   399.99, 3500, '2023-03-01', 'active'),
    ( 8, 'HyperScale Labs',    'americas', 'professional', 149.99, 600,  '2023-09-15', 'active'),
    ( 9, 'Infinity Analytics', 'americas', 'starter',      34.99,  30,   '2024-05-01', 'trial'),
    (10, 'JetBridge Tech',     'americas', 'professional', 109.99, 320,  '2024-01-22', 'active'),
    (11, 'Keystone Data',      'americas', 'enterprise',   449.99, 4200, '2023-01-10', 'active'),
    (12, 'Lumen Insights',     'americas', 'starter',      44.99,  95,   '2024-06-01', 'active'),
    (13, 'MetaFlow Corp',      'americas', 'professional', 119.99, 180,  '2023-12-18', 'active'),
    (14, 'NexGen Solutions',   'americas', 'starter',      29.99,  15,   '2024-07-10', 'trial'),
    (15, 'Orbit Systems',      'americas', 'enterprise',   349.99, 2800, '2023-05-20', 'active'),
    (16, 'PulseMetrics',       'americas', 'professional', 139.99, 510,  '2024-02-14', 'active'),
    (17, 'QuickLedger Inc',    'americas', 'starter',      39.99,  60,   '2024-08-01', 'active'),
    (18, 'RapidScale AI',      'americas', 'professional', 99.99,  200,  '2023-10-30', 'suspended'),
    (19, 'SkyVault Storage',   'americas', 'enterprise',   499.99, 5000, '2023-02-15', 'active'),
    (20, 'TerraNode Labs',     'americas', 'starter',      34.99,  200,  '2024-09-05', 'trial');

-- Region 2: europe (20 rows)
INSERT INTO {{zone_name}}.delta_demos.subscriptions VALUES
    (21, 'Albion Digital',     'europe', 'starter',      29.99,  40,   '2024-01-20', 'active'),
    (22, 'BerlinTech GmbH',   'europe', 'professional', 119.99, 350,  '2023-08-15', 'active'),
    (23, 'Cypher Security',   'europe', 'enterprise',   349.99, 2500, '2023-04-01', 'active'),
    (24, 'DublinStack Ltd',   'europe', 'starter',      44.99,  110,  '2024-03-10', 'trial'),
    (25, 'EuroCloud SAS',     'europe', 'professional', 139.99, 450,  '2023-11-20', 'active'),
    (26, 'FluxData BV',       'europe', 'starter',      39.99,  70,   '2024-05-15', 'active'),
    (27, 'GenevaLabs SA',     'europe', 'enterprise',   399.99, 3200, '2023-02-28', 'active'),
    (28, 'Helsinki IO Oy',    'europe', 'professional', 109.99, 280,  '2024-01-05', 'active'),
    (29, 'IstanbulOps AS',    'europe', 'starter',      34.99,  55,   '2024-06-20', 'trial'),
    (30, 'JohannesTech AB',   'europe', 'professional', 149.99, 520,  '2023-09-01', 'active'),
    (31, 'KrakowSoft Sp',     'europe', 'enterprise',   449.99, 4000, '2023-01-15', 'active'),
    (32, 'LisbonAI Lda',      'europe', 'starter',      49.99,  130,  '2024-07-01', 'active'),
    (33, 'MadridFlow SL',     'europe', 'professional', 129.99, 380,  '2024-02-10', 'active'),
    (34, 'NordicEdge AS',     'europe', 'starter',      29.99,  20,   '2024-08-15', 'trial'),
    (35, 'OsloMetrics AS',    'europe', 'enterprise',   299.99, 1800, '2023-06-25', 'active'),
    (36, 'PragueData sro',    'europe', 'professional', 99.99,  150,  '2023-12-01', 'suspended'),
    (37, 'RigaStack SIA',     'europe', 'starter',      44.99,  85,   '2024-04-20', 'active'),
    (38, 'SofiaLabs EOOD',    'europe', 'professional', 109.99, 220,  '2024-03-15', 'active'),
    (39, 'TallinnOps OU',     'europe', 'enterprise',   349.99, 2600, '2023-07-10', 'active'),
    (40, 'UppsalaAI AB',      'europe', 'starter',      39.99,  180,  '2024-09-01', 'trial');

-- Region 3: asia-pacific (20 rows)
INSERT INTO {{zone_name}}.delta_demos.subscriptions VALUES
    (41, 'AsiaPay Ltd',       'asia-pacific', 'starter',      34.99,  60,   '2024-02-01', 'active'),
    (42, 'BangkokCloud Co',   'asia-pacific', 'professional', 99.99,  200,  '2023-10-15', 'active'),
    (43, 'ChennaiByte Pvt',   'asia-pacific', 'enterprise',   299.99, 1900, '2023-05-01', 'active'),
    (44, 'DelhiScale Pvt',    'asia-pacific', 'starter',      29.99,  25,   '2024-04-10', 'trial'),
    (45, 'ExcelTech Japan',   'asia-pacific', 'professional', 149.99, 550,  '2023-08-20', 'active'),
    (46, 'FujiData KK',       'asia-pacific', 'starter',      44.99,  100,  '2024-06-15', 'active'),
    (47, 'GuangzhouOps',      'asia-pacific', 'enterprise',   449.99, 4500, '2023-01-20', 'active'),
    (48, 'HanoiStack JSC',    'asia-pacific', 'professional', 119.99, 300,  '2024-01-10', 'active'),
    (49, 'IndigoLabs Pte',    'asia-pacific', 'starter',      39.99,  250,  '2024-07-20', 'trial'),
    (50, 'JakartaFlow PT',    'asia-pacific', 'professional', 139.99, 480,  '2023-11-10', 'active'),
    (51, 'KualaOps Sdn',      'asia-pacific', 'enterprise',   399.99, 3800, '2023-03-15', 'active'),
    (52, 'LuzonTech Inc',     'asia-pacific', 'starter',      49.99,  140,  '2024-08-01', 'active'),
    (53, 'MumbaiMetrics',     'asia-pacific', 'professional', 129.99, 420,  '2024-02-20', 'active'),
    (54, 'NanjiSoft Ltd',     'asia-pacific', 'starter',      34.99,  35,   '2024-05-25', 'active'),
    (55, 'OsakaPlatform',     'asia-pacific', 'enterprise',   349.99, 2200, '2023-06-05', 'active'),
    (56, 'PerthData Pty',     'asia-pacific', 'professional', 109.99, 260,  '2023-12-20', 'suspended'),
    (57, 'QuezonEdge Inc',    'asia-pacific', 'starter',      29.99,  120,  '2024-09-10', 'trial'),
    (58, 'RangoonAI Ltd',     'asia-pacific', 'professional', 99.99,  160,  '2024-03-05', 'active'),
    (59, 'SingaporeLabs',     'asia-pacific', 'enterprise',   499.99, 4800, '2023-02-01', 'active'),
    (60, 'TokyoVault KK',     'asia-pacific', 'starter',      44.99,  90,   '2024-04-30', 'active');
