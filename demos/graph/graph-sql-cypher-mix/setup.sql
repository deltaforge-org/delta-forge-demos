-- ============================================================================
-- Sales Territory Optimization — SQL + Cypher Interoperability Setup
-- ============================================================================
-- Creates 4 data tables (customers, referrals, orders, sales_reps),
-- 2 empty working tables (influence_scores, community_assignments),
-- and 1 directed graph definition (customer_network).
--
-- Data model:
--   40 enterprise customers with region/industry/tier attributes
--   96 directed referral edges (partner, vendor, peer, subsidiary)
--   120 orders (3 per customer, deterministic amounts)
--   8 sales reps (2 per region)
--
-- Graph: customer_network — customers as vertices, referrals as edges
-- ============================================================================

-- ############################################################################
-- STEP 1: Zone & Schema
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.customer_network
    COMMENT 'Customer network — SQL/Cypher interop with referral graph';

-- ############################################################################
-- STEP 2: Customers (Vertex Table) — 40 enterprise customers
-- ############################################################################
-- id%10 → name prefix: Acme(0), Bolt(1), Cipher(2), DataFlow(3), Echo(4),
--                       Forge(5), Grid(6), Helix(7), Ion(8), Jet(9)
-- id%4  → suffix: Corp(0), Inc(1), Ltd(2), Labs(3)
-- id%4  → region: North(0), South(1), East(2), West(3)
-- id%5  → industry: Tech(0), Finance(1), Healthcare(2), Retail(3), Manufacturing(4)
-- tier: id%10=0 → Enterprise, id%5=0 → Premium, else → Standard
-- ############################################################################

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.customer_network.customers (
    id         BIGINT,
    name       STRING,
    region     STRING,
    industry   STRING,
    tier       STRING,
    annual_contract INT
) LOCATION 'graph-sql-cypher-mix/sales/customers';


INSERT INTO {{zone_name}}.customer_network.customers VALUES
    ( 1, 'Bolt_Inc',        'South', 'Finance',       'Standard',   10500),
    ( 2, 'Cipher_Ltd',      'East',  'Healthcare',    'Standard',   11000),
    ( 3, 'DataFlow_Labs',   'West',  'Retail',        'Standard',   11500),
    ( 4, 'Echo_Corp',       'North', 'Manufacturing', 'Standard',   12000),
    ( 5, 'Forge_Inc',       'South', 'Tech',          'Premium',    50000),
    ( 6, 'Grid_Ltd',        'East',  'Finance',       'Standard',   13000),
    ( 7, 'Helix_Labs',      'West',  'Healthcare',    'Standard',   13500),
    ( 8, 'Ion_Corp',        'North', 'Retail',        'Standard',   14000),
    ( 9, 'Jet_Inc',         'South', 'Manufacturing', 'Standard',   14500),
    (10, 'Acme_Ltd',        'East',  'Tech',          'Enterprise', 100000),
    (11, 'Bolt_Labs',       'West',  'Finance',       'Standard',   15500),
    (12, 'Cipher_Corp',     'North', 'Healthcare',    'Standard',   16000),
    (13, 'DataFlow_Inc',    'South', 'Retail',        'Standard',   16500),
    (14, 'Echo_Ltd',        'East',  'Manufacturing', 'Standard',   17000),
    (15, 'Forge_Labs',      'West',  'Tech',          'Premium',    50000),
    (16, 'Grid_Corp',       'North', 'Finance',       'Standard',   18000),
    (17, 'Helix_Inc',       'South', 'Healthcare',    'Standard',   18500),
    (18, 'Ion_Ltd',         'East',  'Retail',        'Standard',   19000),
    (19, 'Jet_Labs',        'West',  'Manufacturing', 'Standard',   19500),
    (20, 'Acme_Corp',       'North', 'Tech',          'Enterprise', 100000),
    (21, 'Bolt_Inc',        'South', 'Finance',       'Standard',   20500),
    (22, 'Cipher_Ltd',      'East',  'Healthcare',    'Standard',   21000),
    (23, 'DataFlow_Labs',   'West',  'Retail',        'Standard',   21500),
    (24, 'Echo_Corp',       'North', 'Manufacturing', 'Standard',   22000),
    (25, 'Forge_Inc',       'South', 'Tech',          'Premium',    50000),
    (26, 'Grid_Ltd',        'East',  'Finance',       'Standard',   23000),
    (27, 'Helix_Labs',      'West',  'Healthcare',    'Standard',   23500),
    (28, 'Ion_Corp',        'North', 'Retail',        'Standard',   24000),
    (29, 'Jet_Inc',         'South', 'Manufacturing', 'Standard',   24500),
    (30, 'Acme_Ltd',        'East',  'Tech',          'Enterprise', 100000),
    (31, 'Bolt_Labs',       'West',  'Finance',       'Standard',   25500),
    (32, 'Cipher_Corp',     'North', 'Healthcare',    'Standard',   26000),
    (33, 'DataFlow_Inc',    'South', 'Retail',        'Standard',   26500),
    (34, 'Echo_Ltd',        'East',  'Manufacturing', 'Standard',   27000),
    (35, 'Forge_Labs',      'West',  'Tech',          'Premium',    50000),
    (36, 'Grid_Corp',       'North', 'Finance',       'Standard',   28000),
    (37, 'Helix_Inc',       'South', 'Healthcare',    'Standard',   28500),
    (38, 'Ion_Ltd',         'East',  'Retail',        'Standard',   29000),
    (39, 'Jet_Labs',        'West',  'Manufacturing', 'Standard',   29500),
    (40, 'Acme_Corp',       'North', 'Tech',          'Enterprise', 100000);

-- ############################################################################
-- STEP 3: Referrals (Edge Table) — 96 directed referral edges
-- ############################################################################
-- Batch 1: Regional partnerships (same region, stride 4) — 36 edges
-- Batch 2: Industry cross-referrals (same industry, stride 5) — 35 edges
-- Batch 3: Strategic alliances (prime scatter, cross-region) — 25 edges
-- ############################################################################

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.customer_network.referrals (
    id             BIGINT,
    src            BIGINT,
    dst            BIGINT,
    weight         DOUBLE,
    referral_type  STRING,
    year_established INT
) LOCATION 'graph-sql-cypher-mix/sales/referrals';


-- Batch 1: Regional partnerships (stride 4, same region)
INSERT INTO {{zone_name}}.customer_network.referrals VALUES
    ( 1,  1,  5, 0.6, 'partner', 2019),
    ( 2,  2,  6, 0.7, 'partner', 2020),
    ( 3,  3,  7, 0.8, 'partner', 2021),
    ( 4,  4,  8, 0.9, 'partner', 2022),
    ( 5,  5,  9, 1.0, 'partner', 2023),
    ( 6,  6, 10, 0.5, 'partner', 2018),
    ( 7,  7, 11, 0.6, 'partner', 2019),
    ( 8,  8, 12, 0.7, 'partner', 2020),
    ( 9,  9, 13, 0.8, 'partner', 2021),
    (10, 10, 14, 0.9, 'partner', 2022),
    (11, 11, 15, 1.0, 'partner', 2023),
    (12, 12, 16, 0.5, 'partner', 2018),
    (13, 13, 17, 0.6, 'partner', 2019),
    (14, 14, 18, 0.7, 'partner', 2020),
    (15, 15, 19, 0.8, 'partner', 2021),
    (16, 16, 20, 0.9, 'partner', 2022),
    (17, 17, 21, 1.0, 'partner', 2023),
    (18, 18, 22, 0.5, 'partner', 2018),
    (19, 19, 23, 0.6, 'partner', 2019),
    (20, 20, 24, 0.7, 'partner', 2020),
    (21, 21, 25, 0.8, 'partner', 2021),
    (22, 22, 26, 0.9, 'partner', 2022),
    (23, 23, 27, 1.0, 'partner', 2023),
    (24, 24, 28, 0.5, 'partner', 2018),
    (25, 25, 29, 0.6, 'partner', 2019),
    (26, 26, 30, 0.7, 'partner', 2020),
    (27, 27, 31, 0.8, 'partner', 2021),
    (28, 28, 32, 0.9, 'partner', 2022),
    (29, 29, 33, 1.0, 'partner', 2023),
    (30, 30, 34, 0.5, 'partner', 2018),
    (31, 31, 35, 0.6, 'partner', 2019),
    (32, 32, 36, 0.7, 'partner', 2020),
    (33, 33, 37, 0.8, 'partner', 2021),
    (34, 34, 38, 0.9, 'partner', 2022),
    (35, 35, 39, 1.0, 'partner', 2023),
    (36, 36, 40, 0.5, 'partner', 2018);

-- Batch 2: Industry cross-referrals (stride 5, same industry)
INSERT INTO {{zone_name}}.customer_network.referrals VALUES
    (37,  1,  6, 0.4, 'peer',   2020),
    (38,  2,  7, 0.5, 'vendor', 2021),
    (39,  3,  8, 0.6, 'peer',   2022),
    (40,  4,  9, 0.7, 'vendor', 2023),
    (41,  5, 10, 0.8, 'peer',   2024),
    (42,  6, 11, 0.9, 'vendor', 2019),
    (43,  7, 12, 1.0, 'peer',   2020),
    (44,  8, 13, 0.3, 'vendor', 2021),
    (45,  9, 14, 0.4, 'peer',   2022),
    (46, 10, 15, 0.5, 'vendor', 2023),
    (47, 11, 16, 0.6, 'peer',   2024),
    (48, 12, 17, 0.7, 'vendor', 2019),
    (49, 13, 18, 0.8, 'peer',   2020),
    (50, 14, 19, 0.9, 'vendor', 2021),
    (51, 15, 20, 1.0, 'peer',   2022),
    (52, 16, 21, 0.3, 'vendor', 2023),
    (53, 17, 22, 0.4, 'peer',   2024),
    (54, 18, 23, 0.5, 'vendor', 2019),
    (55, 19, 24, 0.6, 'peer',   2020),
    (56, 20, 25, 0.7, 'vendor', 2021),
    (57, 21, 26, 0.8, 'peer',   2022),
    (58, 22, 27, 0.9, 'vendor', 2023),
    (59, 23, 28, 1.0, 'peer',   2024),
    (60, 24, 29, 0.3, 'vendor', 2019),
    (61, 25, 30, 0.4, 'peer',   2020),
    (62, 26, 31, 0.5, 'vendor', 2021),
    (63, 27, 32, 0.6, 'peer',   2022),
    (64, 28, 33, 0.7, 'vendor', 2023),
    (65, 29, 34, 0.8, 'peer',   2024),
    (66, 30, 35, 0.9, 'vendor', 2019),
    (67, 31, 36, 1.0, 'peer',   2020),
    (68, 32, 37, 0.3, 'vendor', 2021),
    (69, 33, 38, 0.4, 'peer',   2022),
    (70, 34, 39, 0.5, 'vendor', 2023),
    (71, 35, 40, 0.6, 'peer',   2024);

-- Batch 3: Strategic alliances (prime scatter, cross-region)
INSERT INTO {{zone_name}}.customer_network.referrals VALUES
    (72,  1,  3, 0.3, 'partner',    2021),
    (73,  2,  5, 0.4, 'peer',       2022),
    (74,  3,  2, 0.5, 'subsidiary', 2023),
    (75,  4,  1, 0.6, 'partner',    2024),
    (76,  5,  3, 0.7, 'peer',       2020),
    (77,  6,  5, 0.8, 'subsidiary', 2021),
    (78,  7,  2, 0.9, 'partner',    2022),
    (79,  8,  7, 1.0, 'peer',       2023),
    (80,  9,  6, 0.1, 'subsidiary', 2024),
    (81, 10, 13, 0.2, 'partner',    2020),
    (82, 11, 14, 0.3, 'peer',       2021),
    (83, 12, 11, 0.4, 'subsidiary', 2022),
    (84, 13, 16, 0.5, 'partner',    2023),
    (85, 14, 13, 0.6, 'peer',       2024),
    (86, 15, 14, 0.7, 'subsidiary', 2020),
    (87, 16, 17, 0.8, 'partner',    2021),
    (88, 17, 20, 0.9, 'peer',       2022),
    (89, 18, 21, 1.0, 'subsidiary', 2023),
    (90, 19, 22, 0.1, 'partner',    2024),
    (91, 20, 19, 0.2, 'peer',       2020),
    (92, 21, 24, 0.3, 'subsidiary', 2021),
    (93, 22, 21, 0.4, 'partner',    2022),
    (94, 23, 22, 0.5, 'peer',       2023),
    (95, 24, 23, 0.6, 'subsidiary', 2024),
    (96, 25, 24, 0.7, 'partner',    2020);

-- ############################################################################
-- STEP 4: Orders — 120 orders (3 per customer)
-- ############################################################################
-- Deterministic amounts: 5000 + (order_id * 373 + pass * 7919) % 45001
-- Products cycle: Platform, Analytics, Security, Integration
-- Quarters cycle: Q1_2024, Q2_2024, Q3_2024, Q4_2024
-- ############################################################################

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.customer_network.orders (
    order_id    BIGINT,
    customer_id BIGINT,
    amount      DOUBLE,
    product     STRING,
    quarter     STRING
) LOCATION 'graph-sql-cypher-mix/sales/orders';


-- Pass 1: orders 1-40 (one per customer)
INSERT INTO {{zone_name}}.customer_network.orders VALUES
    (  1,  1,  5373, 'Analytics',    'Q2_2024'),
    (  2,  2,  5746, 'Security',     'Q3_2024'),
    (  3,  3,  6119, 'Integration',  'Q4_2024'),
    (  4,  4,  6492, 'Platform',     'Q1_2024'),
    (  5,  5,  6865, 'Analytics',    'Q2_2024'),
    (  6,  6,  7238, 'Security',     'Q3_2024'),
    (  7,  7,  7611, 'Integration',  'Q4_2024'),
    (  8,  8,  7984, 'Platform',     'Q1_2024'),
    (  9,  9,  8357, 'Analytics',    'Q2_2024'),
    ( 10, 10,  8730, 'Security',     'Q3_2024'),
    ( 11, 11,  9103, 'Integration',  'Q4_2024'),
    ( 12, 12,  9476, 'Platform',     'Q1_2024'),
    ( 13, 13,  9849, 'Analytics',    'Q2_2024'),
    ( 14, 14, 10222, 'Security',     'Q3_2024'),
    ( 15, 15, 10595, 'Integration',  'Q4_2024'),
    ( 16, 16, 10968, 'Platform',     'Q1_2024'),
    ( 17, 17, 11341, 'Analytics',    'Q2_2024'),
    ( 18, 18, 11714, 'Security',     'Q3_2024'),
    ( 19, 19, 12087, 'Integration',  'Q4_2024'),
    ( 20, 20, 12460, 'Platform',     'Q1_2024'),
    ( 21, 21, 12833, 'Analytics',    'Q2_2024'),
    ( 22, 22, 13206, 'Security',     'Q3_2024'),
    ( 23, 23, 13579, 'Integration',  'Q4_2024'),
    ( 24, 24, 13952, 'Platform',     'Q1_2024'),
    ( 25, 25, 14325, 'Analytics',    'Q2_2024'),
    ( 26, 26, 14698, 'Security',     'Q3_2024'),
    ( 27, 27, 15071, 'Integration',  'Q4_2024'),
    ( 28, 28, 15444, 'Platform',     'Q1_2024'),
    ( 29, 29, 15817, 'Analytics',    'Q2_2024'),
    ( 30, 30, 16190, 'Security',     'Q3_2024'),
    ( 31, 31, 16563, 'Integration',  'Q4_2024'),
    ( 32, 32, 16936, 'Platform',     'Q1_2024'),
    ( 33, 33, 17309, 'Analytics',    'Q2_2024'),
    ( 34, 34, 17682, 'Security',     'Q3_2024'),
    ( 35, 35, 18055, 'Integration',  'Q4_2024'),
    ( 36, 36, 18428, 'Platform',     'Q1_2024'),
    ( 37, 37, 18801, 'Analytics',    'Q2_2024'),
    ( 38, 38, 19174, 'Security',     'Q3_2024'),
    ( 39, 39, 19547, 'Integration',  'Q4_2024'),
    ( 40, 40, 19920, 'Platform',     'Q1_2024');

-- Pass 2: orders 41-80 (one per customer)
INSERT INTO {{zone_name}}.customer_network.orders VALUES
    ( 41,  1, 28212, 'Analytics',    'Q2_2024'),
    ( 42,  2, 28585, 'Security',     'Q3_2024'),
    ( 43,  3, 28958, 'Integration',  'Q4_2024'),
    ( 44,  4, 29331, 'Platform',     'Q1_2024'),
    ( 45,  5, 29704, 'Analytics',    'Q2_2024'),
    ( 46,  6, 30077, 'Security',     'Q3_2024'),
    ( 47,  7, 30450, 'Integration',  'Q4_2024'),
    ( 48,  8, 30823, 'Platform',     'Q1_2024'),
    ( 49,  9, 31196, 'Analytics',    'Q2_2024'),
    ( 50, 10, 31569, 'Security',     'Q3_2024'),
    ( 51, 11, 31942, 'Integration',  'Q4_2024'),
    ( 52, 12, 32315, 'Platform',     'Q1_2024'),
    ( 53, 13, 32688, 'Analytics',    'Q2_2024'),
    ( 54, 14, 33061, 'Security',     'Q3_2024'),
    ( 55, 15, 33434, 'Integration',  'Q4_2024'),
    ( 56, 16, 33807, 'Platform',     'Q1_2024'),
    ( 57, 17, 34180, 'Analytics',    'Q2_2024'),
    ( 58, 18, 34553, 'Security',     'Q3_2024'),
    ( 59, 19, 34926, 'Integration',  'Q4_2024'),
    ( 60, 20, 35299, 'Platform',     'Q1_2024'),
    ( 61, 21, 35672, 'Analytics',    'Q2_2024'),
    ( 62, 22, 36045, 'Security',     'Q3_2024'),
    ( 63, 23, 36418, 'Integration',  'Q4_2024'),
    ( 64, 24, 36791, 'Platform',     'Q1_2024'),
    ( 65, 25, 37164, 'Analytics',    'Q2_2024'),
    ( 66, 26, 37537, 'Security',     'Q3_2024'),
    ( 67, 27, 37910, 'Integration',  'Q4_2024'),
    ( 68, 28, 38283, 'Platform',     'Q1_2024'),
    ( 69, 29, 38656, 'Analytics',    'Q2_2024'),
    ( 70, 30, 39029, 'Security',     'Q3_2024'),
    ( 71, 31, 39402, 'Integration',  'Q4_2024'),
    ( 72, 32, 39775, 'Platform',     'Q1_2024'),
    ( 73, 33, 40148, 'Analytics',    'Q2_2024'),
    ( 74, 34, 40521, 'Security',     'Q3_2024'),
    ( 75, 35, 40894, 'Integration',  'Q4_2024'),
    ( 76, 36, 41267, 'Platform',     'Q1_2024'),
    ( 77, 37, 41640, 'Analytics',    'Q2_2024'),
    ( 78, 38, 42013, 'Security',     'Q3_2024'),
    ( 79, 39, 42386, 'Integration',  'Q4_2024'),
    ( 80, 40, 42759, 'Platform',     'Q1_2024');

-- Pass 3: orders 81-120 (one per customer)
INSERT INTO {{zone_name}}.customer_network.orders VALUES
    ( 81,  1,  6050, 'Analytics',    'Q2_2024'),
    ( 82,  2,  6423, 'Security',     'Q3_2024'),
    ( 83,  3,  6796, 'Integration',  'Q4_2024'),
    ( 84,  4,  7169, 'Platform',     'Q1_2024'),
    ( 85,  5,  7542, 'Analytics',    'Q2_2024'),
    ( 86,  6,  7915, 'Security',     'Q3_2024'),
    ( 87,  7,  8288, 'Integration',  'Q4_2024'),
    ( 88,  8,  8661, 'Platform',     'Q1_2024'),
    ( 89,  9,  9034, 'Analytics',    'Q2_2024'),
    ( 90, 10,  9407, 'Security',     'Q3_2024'),
    ( 91, 11,  9780, 'Integration',  'Q4_2024'),
    ( 92, 12, 10153, 'Platform',     'Q1_2024'),
    ( 93, 13, 10526, 'Analytics',    'Q2_2024'),
    ( 94, 14, 10899, 'Security',     'Q3_2024'),
    ( 95, 15, 11272, 'Integration',  'Q4_2024'),
    ( 96, 16, 11645, 'Platform',     'Q1_2024'),
    ( 97, 17, 12018, 'Analytics',    'Q2_2024'),
    ( 98, 18, 12391, 'Security',     'Q3_2024'),
    ( 99, 19, 12764, 'Integration',  'Q4_2024'),
    (100, 20, 13137, 'Platform',     'Q1_2024'),
    (101, 21, 13510, 'Analytics',    'Q2_2024'),
    (102, 22, 13883, 'Security',     'Q3_2024'),
    (103, 23, 14256, 'Integration',  'Q4_2024'),
    (104, 24, 14629, 'Platform',     'Q1_2024'),
    (105, 25, 15002, 'Analytics',    'Q2_2024'),
    (106, 26, 15375, 'Security',     'Q3_2024'),
    (107, 27, 15748, 'Integration',  'Q4_2024'),
    (108, 28, 16121, 'Platform',     'Q1_2024'),
    (109, 29, 16494, 'Analytics',    'Q2_2024'),
    (110, 30, 16867, 'Security',     'Q3_2024'),
    (111, 31, 17240, 'Integration',  'Q4_2024'),
    (112, 32, 17613, 'Platform',     'Q1_2024'),
    (113, 33, 17986, 'Analytics',    'Q2_2024'),
    (114, 34, 18359, 'Security',     'Q3_2024'),
    (115, 35, 18732, 'Integration',  'Q4_2024'),
    (116, 36, 19105, 'Platform',     'Q1_2024'),
    (117, 37, 19478, 'Analytics',    'Q2_2024'),
    (118, 38, 19851, 'Security',     'Q3_2024'),
    (119, 39, 20224, 'Integration',  'Q4_2024'),
    (120, 40, 20597, 'Platform',     'Q1_2024');

-- ############################################################################
-- STEP 5: Sales Reps — 8 reps, 2 per region
-- ############################################################################

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.customer_network.sales_reps (
    rep_id    BIGINT,
    rep_name  STRING,
    territory STRING,
    quota     INT
) LOCATION 'graph-sql-cypher-mix/sales/sales_reps';


INSERT INTO {{zone_name}}.customer_network.sales_reps VALUES
    (1, 'Alice_Chen',   'North', 200000),
    (2, 'Bob_Kumar',    'North', 228571),
    (3, 'Carol_Davis',  'South', 257142),
    (4, 'Dan_Smith',    'South', 285713),
    (5, 'Eva_Jones',    'East',  314284),
    (6, 'Frank_Lee',    'East',  342855),
    (7, 'Grace_Park',   'West',  371426),
    (8, 'Hank_Wilson',  'West',  399997);

-- ############################################################################
-- STEP 6: Working Tables (created empty, populated by Cypher in queries.sql)
-- ############################################################################

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.customer_network.influence_scores (
    customer_id     BIGINT,
    influence_score DOUBLE,
    influence_rank  BIGINT
) LOCATION 'graph-sql-cypher-mix/sales/influence_scores';


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.customer_network.community_assignments (
    customer_id  BIGINT,
    community_id BIGINT
) LOCATION 'graph-sql-cypher-mix/sales/community_assignments';


-- ############################################################################
-- STEP 6b: Physical Layout — Z-ORDER for fast data skipping
-- ############################################################################
-- The data was inserted in id-generation order, which has reasonable locality
-- for `id` but scatters frequent filter columns (region, industry) across
-- files.  Z-ORDER rewrites files so rows with similar values on the ordering
-- keys co-locate, giving Parquet min/max statistics much tighter ranges per
-- file.  This benefits three hot paths:
--
--   1. CSR build from the referrals table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `id` co-location lets the Parquet reader skip
--      almost every row group for targeted customer lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE c.region = 'EMEA' AND c.industry = 'Finance'` skip entire
--      files instead of reading the whole customers table.
--
-- One-time cost at setup; every subsequent query benefits.  Orders and
-- sales_reps are SQL-only tables; they don't participate in the graph and
-- are not ZORDER'd.

OPTIMIZE {{zone_name}}.customer_network.customers
    ZORDER BY (id, region, industry);

OPTIMIZE {{zone_name}}.customer_network.referrals
    ZORDER BY (src, dst);

-- ############################################################################
-- STEP 7: Graph Definition
-- ############################################################################
-- Directed graph: customers are vertices, referrals are edges.
-- WEIGHT COLUMN enables weighted PageRank and community detection.
-- EDGE TYPE COLUMN enables filtering by referral_type in Cypher.
-- ############################################################################

CREATE GRAPH IF NOT EXISTS {{zone_name}}.customer_network.customer_network
    VERTEX TABLE {{zone_name}}.customer_network.customers ID COLUMN id NODE TYPE COLUMN region NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.customer_network.referrals SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN referral_type
    DIRECTED;

-- ############################################################################
-- STEP 8: Warm the CSR cache
-- ############################################################################
-- CREATE GRAPHCSR pre-builds the Compressed Sparse Row topology and writes
-- it to disk as a .dcsr file. The first Cypher query then loads in ~200 ms
-- instead of rebuilding from Delta tables. Safe to re-run after bulk edge
-- loads to refresh the cache.

CREATE GRAPHCSR {{zone_name}}.customer_network.customer_network;
