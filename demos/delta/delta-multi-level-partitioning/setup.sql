-- ============================================================================
-- Delta Multi-Level Partitioning — Setup Script
-- ============================================================================
-- Demonstrates compound partition pruning with two partition columns.
-- A single-level partition (BY region) creates 4 directories.
-- A multi-level partition (BY region, quarter) creates 16 directories,
-- enabling much more aggressive pruning.
--
-- Tables:
--   1. sales_ml     — 60 rows, PARTITIONED BY (region, quarter) → 16 partitions
--   2. region_dim   — 4 rows (region dimension with targets)
--   3. quarter_dim  — 4 rows (quarter dimension with budgets)
--
-- On disk, Delta creates a nested directory structure:
--   region=us-east/quarter=Q1-2024/   (4 rows)
--   region=us-east/quarter=Q2-2024/   (4 rows)
--   ...
--   region=ap-south/quarter=Q4-2024/  (3 rows)
--   → 16 partition directories total, 60 rows
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: sales_ml — 60 sales, PARTITIONED BY (region, quarter)
-- ============================================================================
-- Same sales data as the single-level demo, but partitioned on TWO columns.
-- This creates 4 regions × 4 quarters = 16 partition directories.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sales_ml (
    id          INT,
    product_id  INT,
    region      VARCHAR,
    quarter     VARCHAR,
    amount      DOUBLE,
    qty         INT,
    channel     VARCHAR,
    sale_date   VARCHAR
) LOCATION 'delta-multi-level-partitioning/sales_ml'
PARTITIONED BY (region, quarter);


-- us-east region: ids 1-15 (Q1: 4 rows, Q2: 4 rows, Q3: 4 rows, Q4: 3 rows)
INSERT INTO {{zone_name}}.delta_demos.sales_ml VALUES
    (1,  101, 'us-east', 'Q1-2024', 1200.00, 10, 'online',    '2024-01-05'),
    (2,  102, 'us-east', 'Q1-2024', 850.50,  5,  'retail',    '2024-01-12'),
    (3,  103, 'us-east', 'Q1-2024', 2300.00, 20, 'wholesale', '2024-01-19'),
    (4,  104, 'us-east', 'Q1-2024', 475.00,  3,  'online',    '2024-01-26'),
    (5,  105, 'us-east', 'Q2-2024', 320.00,  8,  'retail',    '2024-04-03'),
    (6,  106, 'us-east', 'Q2-2024', 1550.00, 12, 'wholesale', '2024-04-10'),
    (7,  107, 'us-east', 'Q2-2024', 690.00,  7,  'online',    '2024-04-17'),
    (8,  108, 'us-east', 'Q2-2024', 3100.00, 25, 'retail',    '2024-04-24'),
    (9,  109, 'us-east', 'Q3-2024', 410.00,  2,  'wholesale', '2024-07-05'),
    (10, 110, 'us-east', 'Q3-2024', 1875.00, 15, 'online',    '2024-07-12'),
    (11, 111, 'us-east', 'Q3-2024', 560.00,  4,  'retail',    '2024-07-19'),
    (12, 112, 'us-east', 'Q3-2024', 2200.00, 18, 'wholesale', '2024-07-26'),
    (13, 113, 'us-east', 'Q4-2024', 995.00,  8,  'online',    '2024-10-05'),
    (14, 114, 'us-east', 'Q4-2024', 1430.00, 11, 'retail',    '2024-10-12'),
    (15, 115, 'us-east', 'Q4-2024', 780.00,  6,  'wholesale', '2024-10-19');

-- us-west region: ids 16-30
INSERT INTO {{zone_name}}.delta_demos.sales_ml VALUES
    (16, 201, 'us-west', 'Q1-2024', 1450.00, 12, 'online',    '2024-01-08'),
    (17, 202, 'us-west', 'Q1-2024', 920.00,  8,  'retail',    '2024-01-15'),
    (18, 203, 'us-west', 'Q1-2024', 3400.00, 30, 'wholesale', '2024-01-22'),
    (19, 204, 'us-west', 'Q1-2024', 610.00,  5,  'online',    '2024-01-29'),
    (20, 205, 'us-west', 'Q2-2024', 250.00,  3,  'retail',    '2024-04-06'),
    (21, 206, 'us-west', 'Q2-2024', 1780.00, 14, 'wholesale', '2024-04-13'),
    (22, 207, 'us-west', 'Q2-2024', 830.00,  6,  'online',    '2024-04-20'),
    (23, 208, 'us-west', 'Q2-2024', 2650.00, 22, 'retail',    '2024-04-27'),
    (24, 209, 'us-west', 'Q3-2024', 540.00,  4,  'wholesale', '2024-07-08'),
    (25, 210, 'us-west', 'Q3-2024', 1990.00, 16, 'online',    '2024-07-15'),
    (26, 211, 'us-west', 'Q3-2024', 720.00,  5,  'retail',    '2024-07-22'),
    (27, 212, 'us-west', 'Q3-2024', 2850.00, 24, 'wholesale', '2024-07-29'),
    (28, 213, 'us-west', 'Q4-2024', 1100.00, 9,  'online',    '2024-10-08'),
    (29, 214, 'us-west', 'Q4-2024', 1650.00, 13, 'retail',    '2024-10-15'),
    (30, 215, 'us-west', 'Q4-2024', 890.00,  7,  'wholesale', '2024-10-22');

-- eu-west region: ids 31-45
INSERT INTO {{zone_name}}.delta_demos.sales_ml VALUES
    (31, 301, 'eu-west', 'Q1-2024', 980.00,  8,  'online',    '2024-01-10'),
    (32, 302, 'eu-west', 'Q1-2024', 1340.00, 11, 'retail',    '2024-01-17'),
    (33, 303, 'eu-west', 'Q1-2024', 2750.00, 23, 'wholesale', '2024-01-24'),
    (34, 304, 'eu-west', 'Q1-2024', 415.00,  3,  'online',    '2024-01-31'),
    (35, 305, 'eu-west', 'Q2-2024', 190.00,  2,  'retail',    '2024-04-09'),
    (36, 306, 'eu-west', 'Q2-2024', 1620.00, 13, 'wholesale', '2024-04-16'),
    (37, 307, 'eu-west', 'Q2-2024', 750.00,  6,  'online',    '2024-04-23'),
    (38, 308, 'eu-west', 'Q2-2024', 2400.00, 19, 'retail',    '2024-04-30'),
    (39, 309, 'eu-west', 'Q3-2024', 380.00,  3,  'wholesale', '2024-07-10'),
    (40, 310, 'eu-west', 'Q3-2024', 1700.00, 14, 'online',    '2024-07-17'),
    (41, 311, 'eu-west', 'Q3-2024', 630.00,  5,  'retail',    '2024-07-24'),
    (42, 312, 'eu-west', 'Q3-2024', 2100.00, 17, 'wholesale', '2024-07-31'),
    (43, 313, 'eu-west', 'Q4-2024', 870.00,  7,  'online',    '2024-10-10'),
    (44, 314, 'eu-west', 'Q4-2024', 1520.00, 12, 'retail',    '2024-10-17'),
    (45, 315, 'eu-west', 'Q4-2024', 950.00,  8,  'wholesale', '2024-10-24');

-- ap-south region: ids 46-60
INSERT INTO {{zone_name}}.delta_demos.sales_ml VALUES
    (46, 401, 'ap-south', 'Q1-2024', 520.00,  4,  'online',    '2024-01-11'),
    (47, 402, 'ap-south', 'Q1-2024', 1180.00, 9,  'retail',    '2024-01-18'),
    (48, 403, 'ap-south', 'Q1-2024', 2900.00, 25, 'wholesale', '2024-01-25'),
    (49, 404, 'ap-south', 'Q1-2024', 350.00,  2,  'online',    '2024-02-01'),
    (50, 405, 'ap-south', 'Q2-2024', 280.00,  3,  'retail',    '2024-04-11'),
    (51, 406, 'ap-south', 'Q2-2024', 1400.00, 11, 'wholesale', '2024-04-18'),
    (52, 407, 'ap-south', 'Q2-2024', 660.00,  5,  'online',    '2024-04-25'),
    (53, 408, 'ap-south', 'Q2-2024', 2050.00, 17, 'retail',    '2024-05-02'),
    (54, 409, 'ap-south', 'Q3-2024', 430.00,  3,  'wholesale', '2024-07-11'),
    (55, 410, 'ap-south', 'Q3-2024', 1560.00, 12, 'online',    '2024-07-18'),
    (56, 411, 'ap-south', 'Q3-2024', 590.00,  4,  'retail',    '2024-07-25'),
    (57, 412, 'ap-south', 'Q3-2024', 1850.00, 15, 'wholesale', '2024-08-01'),
    (58, 413, 'ap-south', 'Q4-2024', 740.00,  6,  'online',    '2024-10-11'),
    (59, 414, 'ap-south', 'Q4-2024', 1290.00, 10, 'retail',    '2024-10-18'),
    (60, 415, 'ap-south', 'Q4-2024', 810.00,  7,  'wholesale', '2024-10-25');


-- ============================================================================
-- TABLE: region_dim — Region dimension (4 rows)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.region_dim (
    region        VARCHAR,
    target_amount DOUBLE,
    target_qty    INT,
    market        VARCHAR
) LOCATION 'delta-multi-level-partitioning/region_dim';


INSERT INTO {{zone_name}}.delta_demos.region_dim VALUES
    ('us-east',  75000.00, 500, 'domestic'),
    ('us-west',  60000.00, 400, 'domestic'),
    ('eu-west',  45000.00, 350, 'international'),
    ('ap-south', 30000.00, 250, 'international');


-- ============================================================================
-- TABLE: quarter_dim — Quarter dimension (4 rows)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.quarter_dim (
    quarter    VARCHAR,
    budget     DOUBLE,
    target_qty INT,
    half_year  VARCHAR
) LOCATION 'delta-multi-level-partitioning/quarter_dim';


INSERT INTO {{zone_name}}.delta_demos.quarter_dim VALUES
    ('Q1-2024', 20000.00, 150, 'H1'),
    ('Q2-2024', 18000.00, 130, 'H1'),
    ('Q3-2024', 15000.00, 110, 'H2'),
    ('Q4-2024', 12000.00, 90,  'H2');
