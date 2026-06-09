-- ============================================================================
-- Insurance Claim Classification — Setup Script
-- ============================================================================
-- Creates an insurance_claims table with 35 rows of realistic claim data.
-- Mix of auto/home/health/life claims with varying amounts ($500-$150,000),
-- statuses (approved/denied/pending/under_review), some high fraud scores,
-- and NULL values for unassigned adjusters and unscored fraud.
--
-- Counts:  auto=11, home=9, health=9, life=6
-- Status:  approved=20, denied=5, pending=5, under_review=5
-- NULLs:   adjuster_id=7, fraud_score=4
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables — insurance claim classification demo';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';
-- ============================================================================
-- TABLE: insurance_claims — 35 rows
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.insurance_claims (
    claim_id       INT,
    policy_holder  VARCHAR,
    claim_type     VARCHAR,
    claim_amount   DOUBLE,
    deductible     DOUBLE,
    incident_date  DATE,
    filed_date     DATE,
    status         VARCHAR,
    adjuster_id    INT,
    fraud_score    DOUBLE
) LOCATION 'delta-case-conditional-logic/insurance_claims';

-- Auto claims (11 rows)
INSERT INTO {{zone_name}}.delta_demos.insurance_claims VALUES
    (1,  'Alice Johnson',  'auto',  2500.00,   500.00,  '2024-01-05', '2024-01-07', 'approved',     101,  0.05),
    (4,  'Dan Wilson',     'auto',  12000.00,  1000.00, '2024-01-18', '2024-01-25', 'under_review', 104,  0.75),
    (8,  'Hank Taylor',   'auto',  950.00,    250.00,  '2024-02-14', '2024-02-15', 'approved',     103,  0.03),
    (11, 'Karen Hall',    'auto',  4800.00,   750.00,  '2024-03-05', '2024-03-06', 'approved',     101,  0.10),
    (14, 'Noah King',     'auto',  28000.00,  2000.00, '2024-03-20', '2024-03-22', 'approved',     NULL, 0.55),
    (17, 'Quinn Lopez',   'auto',  7500.00,   1000.00, '2024-04-05', '2024-04-06', 'denied',       102,  0.68),
    (21, 'Uma Nelson',    'auto',  18000.00,  1500.00, '2024-04-25', '2024-04-27', 'approved',     101,  0.42),
    (24, 'Xander Bell',   'auto',  550.00,    250.00,  '2024-05-10', '2024-05-10', 'approved',     104,  NULL),
    (27, 'Amy Foster',    'auto',  11000.00,  1000.00, '2024-05-25', '2024-05-26', 'approved',     102,  0.25),
    (30, 'Derek James',   'auto',  1800.00,   500.00,  '2024-06-10', '2024-06-11', 'approved',     104,  0.09),
    (33, 'Gina Stewart',  'auto',  9500.00,   1000.00, '2024-06-25', '2024-06-26', 'approved',     103,  NULL);

-- Home claims (9 rows)
INSERT INTO {{zone_name}}.delta_demos.insurance_claims VALUES
    (2,  'Bob Smith',     'home',  45000.00,  2000.00, '2024-01-10', '2024-01-12', 'approved',     102,  0.12),
    (6,  'Frank Brown',   'home',  8500.00,   1500.00, '2024-02-05', '2024-02-06', 'approved',     101,  0.08),
    (9,  'Irene Clark',   'home',  62000.00,  5000.00, '2024-02-20', '2024-02-28', 'under_review', NULL, 0.71),
    (13, 'Mia Scott',     'home',  500.00,    500.00,  '2024-03-15', '2024-03-16', 'denied',       103,  0.02),
    (18, 'Rita Hill',     'home',  35000.00,  3000.00, '2024-04-10', '2024-04-12', 'approved',     103,  0.30),
    (22, 'Victor Reed',   'home',  9200.00,   1000.00, '2024-05-01', '2024-05-02', 'approved',     102,  0.18),
    (26, 'Zach Perry',    'home',  3100.00,   500.00,  '2024-05-20', '2024-05-21', 'approved',     101,  0.07),
    (29, 'Chloe Ward',    'home',  130000.00, 10000.00,'2024-06-05', '2024-06-15', 'under_review', NULL, 0.78),
    (34, 'Hugo Flores',   'home',  47000.00,  4000.00, '2024-07-01', '2024-07-03', 'approved',     104,  0.33);

-- Health claims (9 rows)
INSERT INTO {{zone_name}}.delta_demos.insurance_claims VALUES
    (3,  'Carol Davis',   'health', 800.00,    200.00,  '2024-01-15', '2024-01-16', 'approved',     103,  NULL),
    (7,  'Grace Lee',     'health', 3200.00,   500.00,  '2024-02-10', '2024-02-11', 'denied',       102,  0.15),
    (12, 'Leo Adams',     'health', 15000.00,  1000.00, '2024-03-10', '2024-03-11', 'approved',     102,  0.22),
    (16, 'Paul Wright',   'health', 1200.00,   300.00,  '2024-04-01', '2024-04-02', 'approved',     101,  NULL),
    (19, 'Sam Young',     'health', 600.00,    150.00,  '2024-04-15', '2024-04-15', 'approved',     104,  0.01),
    (23, 'Wendy Cook',    'health', 42000.00,  2500.00, '2024-05-05', '2024-05-08', 'under_review', 103,  0.73),
    (28, 'Brian Hughes',  'health', 6800.00,   800.00,  '2024-06-01', '2024-06-03', 'pending',      103,  0.50),
    (32, 'Felix Ross',    'health', 22000.00,  1500.00, '2024-06-20', '2024-06-21', 'denied',       102,  0.62),
    (35, 'Iris Diaz',     'health', 700.00,    200.00,  '2024-07-05', '2024-07-05', 'pending',      NULL, 0.04);

-- Life claims (6 rows)
INSERT INTO {{zone_name}}.delta_demos.insurance_claims VALUES
    (5,  'Eve Martinez',   'life', 150000.00, 0.00,    '2024-02-01', '2024-02-03', 'pending',      NULL, 0.82),
    (10, 'Jack White',     'life', 95000.00,  0.00,    '2024-03-01', '2024-03-05', 'pending',      104,  0.45),
    (15, 'Olivia Green',   'life', 120000.00, 0.00,    '2024-03-25', '2024-04-05', 'under_review', 104,  0.88),
    (20, 'Tina Allen',     'life', 75000.00,  0.00,    '2024-04-20', '2024-04-22', 'pending',      NULL, 0.35),
    (25, 'Yara Morgan',    'life', 88000.00,  0.00,    '2024-05-15', '2024-05-18', 'denied',       NULL, 0.91),
    (31, 'Elena Price',    'life', 55000.00,  0.00,    '2024-06-15', '2024-06-17', 'approved',     101,  0.20);
