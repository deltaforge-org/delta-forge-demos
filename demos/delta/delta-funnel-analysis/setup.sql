-- ============================================================================
-- Delta Funnel Analysis — Setup Script
-- ============================================================================
-- Creates the user_events table and loads 100 events across 3 batch inserts
-- representing a SaaS conversion funnel: trial → activation → subscription → renewal.
--
-- Tables created:
--   1. user_events — 100 funnel events across 40 users
--
-- Operations performed:
--   1. Zone & schema creation
--   2. CREATE DELTA TABLE
--   3. INSERT batch 1 — 40 trial_start events (all 40 users)
--   4. INSERT batch 2 — 30 activation events (U001–U030)
--   5. INSERT batch 3 — 20 subscription + 10 renewal events
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: user_events — SaaS product funnel events
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.user_events (
    id          INT,
    user_id     VARCHAR,
    event_type  VARCHAR,
    plan_type   VARCHAR,
    revenue     INT,
    channel     VARCHAR,
    event_date  VARCHAR
) LOCATION 'delta-funnel-analysis/user_events';


-- ============================================================================
-- STEP 2: Batch 1 — 40 trial_start events (every user begins with a trial)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.user_events VALUES
    (1,  'U001', 'trial_start', 'basic',      0, 'organic',  '2025-01-01'),
    (2,  'U002', 'trial_start', 'basic',      0, 'referral', '2025-01-01'),
    (3,  'U003', 'trial_start', 'pro',        0, 'paid_ad',  '2025-01-01'),
    (4,  'U004', 'trial_start', 'basic',      0, 'organic',  '2025-01-02'),
    (5,  'U005', 'trial_start', 'pro',        0, 'organic',  '2025-01-02'),
    (6,  'U006', 'trial_start', 'basic',      0, 'referral', '2025-01-02'),
    (7,  'U007', 'trial_start', 'enterprise', 0, 'sales',    '2025-01-03'),
    (8,  'U008', 'trial_start', 'basic',      0, 'paid_ad',  '2025-01-03'),
    (9,  'U009', 'trial_start', 'pro',        0, 'organic',  '2025-01-03'),
    (10, 'U010', 'trial_start', 'basic',      0, 'referral', '2025-01-04'),
    (11, 'U011', 'trial_start', 'pro',        0, 'paid_ad',  '2025-01-04'),
    (12, 'U012', 'trial_start', 'basic',      0, 'organic',  '2025-01-04'),
    (13, 'U013', 'trial_start', 'enterprise', 0, 'sales',    '2025-01-05'),
    (14, 'U014', 'trial_start', 'basic',      0, 'organic',  '2025-01-05'),
    (15, 'U015', 'trial_start', 'pro',        0, 'referral', '2025-01-05'),
    (16, 'U016', 'trial_start', 'basic',      0, 'paid_ad',  '2025-01-06'),
    (17, 'U017', 'trial_start', 'pro',        0, 'organic',  '2025-01-06'),
    (18, 'U018', 'trial_start', 'basic',      0, 'referral', '2025-01-06'),
    (19, 'U019', 'trial_start', 'enterprise', 0, 'sales',    '2025-01-07'),
    (20, 'U020', 'trial_start', 'basic',      0, 'organic',  '2025-01-07'),
    (21, 'U021', 'trial_start', 'pro',        0, 'paid_ad',  '2025-01-07'),
    (22, 'U022', 'trial_start', 'basic',      0, 'organic',  '2025-01-08'),
    (23, 'U023', 'trial_start', 'pro',        0, 'referral', '2025-01-08'),
    (24, 'U024', 'trial_start', 'basic',      0, 'paid_ad',  '2025-01-08'),
    (25, 'U025', 'trial_start', 'enterprise', 0, 'sales',    '2025-01-09'),
    (26, 'U026', 'trial_start', 'basic',      0, 'organic',  '2025-01-09'),
    (27, 'U027', 'trial_start', 'pro',        0, 'organic',  '2025-01-09'),
    (28, 'U028', 'trial_start', 'basic',      0, 'referral', '2025-01-10'),
    (29, 'U029', 'trial_start', 'pro',        0, 'paid_ad',  '2025-01-10'),
    (30, 'U030', 'trial_start', 'basic',      0, 'organic',  '2025-01-10'),
    (31, 'U031', 'trial_start', 'pro',        0, 'organic',  '2025-01-11'),
    (32, 'U032', 'trial_start', 'basic',      0, 'referral', '2025-01-11'),
    (33, 'U033', 'trial_start', 'enterprise', 0, 'sales',    '2025-01-11'),
    (34, 'U034', 'trial_start', 'basic',      0, 'paid_ad',  '2025-01-12'),
    (35, 'U035', 'trial_start', 'pro',        0, 'organic',  '2025-01-12'),
    (36, 'U036', 'trial_start', 'basic',      0, 'organic',  '2025-01-12'),
    (37, 'U037', 'trial_start', 'pro',        0, 'referral', '2025-01-13'),
    (38, 'U038', 'trial_start', 'basic',      0, 'paid_ad',  '2025-01-13'),
    (39, 'U039', 'trial_start', 'enterprise', 0, 'sales',    '2025-01-14'),
    (40, 'U040', 'trial_start', 'basic',      0, 'organic',  '2025-01-14');


-- ============================================================================
-- STEP 3: Batch 2 — 30 activation events (U001–U030 activate their accounts)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.user_events
SELECT * FROM (VALUES
    (41, 'U001', 'activation', 'basic',      0, 'organic',  '2025-01-05'),
    (42, 'U002', 'activation', 'basic',      0, 'referral', '2025-01-04'),
    (43, 'U003', 'activation', 'pro',        0, 'paid_ad',  '2025-01-04'),
    (44, 'U004', 'activation', 'basic',      0, 'organic',  '2025-01-06'),
    (45, 'U005', 'activation', 'pro',        0, 'organic',  '2025-01-05'),
    (46, 'U006', 'activation', 'basic',      0, 'referral', '2025-01-06'),
    (47, 'U007', 'activation', 'enterprise', 0, 'sales',    '2025-01-06'),
    (48, 'U008', 'activation', 'basic',      0, 'paid_ad',  '2025-01-07'),
    (49, 'U009', 'activation', 'pro',        0, 'organic',  '2025-01-07'),
    (50, 'U010', 'activation', 'basic',      0, 'referral', '2025-01-08'),
    (51, 'U011', 'activation', 'pro',        0, 'paid_ad',  '2025-01-08'),
    (52, 'U012', 'activation', 'basic',      0, 'organic',  '2025-01-08'),
    (53, 'U013', 'activation', 'enterprise', 0, 'sales',    '2025-01-09'),
    (54, 'U014', 'activation', 'basic',      0, 'organic',  '2025-01-09'),
    (55, 'U015', 'activation', 'pro',        0, 'referral', '2025-01-09'),
    (56, 'U016', 'activation', 'basic',      0, 'paid_ad',  '2025-01-10'),
    (57, 'U017', 'activation', 'pro',        0, 'organic',  '2025-01-10'),
    (58, 'U018', 'activation', 'basic',      0, 'referral', '2025-01-10'),
    (59, 'U019', 'activation', 'enterprise', 0, 'sales',    '2025-01-11'),
    (60, 'U020', 'activation', 'basic',      0, 'organic',  '2025-01-11'),
    (61, 'U021', 'activation', 'pro',        0, 'paid_ad',  '2025-01-11'),
    (62, 'U022', 'activation', 'basic',      0, 'organic',  '2025-01-12'),
    (63, 'U023', 'activation', 'pro',        0, 'referral', '2025-01-12'),
    (64, 'U024', 'activation', 'basic',      0, 'paid_ad',  '2025-01-12'),
    (65, 'U025', 'activation', 'enterprise', 0, 'sales',    '2025-01-13'),
    (66, 'U026', 'activation', 'basic',      0, 'organic',  '2025-01-13'),
    (67, 'U027', 'activation', 'pro',        0, 'organic',  '2025-01-13'),
    (68, 'U028', 'activation', 'basic',      0, 'referral', '2025-01-14'),
    (69, 'U029', 'activation', 'pro',        0, 'paid_ad',  '2025-01-14'),
    (70, 'U030', 'activation', 'basic',      0, 'organic',  '2025-01-14')
) AS t(id, user_id, event_type, plan_type, revenue, channel, event_date);


-- ============================================================================
-- STEP 4: Batch 3 — 20 subscriptions (U001–U020) + 10 renewals (U001–U010)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.user_events
SELECT * FROM (VALUES
    (71,  'U001', 'subscription', 'basic',       29, 'organic',  '2025-01-15'),
    (72,  'U002', 'subscription', 'basic',       29, 'referral', '2025-01-15'),
    (73,  'U003', 'subscription', 'pro',         79, 'paid_ad',  '2025-01-15'),
    (74,  'U004', 'subscription', 'basic',       29, 'organic',  '2025-01-16'),
    (75,  'U005', 'subscription', 'pro',         79, 'organic',  '2025-01-16'),
    (76,  'U006', 'subscription', 'basic',       29, 'referral', '2025-01-16'),
    (77,  'U007', 'subscription', 'enterprise', 199, 'sales',    '2025-01-17'),
    (78,  'U008', 'subscription', 'basic',       29, 'paid_ad',  '2025-01-17'),
    (79,  'U009', 'subscription', 'pro',         79, 'organic',  '2025-01-17'),
    (80,  'U010', 'subscription', 'basic',       29, 'referral', '2025-01-18'),
    (81,  'U011', 'subscription', 'pro',         79, 'paid_ad',  '2025-01-18'),
    (82,  'U012', 'subscription', 'basic',       29, 'organic',  '2025-01-18'),
    (83,  'U013', 'subscription', 'enterprise', 199, 'sales',    '2025-01-19'),
    (84,  'U014', 'subscription', 'basic',       29, 'organic',  '2025-01-19'),
    (85,  'U015', 'subscription', 'pro',         79, 'referral', '2025-01-19'),
    (86,  'U016', 'subscription', 'basic',       29, 'paid_ad',  '2025-01-20'),
    (87,  'U017', 'subscription', 'pro',         79, 'organic',  '2025-01-20'),
    (88,  'U018', 'subscription', 'basic',       29, 'referral', '2025-01-20'),
    (89,  'U019', 'subscription', 'enterprise', 199, 'sales',    '2025-01-21'),
    (90,  'U020', 'subscription', 'basic',       29, 'organic',  '2025-01-21'),
    (91,  'U001', 'renewal',      'basic',       29, 'organic',  '2025-02-15'),
    (92,  'U002', 'renewal',      'basic',       29, 'referral', '2025-02-15'),
    (93,  'U003', 'renewal',      'pro',         79, 'paid_ad',  '2025-02-15'),
    (94,  'U004', 'renewal',      'basic',       29, 'organic',  '2025-02-16'),
    (95,  'U005', 'renewal',      'pro',         79, 'organic',  '2025-02-16'),
    (96,  'U006', 'renewal',      'basic',       29, 'referral', '2025-02-16'),
    (97,  'U007', 'renewal',      'enterprise', 199, 'sales',    '2025-02-17'),
    (98,  'U008', 'renewal',      'basic',       29, 'paid_ad',  '2025-02-17'),
    (99,  'U009', 'renewal',      'pro',         79, 'organic',  '2025-02-17'),
    (100, 'U010', 'renewal',      'basic',       29, 'referral', '2025-02-18')
) AS t(id, user_id, event_type, plan_type, revenue, channel, event_date);
