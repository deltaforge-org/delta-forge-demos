-- ============================================================================
-- Delta Audit Trail — Native Version-Based Compliance — Setup Script
-- ============================================================================
-- Demonstrates using Delta's native transaction log and time travel as an
-- immutable audit trail for financial compliance. Each INSERT/MERGE creates
-- a new Delta version that auditors can inspect with DESCRIBE HISTORY and
-- reconstruct with VERSION AS OF.
--
-- Tables created:
--   1. compliance_events — 42 final rows across 5 Delta versions (V0–V4)
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE with CDF enabled
--   3. INSERT — 20 account openings and initial deposits (Version 1)
--   4. INSERT — 12 transactions: deposits, withdrawals, transfers (Version 2)
--   5. INSERT — 5 compliance events: freezes, closures, large movements (Version 3)
--   6. INSERT — 5 late-arriving events from offline branch system (Version 4)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: compliance_events — Financial account event log
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.compliance_events (
    event_id        INT,
    account_id      VARCHAR,
    account_name    VARCHAR,
    event_type      VARCHAR,
    amount          DECIMAL(12,2),
    balance         DECIMAL(12,2),
    officer         VARCHAR,
    branch          VARCHAR,
    event_date      VARCHAR
) LOCATION 'delta-audit-trail-versioning/compliance_events'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);


-- ============================================================================
-- VERSION 1: Account openings and initial deposits (20 rows)
-- ============================================================================
-- 10 commercial accounts opened across 4 branches by 4 officers.
-- Each account has an 'open' event (zero balance) followed by an initial deposit.
INSERT INTO {{zone_name}}.delta_demos.compliance_events VALUES
    (1,  'ACCT-1001', 'Meridian Holdings LLC',    'open',       NULL,      0.00,       'j.chen',     'downtown',  '2024-01-02 09:00:00'),
    (2,  'ACCT-1001', 'Meridian Holdings LLC',    'deposit',    250000.00, 250000.00,  'j.chen',     'downtown',  '2024-01-02 09:05:00'),
    (3,  'ACCT-1002', 'Northstar Ventures Inc',   'open',       NULL,      0.00,       'r.patel',    'midtown',   '2024-01-03 10:00:00'),
    (4,  'ACCT-1002', 'Northstar Ventures Inc',   'deposit',    175000.00, 175000.00,  'r.patel',    'midtown',   '2024-01-03 10:05:00'),
    (5,  'ACCT-1003', 'Cascade Financial Group',  'open',       NULL,      0.00,       's.williams', 'westside',  '2024-01-04 08:30:00'),
    (6,  'ACCT-1003', 'Cascade Financial Group',  'deposit',    500000.00, 500000.00,  's.williams', 'westside',  '2024-01-04 08:35:00'),
    (7,  'ACCT-1004', 'Pinnacle Trust Co',        'open',       NULL,      0.00,       'j.chen',     'downtown',  '2024-01-05 11:00:00'),
    (8,  'ACCT-1004', 'Pinnacle Trust Co',        'deposit',    320000.00, 320000.00,  'j.chen',     'downtown',  '2024-01-05 11:05:00'),
    (9,  'ACCT-1005', 'Summit Capital Partners',  'open',       NULL,      0.00,       'r.patel',    'midtown',   '2024-01-08 09:00:00'),
    (10, 'ACCT-1005', 'Summit Capital Partners',  'deposit',    890000.00, 890000.00,  'r.patel',    'midtown',   '2024-01-08 09:05:00'),
    (11, 'ACCT-1006', 'Ironclad Securities',      'open',       NULL,      0.00,       's.williams', 'westside',  '2024-01-09 10:30:00'),
    (12, 'ACCT-1006', 'Ironclad Securities',      'deposit',    415000.00, 415000.00,  's.williams', 'westside',  '2024-01-09 10:35:00'),
    (13, 'ACCT-1007', 'BlueSky Investments',      'open',       NULL,      0.00,       'm.torres',   'eastpoint', '2024-01-10 08:00:00'),
    (14, 'ACCT-1007', 'BlueSky Investments',      'deposit',    660000.00, 660000.00,  'm.torres',   'eastpoint', '2024-01-10 08:05:00'),
    (15, 'ACCT-1008', 'Redwood Asset Management', 'open',       NULL,      0.00,       'm.torres',   'eastpoint', '2024-01-11 09:15:00'),
    (16, 'ACCT-1008', 'Redwood Asset Management', 'deposit',    1200000.00, 1200000.00, 'm.torres',  'eastpoint', '2024-01-11 09:20:00'),
    (17, 'ACCT-1009', 'Granite Partners',         'open',       NULL,      0.00,       'j.chen',     'downtown',  '2024-01-12 10:00:00'),
    (18, 'ACCT-1009', 'Granite Partners',         'deposit',    95000.00,  95000.00,   'j.chen',     'downtown',  '2024-01-12 10:05:00'),
    (19, 'ACCT-1010', 'Apex Consulting Group',    'open',       NULL,      0.00,       'r.patel',    'midtown',   '2024-01-15 08:45:00'),
    (20, 'ACCT-1010', 'Apex Consulting Group',    'deposit',    55000.00,  55000.00,   'r.patel',    'midtown',   '2024-01-15 08:50:00');


-- ============================================================================
-- VERSION 2: Transactions — deposits, withdrawals, transfers (12 rows)
-- ============================================================================
-- Normal banking activity across February. Withdrawals and transfers move
-- funds between accounts; each event captures the resulting balance.
INSERT INTO {{zone_name}}.delta_demos.compliance_events VALUES
    (21, 'ACCT-1001', 'Meridian Holdings LLC',    'withdrawal', 75000.00,  175000.00,  'j.chen',     'downtown',  '2024-02-01 14:00:00'),
    (22, 'ACCT-1001', 'Meridian Holdings LLC',    'deposit',    30000.00,  205000.00,  'j.chen',     'downtown',  '2024-02-05 10:30:00'),
    (23, 'ACCT-1002', 'Northstar Ventures Inc',   'transfer',   50000.00,  125000.00,  'r.patel',    'midtown',   '2024-02-03 11:00:00'),
    (24, 'ACCT-1003', 'Cascade Financial Group',  'withdrawal', 120000.00, 380000.00,  's.williams', 'westside',  '2024-02-07 09:15:00'),
    (25, 'ACCT-1003', 'Cascade Financial Group',  'deposit',    45000.00,  425000.00,  's.williams', 'westside',  '2024-02-10 15:00:00'),
    (26, 'ACCT-1004', 'Pinnacle Trust Co',        'transfer',   80000.00,  240000.00,  'j.chen',     'downtown',  '2024-02-08 16:30:00'),
    (27, 'ACCT-1005', 'Summit Capital Partners',  'withdrawal', 200000.00, 690000.00,  'r.patel',    'midtown',   '2024-02-12 10:00:00'),
    (28, 'ACCT-1006', 'Ironclad Securities',      'deposit',    85000.00,  500000.00,  's.williams', 'westside',  '2024-02-14 11:30:00'),
    (29, 'ACCT-1007', 'BlueSky Investments',      'withdrawal', 160000.00, 500000.00,  'm.torres',   'eastpoint', '2024-02-15 13:45:00'),
    (30, 'ACCT-1008', 'Redwood Asset Management', 'deposit',    300000.00, 1500000.00, 'm.torres',   'eastpoint', '2024-02-18 09:00:00'),
    (31, 'ACCT-1009', 'Granite Partners',         'withdrawal', 25000.00,  70000.00,   'j.chen',     'downtown',  '2024-02-20 14:15:00'),
    (32, 'ACCT-1010', 'Apex Consulting Group',    'deposit',    15000.00,  70000.00,   'r.patel',    'midtown',   '2024-02-22 10:45:00');


-- ============================================================================
-- VERSION 3: Compliance events — freezes, closures, large movements (5 rows)
-- ============================================================================
-- March: compliance officer freezes two accounts under investigation,
-- one account is closed, and two large movements are recorded.
INSERT INTO {{zone_name}}.delta_demos.compliance_events VALUES
    (33, 'ACCT-1009', 'Granite Partners',         'freeze',     NULL,      70000.00,   'compliance', 'downtown',  '2024-03-01 08:00:00'),
    (34, 'ACCT-1010', 'Apex Consulting Group',    'close',      55000.00,  0.00,       'r.patel',    'midtown',   '2024-03-05 16:00:00'),
    (35, 'ACCT-1002', 'Northstar Ventures Inc',   'freeze',     NULL,      125000.00,  'compliance', 'midtown',   '2024-03-08 09:30:00'),
    (36, 'ACCT-1005', 'Summit Capital Partners',  'withdrawal', 100000.00, 590000.00,  'r.patel',    'midtown',   '2024-03-10 11:00:00'),
    (37, 'ACCT-1008', 'Redwood Asset Management', 'transfer',   500000.00, 1000000.00, 'm.torres',   'eastpoint', '2024-03-12 14:30:00');


-- ============================================================================
-- VERSION 4: Late-arriving events from an offline branch system (5 rows)
-- ============================================================================
-- April: a batch of 5 events arrives from a branch system that was offline.
-- Each creates a new Delta version entry that auditors can trace.
INSERT INTO {{zone_name}}.delta_demos.compliance_events VALUES
    (38, 'ACCT-1001', 'Meridian Holdings LLC',    'deposit',    100000.00, 305000.00,  'j.chen',     'downtown',  '2024-04-01 09:00:00'),
    (39, 'ACCT-1003', 'Cascade Financial Group',  'withdrawal', 75000.00,  350000.00,  's.williams', 'westside',  '2024-04-02 10:30:00'),
    (40, 'ACCT-1006', 'Ironclad Securities',      'transfer',   100000.00, 400000.00,  's.williams', 'westside',  '2024-04-03 15:00:00'),
    (41, 'ACCT-1007', 'BlueSky Investments',      'deposit',    200000.00, 700000.00,  'm.torres',   'eastpoint', '2024-04-05 11:15:00'),
    (42, 'ACCT-1004', 'Pinnacle Trust Co',        'deposit',    60000.00,  300000.00,  'j.chen',     'downtown',  '2024-04-08 08:45:00');
