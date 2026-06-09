-- ============================================================================
-- Delta VACUUM — Storage Cost Savings — Setup Script
-- ============================================================================
-- Simulates a SaaS billing platform where monthly subscription charges
-- accumulate orphaned Parquet files through price corrections, refunds,
-- and cancellations. Each DML operation creates new files via copy-on-write,
-- orphaning the previous versions.
--
-- Operations:
--   1. CREATE DELTA TABLE + INSERT 30 billing transactions (3 months)
--   2. UPDATE — 15% price increase on Enterprise plan (12 rows rewritten)
--   3. UPDATE — refund 5 January transactions (status → 'refunded')
--   4. DELETE — remove 3 cancelled transactions
--   5. INSERT — 5 new March late additions
--
-- After setup, the table has 32 rows and multiple orphaned file versions.
-- The queries.sql script uses DESCRIBE DETAIL and VACUUM RETAIN 0 HOURS
-- to measure and reclaim the wasted storage.
--
-- Tables created:
--   1. billing_transactions — 32 final rows after multiple DML operations
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: billing_transactions — SaaS subscription billing
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.billing_transactions (
    id              INT,
    customer        VARCHAR,
    plan            VARCHAR,
    billing_month   VARCHAR,
    amount          DOUBLE,
    status          VARCHAR,
    created_date    VARCHAR
) LOCATION 'delta-vacuum-storage-savings/billing_transactions';


-- STEP 2: Insert 30 billing transactions across 3 months (Jan/Feb/Mar 2025)
INSERT INTO {{zone_name}}.delta_demos.billing_transactions VALUES
    (1,  'Acme Corp',       'Enterprise', '2025-01', 499.00, 'active', '2025-01-02'),
    (2,  'Beta LLC',        'Enterprise', '2025-01', 499.00, 'active', '2025-01-02'),
    (3,  'Coral Inc',       'Pro',        '2025-01',  99.00, 'active', '2025-01-03'),
    (4,  'Delta Co',        'Pro',        '2025-01',  99.00, 'active', '2025-01-03'),
    (5,  'Echo Ltd',        'Starter',    '2025-01',  29.00, 'active', '2025-01-04'),
    (6,  'Foxtrot SA',      'Starter',    '2025-01',  29.00, 'active', '2025-01-04'),
    (7,  'Gamma GmbH',      'Enterprise', '2025-01', 499.00, 'active', '2025-01-05'),
    (8,  'Hotel Pty',       'Pro',        '2025-01',  99.00, 'active', '2025-01-05'),
    (9,  'Indigo BV',       'Starter',    '2025-01',  29.00, 'active', '2025-01-06'),
    (10, 'Juliet Corp',     'Enterprise', '2025-01', 499.00, 'active', '2025-01-06'),
    (11, 'Acme Corp',       'Enterprise', '2025-02', 499.00, 'active', '2025-02-01'),
    (12, 'Beta LLC',        'Enterprise', '2025-02', 499.00, 'active', '2025-02-01'),
    (13, 'Coral Inc',       'Pro',        '2025-02',  99.00, 'active', '2025-02-02'),
    (14, 'Delta Co',        'Pro',        '2025-02',  99.00, 'active', '2025-02-02'),
    (15, 'Echo Ltd',        'Starter',    '2025-02',  29.00, 'active', '2025-02-03'),
    (16, 'Foxtrot SA',      'Starter',    '2025-02',  29.00, 'active', '2025-02-03'),
    (17, 'Gamma GmbH',      'Enterprise', '2025-02', 499.00, 'active', '2025-02-04'),
    (18, 'Hotel Pty',       'Pro',        '2025-02',  99.00, 'active', '2025-02-04'),
    (19, 'Indigo BV',       'Starter',    '2025-02',  29.00, 'active', '2025-02-05'),
    (20, 'Juliet Corp',     'Enterprise', '2025-02', 499.00, 'active', '2025-02-05'),
    (21, 'Acme Corp',       'Enterprise', '2025-03', 499.00, 'active', '2025-03-01'),
    (22, 'Beta LLC',        'Enterprise', '2025-03', 499.00, 'active', '2025-03-01'),
    (23, 'Coral Inc',       'Pro',        '2025-03',  99.00, 'active', '2025-03-02'),
    (24, 'Delta Co',        'Pro',        '2025-03',  99.00, 'active', '2025-03-02'),
    (25, 'Echo Ltd',        'Starter',    '2025-03',  29.00, 'active', '2025-03-03'),
    (26, 'Foxtrot SA',      'Starter',    '2025-03',  29.00, 'active', '2025-03-03'),
    (27, 'Gamma GmbH',      'Enterprise', '2025-03', 499.00, 'active', '2025-03-04'),
    (28, 'Hotel Pty',       'Pro',        '2025-03',  99.00, 'active', '2025-03-04'),
    (29, 'Indigo BV',       'Starter',    '2025-03',  29.00, 'active', '2025-03-05'),
    (30, 'Juliet Corp',     'Enterprise', '2025-03', 499.00, 'active', '2025-03-05');


-- ============================================================================
-- STEP 3: UPDATE — 15% price increase on Enterprise plan
-- ============================================================================
-- Enterprise customers (12 rows) get a contractual price increase.
-- Delta rewrites every file containing an Enterprise row with the new amount.
-- The old files with $499.00 amounts become orphaned on disk.
UPDATE {{zone_name}}.delta_demos.billing_transactions
SET amount = ROUND(amount * 1.15, 2)
WHERE plan = 'Enterprise';


-- ============================================================================
-- STEP 4: UPDATE — refund 5 January transactions
-- ============================================================================
-- Five January customers disputed charges. Status changes to 'refunded'.
-- Each affected file is rewritten with the new status, orphaning old versions.
UPDATE {{zone_name}}.delta_demos.billing_transactions
SET status = 'refunded'
WHERE id IN (3, 5, 6, 8, 9);


-- ============================================================================
-- STEP 5: DELETE — remove 3 cancelled transactions
-- ============================================================================
-- Three transactions (ids 4, 14, 19) were cancelled before the billing cycle
-- closed. Delta rewrites affected files WITHOUT these rows and adds "remove"
-- actions to the log. The old files become orphaned.
DELETE FROM {{zone_name}}.delta_demos.billing_transactions
WHERE id IN (4, 14, 19);


-- ============================================================================
-- STEP 6: INSERT — 5 new March late additions
-- ============================================================================
-- Five new customers signed up late in March. These create fresh Parquet files
-- that are NOT orphaned since no subsequent operation supersedes them.
INSERT INTO {{zone_name}}.delta_demos.billing_transactions
SELECT * FROM (VALUES
    (31, 'Kilo Systems',    'Enterprise', '2025-03', 573.85, 'active', '2025-03-10'),
    (32, 'Lima Digital',     'Pro',        '2025-03',  99.00, 'active', '2025-03-10'),
    (33, 'Mike Analytics',   'Starter',    '2025-03',  29.00, 'active', '2025-03-11'),
    (34, 'November AI',      'Pro',        '2025-03',  99.00, 'active', '2025-03-11'),
    (35, 'Oscar Cloud',      'Enterprise', '2025-03', 573.85, 'active', '2025-03-12')
) AS t(id, customer, plan, billing_month, amount, status, created_date);
