-- ============================================================================
-- Delta Change Data Feed — Educational Queries
-- ============================================================================
-- WHAT: CDF records row-level changes (insert, update_preimage,
--       update_postimage, delete) in a separate _change_data directory
-- WHY:  Without CDF, downstream systems must re-read the entire table to
--       detect changes — CDF enables efficient incremental ETL by exposing
--       only the rows that changed between any two versions
-- HOW:  The TBLPROPERTY 'delta.enableChangeDataFeed' = 'true' tells Delta to
--       write change records alongside data files. Each change record includes
--       _change_type, _commit_version, and _commit_timestamp metadata columns.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline Table State (V0) — 40 Customers
-- ============================================================================
-- The setup script created the table with CDF enabled and inserted 40
-- customer accounts. Let's inspect the starting state before any changes.

ASSERT ROW_COUNT = 2
SELECT tier,
       COUNT(*) AS customer_count,
       ROUND(AVG(balance), 2) AS avg_balance,
       ROUND(MIN(balance), 2) AS min_balance,
       ROUND(MAX(balance), 2) AS max_balance
FROM {{zone_name}}.delta_demos.customer_accounts
GROUP BY tier
ORDER BY tier;


-- ============================================================================
-- V1: UPDATE — Upgrade 10 Customers to 'gold' Tier
-- ============================================================================
-- CDF tracks UPDATEs by recording two rows per changed record:
--   'update_preimage'  — the row BEFORE the change (old values)
--   'update_postimage' — the row AFTER the change (new values)
-- This lets downstream consumers see exactly what changed, not just the
-- final state. Here we promote the top 10 customers by balance to gold.
-- ids: 31(8500), 8(8100), 23(7800), 12(7200), 33(7100), 39(6800),
--      6(6700), 29(6500), 17(6300), 27(5900)

UPDATE {{zone_name}}.delta_demos.customer_accounts
SET tier = 'gold'
WHERE id IN (31, 8, 23, 12, 33, 39, 6, 29, 17, 27);

-- Confirm: the 10 gold tier customers after the upgrade
ASSERT ROW_COUNT = 10
SELECT id, name, tier, balance
FROM {{zone_name}}.delta_demos.customer_accounts
WHERE tier = 'gold'
ORDER BY balance DESC;


-- ============================================================================
-- V2: INSERT — Add 8 New Customers
-- ============================================================================
-- CDF records INSERTs with _change_type = 'insert'. Each new row appears
-- exactly once in the change feed. This is the simplest change type —
-- no preimage/postimage pair, just the new data.

INSERT INTO {{zone_name}}.delta_demos.customer_accounts
SELECT * FROM (VALUES
    (41, 'Oscar Fernandez', 'oscar.fernandez@mail.com',  'bronze', 2500.00, 'active', '2024-03-01'),
    (42, 'Priya Sharma',    'priya.sharma@mail.com',     'silver', 4800.00, 'active', '2024-03-05'),
    (43, 'Remy Laurent',    'remy.laurent@mail.com',     'bronze', 1600.00, 'active', '2024-03-10'),
    (44, 'Sofia Rossi',     'sofia.rossi@mail.com',      'silver', 5300.00, 'active', '2024-03-15'),
    (45, 'Tariq Mansour',   'tariq.mansour@mail.com',    'bronze', 900.00,  'active', '2024-04-01'),
    (46, 'Uma Reddy',       'uma.reddy@mail.com',        'silver', 3700.00, 'active', '2024-04-05'),
    (47, 'Viktor Novak',    'viktor.novak@mail.com',     'bronze', 2100.00, 'active', '2024-04-10'),
    (48, 'Wendy Chang',     'wendy.chang@mail.com',      'silver', 4400.00, 'active', '2024-04-15')
) AS t(id, name, email, tier, balance, status, created_date);

-- Confirm: the 8 newly inserted customers
ASSERT ROW_COUNT = 8
SELECT id, name, email, tier, balance, created_date
FROM {{zone_name}}.delta_demos.customer_accounts
WHERE id BETWEEN 41 AND 48
ORDER BY id;


-- ============================================================================
-- LEARN: Why CDF Matters for Incremental ETL
-- ============================================================================
-- Consider a downstream analytics system that needs to stay in sync with this
-- table. Without CDF, it must re-read all rows every time. With CDF, it can
-- query only the changes since the last processed version.
--
-- For example, to process only the V2 inserts, a consumer would read:
--   table_changes('customer_accounts', 2, 2)
-- and see exactly 8 rows with _change_type = 'insert'.
--
-- Current state: 48 rows (40 original + 8 new), 10 gold, rest silver/bronze.

-- Verify total is now 48 after inserting 8 new customers
ASSERT VALUE total_count = 48
SELECT COUNT(*) AS total_count FROM {{zone_name}}.delta_demos.customer_accounts;

ASSERT ROW_COUNT = 3
SELECT tier, COUNT(*) AS customer_count
FROM {{zone_name}}.delta_demos.customer_accounts
GROUP BY tier
ORDER BY tier;


-- ============================================================================
-- V3: UPDATE + DELETE — Close 3 Accounts
-- ============================================================================
-- Account closures involve two operations. First we mark accounts as closed,
-- then delete them. CDF captures both steps:
--   - The UPDATE generates update_preimage (status='active') and
--     update_postimage (status='closed') pairs
--   - The DELETE generates _change_type = 'delete' rows showing the final
--     state of each removed record
-- ids: 16 (Patrick, balance 900), 28 (Beatrice, balance 1100),
--      45 (Tariq, balance 900)

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.customer_accounts
SET status = 'closed'
WHERE id IN (16, 28, 45);

ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.delta_demos.customer_accounts
WHERE status = 'closed';

-- Confirm: the 3 deleted accounts are truly gone
ASSERT ROW_COUNT = 0
SELECT id, name
FROM {{zone_name}}.delta_demos.customer_accounts
WHERE id IN (16, 28, 45)
ORDER BY id;


-- ============================================================================
-- V4: UPDATE — Adjust Balances for 5 Premium (Gold) Customers +20%
-- ============================================================================
-- CDF records both the preimage (old balance) and postimage (new balance)
-- for each updated row — perfect for auditing financial changes.
-- ids: 31(8500->10200), 8(8100->9720), 23(7800->9360),
--      12(7200->8640), 33(7100->8520)

UPDATE {{zone_name}}.delta_demos.customer_accounts
SET balance = ROUND(balance * 1.20, 2)
WHERE id IN (31, 8, 23, 12, 33);

-- Confirm: gold tier customers with their adjusted balances
-- Verify id=31 balance increased from 8500 to 10200 (+20%)
ASSERT VALUE balance = 10200.0
SELECT balance FROM {{zone_name}}.delta_demos.customer_accounts WHERE id = 31;

ASSERT ROW_COUNT = 10
SELECT id, name, tier, balance,
       CASE WHEN id IN (31, 8, 23, 12, 33)
            THEN ROUND(balance / 1.20, 2)
            ELSE NULL END AS original_balance,
       CASE WHEN id IN (31, 8, 23, 12, 33)
            THEN '+20% applied'
            ELSE 'unchanged' END AS adjustment
FROM {{zone_name}}.delta_demos.customer_accounts
WHERE tier = 'gold'
ORDER BY balance DESC;


-- ============================================================================
-- EXPLORE: Full Customer Directory — Final State
-- ============================================================================

ASSERT ROW_COUNT = 45
SELECT id, name, email, tier, balance, status, created_date
FROM {{zone_name}}.delta_demos.customer_accounts
ORDER BY tier, name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Verification of final state after all 5 versions of changes.

-- Verify total row count
ASSERT ROW_COUNT = 45
SELECT * FROM {{zone_name}}.delta_demos.customer_accounts;

-- Verify gold tier count
ASSERT VALUE gold_tier_count = 10
SELECT COUNT(*) AS gold_tier_count FROM {{zone_name}}.delta_demos.customer_accounts WHERE tier = 'gold';

-- Verify closed accounts are gone
ASSERT VALUE closed_count = 0
SELECT COUNT(*) AS closed_count FROM {{zone_name}}.delta_demos.customer_accounts WHERE id IN (16, 28, 45);

-- Verify new customers count (excluding deleted id 45)
ASSERT VALUE new_customers_count = 7
SELECT COUNT(*) AS new_customers_count FROM {{zone_name}}.delta_demos.customer_accounts WHERE id BETWEEN 41 AND 48;

-- Verify Erik's balance after 20% increase
ASSERT VALUE balance = 10200.0
SELECT balance FROM {{zone_name}}.delta_demos.customer_accounts WHERE id = 31;

-- Verify silver tier count
ASSERT VALUE silver_tier_count = 15
SELECT COUNT(*) AS silver_tier_count FROM {{zone_name}}.delta_demos.customer_accounts WHERE tier = 'silver';

-- Verify all accounts are active
ASSERT VALUE inactive_count = 0
SELECT COUNT(*) AS inactive_count FROM {{zone_name}}.delta_demos.customer_accounts WHERE status != 'active';

-- Verify Alice's balance unchanged
ASSERT VALUE balance = 5200.0
SELECT balance FROM {{zone_name}}.delta_demos.customer_accounts WHERE id = 1;
