-- ============================================================================
-- Delta Overflow Detection — Treasury Balance Monitoring — Educational Queries
-- ============================================================================
-- WHAT: INT columns hold values up to 2,147,483,647. Financial balances can
--       silently overflow this limit, corrupting data. Overflow detection
--       queries identify at-risk accounts before damage occurs.
-- WHY:  Treasury systems often start with INT for dollar amounts. As accounts
--       grow from millions to billions, balances approach the INT ceiling.
--       Detecting this early allows proactive type widening to BIGINT.
-- HOW:  Query MAX(running_balance) per account, compute percentage of INT max,
--       flag accounts above a threshold (e.g., 60%), then widen via
--       ALTER TABLE ALTER COLUMN TYPE before the next deposit overflows.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Account overview — 5 tiers from small business to sovereign
-- ============================================================================
-- Each account represents a different scale of financial operations:
--   ACCT-1001: Small business ($4.5M peak)
--   ACCT-2001: Startup ($10.7M peak)
--   ACCT-3001: Investment fund ($92M peak)
--   ACCT-4001: Corporate treasury ($1.45B peak)
--   ACCT-5001: Sovereign wealth fund ($2.05B peak — 95% of INT max)

ASSERT ROW_COUNT = 5
ASSERT VALUE max_balance = 2050000000 WHERE account_id = 'ACCT-5001'
ASSERT VALUE max_balance = 1450000000 WHERE account_id = 'ACCT-4001'
SELECT account_id,
       COUNT(*) AS tx_count,
       MAX(running_balance) AS max_balance,
       SUM(amount) AS net_flow
FROM {{zone_name}}.delta_demos.transaction_ledger
GROUP BY account_id
ORDER BY max_balance DESC;


-- ============================================================================
-- LEARN: INT boundary proximity detection
-- ============================================================================
-- The critical query: measure each account's peak balance as a percentage
-- of the INT maximum (2,147,483,647). Any account above 60% is at risk —
-- a single large deposit could push it past the limit.
--
-- ACCT-5001 at 95.46% is in the danger zone. ACCT-4001 at 67.53% needs
-- monitoring. The other three accounts are safely below 5%.

ASSERT ROW_COUNT = 5
-- Non-deterministic: DOUBLE division rounding may vary slightly
ASSERT WARNING VALUE pct_of_limit BETWEEN 95.45 AND 95.47 WHERE account_id = 'ACCT-5001'
-- Non-deterministic: DOUBLE division rounding may vary slightly
ASSERT WARNING VALUE pct_of_limit BETWEEN 67.52 AND 67.54 WHERE account_id = 'ACCT-4001'
SELECT account_id,
       MAX(running_balance) AS peak_balance,
       ROUND(CAST(MAX(running_balance) AS DOUBLE) / 2147483647 * 100, 2) AS pct_of_limit,
       CASE
         WHEN MAX(running_balance) > 1288490188 THEN 'CRITICAL (>60%)'
         WHEN MAX(running_balance) > 214748364  THEN 'WATCH (>10%)'
         ELSE 'SAFE'
       END AS risk_level
FROM {{zone_name}}.delta_demos.transaction_ledger
GROUP BY account_id
ORDER BY peak_balance DESC;


-- ============================================================================
-- PHASE 1: Enable type widening and promote columns to BIGINT
-- ============================================================================
-- ACCT-5001 is at 95% of INT max. The next quarterly deposit (~$200M)
-- would push it past the limit. We widen BEFORE that deposit arrives.
-- This is metadata-only — existing Parquet files are not rewritten.

ALTER TABLE {{zone_name}}.delta_demos.transaction_ledger SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');

ALTER TABLE {{zone_name}}.delta_demos.transaction_ledger ALTER COLUMN running_balance TYPE BIGINT;

ALTER TABLE {{zone_name}}.delta_demos.transaction_ledger ALTER COLUMN amount TYPE BIGINT;


-- ============================================================================
-- PHASE 2: Simulate quarterly growth that would have overflowed INT
-- ============================================================================
-- A $200M adjustment to ACCT-5001 deposit balances pushes id=30 to
-- $2.25B — safely beyond INT max. Without type widening, this UPDATE
-- would have silently corrupted the data or thrown an overflow error.

UPDATE {{zone_name}}.delta_demos.transaction_ledger
SET running_balance = running_balance + 200000000
WHERE account_id = 'ACCT-5001' AND tx_type = 'deposit';


-- ============================================================================
-- OBSERVE: The overflow that didn't happen
-- ============================================================================
-- Row id=30 now has running_balance = 2,250,000,000 — exceeding INT max
-- by over 100 million. This value is safe in BIGINT (max 9.2 quintillion).

ASSERT ROW_COUNT = 6
ASSERT VALUE running_balance = 2250000000 WHERE id = 30
ASSERT VALUE running_balance = 2100000000 WHERE id = 24
SELECT id, account_id, tx_type, amount, running_balance, description
FROM {{zone_name}}.delta_demos.transaction_ledger
WHERE account_id = 'ACCT-5001'
ORDER BY id;


-- ============================================================================
-- PHASE 3: Insert sovereign and institutional transactions (BIGINT range)
-- ============================================================================
-- New accounts enter the system with balances that never would have fit
-- in INT: central bank reserves, pension funds, sovereign wealth transfers.

ASSERT ROW_COUNT = 10
INSERT INTO {{zone_name}}.delta_demos.transaction_ledger
SELECT * FROM (VALUES
    (31, 'ACCT-5001', 'deposit',    3000000000,  5250000000,  'Mega infrastructure bond',  '2025-05-01'),
    (32, 'ACCT-5001', 'deposit',    2500000000,  7750000000,  'Sovereign wealth transfer', '2025-05-15'),
    (33, 'ACCT-6001', 'deposit',    5000000000,  5000000000,  'Central bank reserve',      '2025-05-01'),
    (34, 'ACCT-6001', 'deposit',    8000000000,  13000000000, 'Foreign exchange reserve',  '2025-05-15'),
    (35, 'ACCT-6001', 'withdrawal', -2000000000, 11000000000, 'Currency stabilization',    '2025-06-01'),
    (36, 'ACCT-7001', 'deposit',    10000000000, 10000000000, 'Pension fund seed',         '2025-05-01'),
    (37, 'ACCT-7001', 'deposit',    7500000000,  17500000000, 'Annual contributions',      '2025-05-15'),
    (38, 'ACCT-7001', 'withdrawal', -3000000000, 14500000000, 'Benefit payments',          '2025-06-01'),
    (39, 'ACCT-7001', 'deposit',    12000000000, 26500000000, 'Investment returns',        '2025-06-15'),
    (40, 'ACCT-7001', 'deposit',    9000000000,  35500000000, 'Rebalance gains',           '2025-07-01')
) AS t(id, account_id, tx_type, amount, running_balance, description, tx_date);


-- ============================================================================
-- LEARN: Account tier analysis — balances spanning 8 orders of magnitude
-- ============================================================================
-- The ledger now spans from $4.5M (small business) to $35.5B (pension fund).
-- Without type widening, accounts above $2.1B would have required a
-- separate table or a disruptive full-table rewrite.

ASSERT ROW_COUNT = 4
ASSERT VALUE account_count = 2 WHERE balance_tier = 'Tier 1: Sovereign/Institutional'
ASSERT VALUE account_count = 1 WHERE balance_tier = 'Tier 4: Standard'
SELECT balance_tier,
       COUNT(*) AS account_count,
       SUM(peak_balance) AS tier_total_balance
FROM (
    SELECT account_id,
           MAX(running_balance) AS peak_balance,
           CASE
             WHEN MAX(running_balance) > 10000000000 THEN 'Tier 1: Sovereign/Institutional'
             WHEN MAX(running_balance) > 1000000000  THEN 'Tier 2: Large Enterprise'
             WHEN MAX(running_balance) > 10000000    THEN 'Tier 3: Mid-Market'
             ELSE 'Tier 4: Standard'
           END AS balance_tier
    FROM {{zone_name}}.delta_demos.transaction_ledger
    GROUP BY account_id
) AS account_tiers
GROUP BY balance_tier
ORDER BY tier_total_balance DESC;


-- ============================================================================
-- LEARN: Deposit vs withdrawal flow analysis
-- ============================================================================
-- Across all 40 transactions, deposits dominate — typical for growing
-- treasury portfolios. The net flow demonstrates why balances grow
-- relentlessly toward type boundaries.

ASSERT ROW_COUNT = 2
ASSERT VALUE tx_count = 30 WHERE tx_type = 'deposit'
ASSERT VALUE tx_count = 10 WHERE tx_type = 'withdrawal'
SELECT tx_type,
       COUNT(*) AS tx_count,
       SUM(amount) AS total_amount
FROM {{zone_name}}.delta_demos.transaction_ledger
GROUP BY tx_type
ORDER BY tx_type DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 40
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.transaction_ledger;

-- Verify 7 distinct accounts
ASSERT VALUE account_count = 7
SELECT COUNT(DISTINCT account_id) AS account_count FROM {{zone_name}}.delta_demos.transaction_ledger;

-- Verify ACCT-5001 row 24 balance after growth update (1900000000 + 200000000)
ASSERT VALUE running_balance = 2100000000
SELECT running_balance FROM {{zone_name}}.delta_demos.transaction_ledger WHERE id = 24;

-- Verify ACCT-5001 row 30 balance after growth update (2050000000 + 200000000)
ASSERT VALUE running_balance = 2250000000
SELECT running_balance FROM {{zone_name}}.delta_demos.transaction_ledger WHERE id = 30;

-- Verify pension fund final balance
ASSERT VALUE running_balance = 35500000000
SELECT running_balance FROM {{zone_name}}.delta_demos.transaction_ledger WHERE id = 40;

-- Verify maximum running balance across all accounts
ASSERT VALUE max_balance = 35500000000
SELECT MAX(running_balance) AS max_balance FROM {{zone_name}}.delta_demos.transaction_ledger;

-- Verify count of rows exceeding INT max
ASSERT VALUE overflow_count = 11
SELECT COUNT(*) AS overflow_count FROM {{zone_name}}.delta_demos.transaction_ledger WHERE running_balance > 2147483647;

-- Verify ACCT-7001 net flow
ASSERT VALUE net_flow = 35500000000
SELECT SUM(amount) AS net_flow FROM {{zone_name}}.delta_demos.transaction_ledger WHERE account_id = 'ACCT-7001';

-- Verify ACCT-1001 balance unchanged by ACCT-5001 update
ASSERT VALUE running_balance = 4480000
SELECT running_balance FROM {{zone_name}}.delta_demos.transaction_ledger WHERE id = 26;
