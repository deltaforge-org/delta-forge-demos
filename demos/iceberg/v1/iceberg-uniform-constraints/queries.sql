-- ============================================================================
-- Demo: Bank Transaction Validation — CHECK Constraints with UniForm
-- ============================================================================
-- Tests that CHECK constraints (delta.constraints.*) protect data quality
-- on a UniForm Iceberg table. All amounts must be positive and currencies
-- must be USD, EUR, or GBP.

-- ============================================================================
-- Query 1: Baseline — All 25 Transactions Present
-- ============================================================================

ASSERT ROW_COUNT = 25
SELECT * FROM {{zone_name}}.iceberg_demos.transactions ORDER BY txn_id;

-- ============================================================================
-- Query 2: Constraint Metadata — SHOW TBLPROPERTIES
-- ============================================================================
-- Verify that the CHECK constraints are registered in the table properties.

ASSERT WARNING ROW_COUNT >= 4
SHOW TBLPROPERTIES {{zone_name}}.iceberg_demos.transactions;

-- ============================================================================
-- Query 3: Per-Account Summary
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE txn_count = 6 WHERE account_id = 'ACC-1001'
ASSERT VALUE total_amount = 14000.00 WHERE account_id = 'ACC-1001'
ASSERT VALUE txn_count = 5 WHERE account_id = 'ACC-1002'
ASSERT VALUE total_amount = 19000.00 WHERE account_id = 'ACC-1002'
ASSERT VALUE txn_count = 5 WHERE account_id = 'ACC-1004'
ASSERT VALUE total_amount = 24000.00 WHERE account_id = 'ACC-1004'
SELECT
    account_id,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount
FROM {{zone_name}}.iceberg_demos.transactions
GROUP BY account_id
ORDER BY account_id;

-- ============================================================================
-- Query 4: Per-Currency Summary
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 11 WHERE currency = 'USD'
ASSERT VALUE total_amount = 38000.00 WHERE currency = 'USD'
ASSERT VALUE txn_count = 9 WHERE currency = 'EUR'
ASSERT VALUE total_amount = 23500.00 WHERE currency = 'EUR'
ASSERT VALUE txn_count = 5 WHERE currency = 'GBP'
ASSERT VALUE total_amount = 10400.00 WHERE currency = 'GBP'
SELECT
    currency,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount
FROM {{zone_name}}.iceberg_demos.transactions
GROUP BY currency
ORDER BY currency;

-- ============================================================================
-- Query 5: Per-Type Summary
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 11 WHERE txn_type = 'deposit'
ASSERT VALUE total_amount = 50900.00 WHERE txn_type = 'deposit'
ASSERT VALUE txn_count = 7 WHERE txn_type = 'transfer'
ASSERT VALUE total_amount = 13500.00 WHERE txn_type = 'transfer'
ASSERT VALUE txn_count = 7 WHERE txn_type = 'withdrawal'
ASSERT VALUE total_amount = 7500.00 WHERE txn_type = 'withdrawal'
SELECT
    txn_type,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount
FROM {{zone_name}}.iceberg_demos.transactions
GROUP BY txn_type
ORDER BY txn_type;

-- ============================================================================
-- Query 6: Constraint Validation — All Amounts Positive
-- ============================================================================
-- If constraints are enforced, no rows should have amount <= 0.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.transactions
WHERE amount <= 0;

-- ============================================================================
-- Query 7: Constraint Validation — All Currencies Valid
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.transactions
WHERE currency NOT IN ('USD', 'EUR', 'GBP');

-- ============================================================================
-- Query 8: Latest Balance per Account
-- ============================================================================
-- Finds the most recent transaction per account via CTE + ROW_NUMBER + JOIN
-- (Delta Forge does not currently support correlated scalar subqueries).

ASSERT ROW_COUNT = 5
ASSERT VALUE balance_after = 8000.00 WHERE account_id = 'ACC-1001'
ASSERT VALUE balance_after = 4000.00 WHERE account_id = 'ACC-1002'
ASSERT VALUE balance_after = 5000.00 WHERE account_id = 'ACC-1003'
ASSERT VALUE balance_after = 10000.00 WHERE account_id = 'ACC-1004'
ASSERT VALUE balance_after = 2900.00 WHERE account_id = 'ACC-1005'
WITH ranked AS (
    SELECT
        account_id,
        balance_after,
        txn_date,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_date DESC) AS rn
    FROM {{zone_name}}.iceberg_demos.transactions
)
SELECT account_id, balance_after, txn_date
FROM ranked
WHERE rn = 1
ORDER BY account_id;

-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.transactions_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.transactions_iceberg
USING ICEBERG
LOCATION '{{data_path}}/transactions';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.transactions_iceberg TO USER {{current_user}};

-- ============================================================================
-- Iceberg Verify 1: Row Count
-- ============================================================================

ASSERT ROW_COUNT = 25
SELECT * FROM {{zone_name}}.iceberg_demos.transactions_iceberg ORDER BY txn_id;

-- ============================================================================
-- Iceberg Verify 2: Per-Currency Totals — Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE total_amount = 38000.00 WHERE currency = 'USD'
ASSERT VALUE total_amount = 23500.00 WHERE currency = 'EUR'
ASSERT VALUE total_amount = 10400.00 WHERE currency = 'GBP'
SELECT
    currency,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount
FROM {{zone_name}}.iceberg_demos.transactions_iceberg
GROUP BY currency
ORDER BY currency;

-- ============================================================================
-- Iceberg Verify 3: Spot-Check — Largest Transaction
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE account_id = 'ACC-1004' WHERE txn_id = 8
ASSERT VALUE amount = 12000.00 WHERE txn_id = 8
SELECT *
FROM {{zone_name}}.iceberg_demos.transactions_iceberg
WHERE txn_id = 8;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_txns = 25
ASSERT VALUE total_amount = 71900.00
ASSERT VALUE avg_amount = 2876.00
ASSERT VALUE account_count = 5
ASSERT VALUE currency_count = 3
ASSERT VALUE max_amount = 12000.00
ASSERT VALUE min_amount = 300.00
SELECT
    COUNT(*) AS total_txns,
    SUM(amount) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    COUNT(DISTINCT account_id) AS account_count,
    COUNT(DISTINCT currency) AS currency_count,
    MAX(amount) AS max_amount,
    MIN(amount) AS min_amount
FROM {{zone_name}}.iceberg_demos.transactions;
