-- ============================================================================
-- ORC Banking Transactions — Verification Queries
-- ============================================================================
-- Each query verifies a specific aspect of the banking transaction data:
-- multi-file aggregation, fraud monitoring filters, and branch isolation.
-- ============================================================================


-- ============================================================================
-- 1. FULL SCAN — 100 transactions across 2 branch files
-- ============================================================================

ASSERT ROW_COUNT = 100
SELECT *
FROM {{zone_name}}.orc_bank.all_transactions;


-- ============================================================================
-- 2. TRANSACTION TYPE BREAKDOWN — count and total amount per type
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE txn_count = 46 WHERE txn_type = 'Purchase'
ASSERT VALUE txn_count = 18 WHERE txn_type = 'Withdrawal'
ASSERT VALUE txn_count = 14 WHERE txn_type = 'Deposit'
SELECT txn_type,
       COUNT(*) AS txn_count,
       ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.orc_bank.all_transactions
GROUP BY txn_type
ORDER BY txn_count DESC;


-- ============================================================================
-- 3. FLAGGED TRANSACTIONS — fraud monitoring filter
-- ============================================================================

ASSERT ROW_COUNT = 9
SELECT txn_id, account_id, txn_date, txn_type, amount,
       currency, merchant, risk_score
FROM {{zone_name}}.orc_bank.all_transactions
WHERE is_flagged = true
ORDER BY risk_score DESC;


-- ============================================================================
-- 4. CATEGORY ANALYSIS — average amount and count per category
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE txn_count = 38 WHERE category = 'Retail'
ASSERT VALUE txn_count = 17 WHERE category = 'Gas'
SELECT category,
       COUNT(*) AS txn_count,
       ROUND(AVG(amount), 2) AS avg_amount
FROM {{zone_name}}.orc_bank.all_transactions
GROUP BY category
ORDER BY txn_count DESC;


-- ============================================================================
-- 5. HIGH-RISK DETECTION — transactions with risk_score >= 80
-- ============================================================================

ASSERT ROW_COUNT = 5
SELECT txn_id, account_id, amount, currency, merchant,
       risk_score, is_flagged
FROM {{zone_name}}.orc_bank.all_transactions
WHERE risk_score >= 80
ORDER BY risk_score DESC;


-- ============================================================================
-- 6. PER-ACCOUNT SUMMARY — total spend and transaction count per account
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT account_id,
       COUNT(*) AS txn_count,
       ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.orc_bank.all_transactions
GROUP BY account_id
ORDER BY total_amount DESC;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check on all key metrics.

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_rows_100'
ASSERT VALUE result = 'PASS' WHERE check_name = 'flagged_count_9'
ASSERT VALUE result = 'PASS' WHERE check_name = 'distinct_accounts_20'
ASSERT VALUE result = 'PASS' WHERE check_name = 'downtown_only_50'
SELECT check_name, result FROM (

    -- Check 1: Total row count = 100
    SELECT 'total_rows_100' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_bank.all_transactions) = 100
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Flagged transaction count = 9
    SELECT 'flagged_count_9' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_bank.all_transactions
               WHERE is_flagged = true
           ) = 9 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 20 distinct accounts
    SELECT 'distinct_accounts_20' AS check_name,
           CASE WHEN (
               SELECT COUNT(DISTINCT account_id) FROM {{zone_name}}.orc_bank.all_transactions
           ) = 20 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Downtown-only table has 50 rows
    SELECT 'downtown_only_50' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_bank.downtown_only) = 50
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Sum amount matches expected total
    SELECT 'sum_amount_check' AS check_name,
           CASE WHEN (
               SELECT ROUND(SUM(amount), 2) FROM {{zone_name}}.orc_bank.all_transactions
           ) = 152214.67 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
