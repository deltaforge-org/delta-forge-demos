-- ============================================================================
-- Iceberg UniForm Column Rename (Field-ID Stability) — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH COLUMN RENAMES
-- --------------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- When ALTER TABLE RENAME COLUMN runs, Delta Forge:
--   1. Updates the column name in _delta_log/ column mapping metadata
--   2. Updates the field name in the Iceberg schema within metadata.json
--   3. Crucially, the Iceberg field-id stays the same — only the name changes
--
-- This field-ID stability is what makes column renames safe for Iceberg
-- consumers: engines that resolve columns by ID (the Iceberg standard)
-- transparently see the renamed column without rewriting any data files.
--
-- REAL-WORLD SCENARIO
-- -------------------
-- A financial institution migrates legacy column names (amt, ccy, acct_num)
-- to IFRS-standardized names (transaction_amount, currency_code,
-- account_number) without any data rewrite or downtime.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify field-ID stability in metadata with:
--   python3 verify_iceberg_metadata.py <table_data_path>/financial_transactions -v
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline Data — 24 Transactions with Legacy Column Names
-- ============================================================================

ASSERT ROW_COUNT = 24
SELECT * FROM {{zone_name}}.iceberg_demos.financial_transactions ORDER BY txn_id;


-- ============================================================================
-- Query 1: Baseline — Revenue by Transaction Type (Legacy Names)
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE total_amt = 4776.50 WHERE txn_type = 'checking'
ASSERT VALUE total_amt = 1450.48 WHERE txn_type = 'credit'
ASSERT VALUE total_amt = 49500.00 WHERE txn_type = 'investment'
ASSERT VALUE total_amt = 17700.00 WHERE txn_type = 'savings'
SELECT
    txn_type,
    COUNT(*) AS txn_count,
    ROUND(SUM(amt), 2) AS total_amt
FROM {{zone_name}}.iceberg_demos.financial_transactions
GROUP BY txn_type
ORDER BY txn_type;


-- ============================================================================
-- Query 2: Baseline — Revenue by Currency (Legacy Names)
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE total_amt = 25340.00 WHERE ccy = 'EUR'
ASSERT VALUE total_amt = 14395.99 WHERE ccy = 'GBP'
ASSERT VALUE total_amt = 33690.99 WHERE ccy = 'USD'
SELECT
    ccy,
    COUNT(*) AS txn_count,
    ROUND(SUM(amt), 2) AS total_amt
FROM {{zone_name}}.iceberg_demos.financial_transactions
GROUP BY ccy
ORDER BY ccy;


-- ============================================================================
-- LEARN: Rename Step 1 — amt → transaction_amount (Version 2)
-- ============================================================================
-- RENAME COLUMN is a metadata-only operation. No data files are rewritten.
-- The Iceberg field-id for this column stays the same; only the name changes
-- in both the Delta column mapping and the Iceberg schema entry.

ALTER TABLE {{zone_name}}.iceberg_demos.financial_transactions RENAME COLUMN amt TO transaction_amount;


-- ============================================================================
-- LEARN: Rename Step 2 — ccy → currency_code (Version 3)
-- ============================================================================

ALTER TABLE {{zone_name}}.iceberg_demos.financial_transactions RENAME COLUMN ccy TO currency_code;


-- ============================================================================
-- LEARN: Rename Step 3 — acct_num → account_number (Version 4)
-- ============================================================================

ALTER TABLE {{zone_name}}.iceberg_demos.financial_transactions RENAME COLUMN acct_num TO account_number;


-- ============================================================================
-- Query 3: Same Data, New Names — Revenue by Transaction Type
-- ============================================================================
-- Identical results to Query 1, but using the renamed column.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_transaction_amount = 4776.50 WHERE txn_type = 'checking'
ASSERT VALUE total_transaction_amount = 1450.48 WHERE txn_type = 'credit'
ASSERT VALUE total_transaction_amount = 49500.00 WHERE txn_type = 'investment'
ASSERT VALUE total_transaction_amount = 17700.00 WHERE txn_type = 'savings'
SELECT
    txn_type,
    COUNT(*) AS txn_count,
    ROUND(SUM(transaction_amount), 2) AS total_transaction_amount
FROM {{zone_name}}.iceberg_demos.financial_transactions
GROUP BY txn_type
ORDER BY txn_type;


-- ============================================================================
-- Query 4: Same Data, New Names — Revenue by Currency
-- ============================================================================
-- Identical results to Query 2, but using currency_code and account_number.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_transaction_amount = 25340.00 WHERE currency_code = 'EUR'
ASSERT VALUE total_transaction_amount = 14395.99 WHERE currency_code = 'GBP'
ASSERT VALUE total_transaction_amount = 33690.99 WHERE currency_code = 'USD'
SELECT
    currency_code,
    COUNT(*) AS txn_count,
    ROUND(SUM(transaction_amount), 2) AS total_transaction_amount
FROM {{zone_name}}.iceberg_demos.financial_transactions
GROUP BY currency_code
ORDER BY currency_code;


-- ============================================================================
-- Query 5: Verify All Renamed Columns Are Accessible
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_accounts = 12
ASSERT VALUE grand_total = 73426.98
SELECT
    COUNT(DISTINCT account_number) AS distinct_accounts,
    COUNT(DISTINCT currency_code) AS distinct_currencies,
    ROUND(SUM(transaction_amount), 2) AS grand_total
FROM {{zone_name}}.iceberg_demos.financial_transactions;


-- ============================================================================
-- LEARN: Insert New Rows Using Renamed Columns (Version 5)
-- ============================================================================
-- New transactions inserted with the standardized IFRS column names.

INSERT INTO {{zone_name}}.iceberg_demos.financial_transactions VALUES
    (25, 'CHK-10004', 'checking',     950.00, 'USD', '2024-04-10', 'BR-NYC'),
    (26, 'SAV-20004', 'savings',     3200.00, 'EUR', '2024-04-12', 'BR-LON'),
    (27, 'CRD-30004', 'credit',       410.00, 'GBP', '2024-04-15', 'BR-LON'),
    (28, 'INV-40004', 'investment',  7000.00, 'USD', '2024-04-18', 'BR-CHI');


-- ============================================================================
-- Query 6: All 28 Rows — Revenue by Type After Insert
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE total_transaction_amount = 5726.50 WHERE txn_type = 'checking'
ASSERT VALUE total_transaction_amount = 1860.48 WHERE txn_type = 'credit'
ASSERT VALUE total_transaction_amount = 56500.00 WHERE txn_type = 'investment'
ASSERT VALUE total_transaction_amount = 20900.00 WHERE txn_type = 'savings'
SELECT
    txn_type,
    COUNT(*) AS txn_count,
    ROUND(SUM(transaction_amount), 2) AS total_transaction_amount
FROM {{zone_name}}.iceberg_demos.financial_transactions
GROUP BY txn_type
ORDER BY txn_type;


-- ============================================================================
-- Query 7: Time Travel — Read Version 1 (Original Column Names)
-- ============================================================================
-- Reading the pre-rename version. After rename, the old column names may
-- still appear in the old version's schema. We select explicitly by the
-- original column names to verify the data is unchanged.

ASSERT ROW_COUNT = 24
SELECT
    txn_id, acct_num, txn_type, amt, ccy, txn_date, branch_code
FROM {{zone_name}}.iceberg_demos.financial_transactions VERSION AS OF 1
ORDER BY txn_id;


-- ============================================================================
-- Query 8: Version History — Rename Operations Trail
-- ============================================================================
-- The history shows the progression of rename operations and the insert.

ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.financial_transactions;


-- ============================================================================
-- VERIFY: Comprehensive Final-State Validation
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_transactions = 28
ASSERT VALUE total_amount = 84986.98
ASSERT VALUE distinct_types = 4
ASSERT VALUE distinct_currencies = 3
SELECT
    COUNT(*) AS total_transactions,
    ROUND(SUM(transaction_amount), 2) AS total_amount,
    COUNT(DISTINCT txn_type) AS distinct_types,
    COUNT(DISTINCT currency_code) AS distinct_currencies
FROM {{zone_name}}.iceberg_demos.financial_transactions;


-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents the renamed columns — Iceberg
-- engines resolve by field-id, so the renamed column names appear
-- transparently without data file rewrites.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.financial_transactions_iceberg
USING ICEBERG
LOCATION '{{data_path}}/financial_transactions';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.financial_transactions_iceberg TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg_demos.financial_transactions_iceberg;


-- ============================================================================
-- Iceberg Verify 1: Data Integrity — Spot-Check Rows via Renamed Columns
-- ============================================================================
-- Verify that specific rows are readable through Iceberg metadata and that
-- the renamed columns (transaction_amount, currency_code, account_number)
-- resolve correctly by field-id.

ASSERT ROW_COUNT = 28
ASSERT VALUE transaction_amount = 1500.00 WHERE txn_id = 1
ASSERT VALUE currency_code = 'USD' WHERE txn_id = 1
ASSERT VALUE account_number = 'CHK-10001' WHERE txn_id = 1
ASSERT VALUE transaction_amount = 7500.00 WHERE txn_id = 8
ASSERT VALUE currency_code = 'EUR' WHERE txn_id = 8
ASSERT VALUE transaction_amount = 12000.00 WHERE txn_id = 23
ASSERT VALUE currency_code = 'EUR' WHERE txn_id = 23
ASSERT VALUE transaction_amount = 950.00 WHERE txn_id = 25
ASSERT VALUE account_number = 'CHK-10004' WHERE txn_id = 25
ASSERT VALUE transaction_amount = 7000.00 WHERE txn_id = 28
ASSERT VALUE currency_code = 'USD' WHERE txn_id = 28
SELECT * FROM {{zone_name}}.iceberg_demos.financial_transactions_iceberg ORDER BY txn_id;


-- ============================================================================
-- Iceberg Verify 2: Revenue Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_transactions = 28
ASSERT VALUE total_amount = 84986.98
SELECT
    COUNT(*) AS total_transactions,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM {{zone_name}}.iceberg_demos.financial_transactions_iceberg;


-- ============================================================================
-- Iceberg Verify 3: Per-Type Breakdown — Matches Delta Query 6
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE total_transaction_amount = 5726.50 WHERE txn_type = 'checking'
ASSERT VALUE total_transaction_amount = 1860.48 WHERE txn_type = 'credit'
ASSERT VALUE total_transaction_amount = 56500.00 WHERE txn_type = 'investment'
ASSERT VALUE total_transaction_amount = 20900.00 WHERE txn_type = 'savings'
SELECT
    txn_type,
    COUNT(*) AS txn_count,
    ROUND(SUM(transaction_amount), 2) AS total_transaction_amount
FROM {{zone_name}}.iceberg_demos.financial_transactions_iceberg
GROUP BY txn_type
ORDER BY txn_type;
