-- ============================================================================
-- Delta Append-Only Ledger — Educational Queries
-- ============================================================================
-- WHAT: The delta.appendOnly table property declares that a Delta table
--       should only receive INSERT operations — no UPDATE or DELETE.
-- WHY:  Financial regulators require immutable transaction ledgers where
--       records can never be altered or deleted after the fact. The
--       append-only property marks this intent in the Delta protocol.
-- HOW:  We create an append-only ledger and a mutable control table with
--       identical data. We verify the append-only configuration via
--       DESCRIBE DETAIL and SHOW TBLPROPERTIES, append new batches to
--       prove the count-only-grows invariant, and use DESCRIBE HISTORY
--       to audit the operation timeline. Meanwhile, the mutable table
--       demonstrates what happens when records CAN be modified.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Both Ledgers Start Identical
-- ============================================================================
-- Both tables were loaded with the same 20 financial transactions.
-- Let's confirm they match before we diverge their histories.

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 10 WHERE txn_type = 'DEPOSIT'
ASSERT VALUE txn_count = 6 WHERE txn_type = 'WITHDRAWAL'
ASSERT VALUE txn_count = 4 WHERE txn_type = 'TRANSFER'
SELECT txn_type,
       COUNT(*) AS txn_count,
       ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.delta_demos.compliance_ledger
GROUP BY txn_type
ORDER BY txn_count DESC;


-- ============================================================================
-- EXPLORE: Account Balances — The Regulatory View
-- ============================================================================
-- Auditors need per-account net positions. Both ledgers show identical
-- balances at this point.

ASSERT ROW_COUNT = 5
ASSERT VALUE balance = 9900.0 WHERE account_id = 'ACC-1001'
ASSERT VALUE balance = 7100.0 WHERE account_id = 'ACC-1002'
ASSERT VALUE balance = 13900.0 WHERE account_id = 'ACC-1003'
ASSERT VALUE balance = 14600.0 WHERE account_id = 'ACC-1004'
ASSERT VALUE balance = 3300.0 WHERE account_id = 'ACC-1005'
SELECT account_id,
       COUNT(*) AS txn_count,
       ROUND(SUM(amount), 2) AS balance
FROM {{zone_name}}.delta_demos.compliance_ledger
GROUP BY account_id
ORDER BY account_id;


-- ============================================================================
-- LEARN: DESCRIBE DETAIL — Verify Append-Only Configuration
-- ============================================================================
-- DESCRIBE DETAIL reveals the protocol metadata. For the compliance_ledger,
-- the table_features should include append-only support, confirming the
-- table was created with the correct configuration.

ASSERT ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.compliance_ledger;


-- ============================================================================
-- LEARN: SHOW TBLPROPERTIES — The Configuration Source of Truth
-- ============================================================================
-- SHOW TBLPROPERTIES confirms delta.appendOnly = true is set. This property
-- is stored in the Delta transaction log's metaData action and persists
-- across all future operations.

ASSERT ROW_COUNT >= 1
SHOW TBLPROPERTIES {{zone_name}}.delta_demos.compliance_ledger;


-- ============================================================================
-- ACTION: Mutable Table — UPDATE Modifies History (The Problem)
-- ============================================================================
-- On the mutable table, we can freely modify records. Here we apply a 5%
-- fee adjustment to ACC-1001 deposits. This is exactly the kind of
-- retroactive change that regulators want to prevent on official ledgers.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.mutable_ledger
SET amount = ROUND(amount * 1.05, 2)
WHERE account_id = 'ACC-1001' AND txn_type = 'DEPOSIT';


-- Verify the mutable table was modified — ACC-1001 balance changed
ASSERT VALUE new_balance = 10530.0
ASSERT ROW_COUNT = 1
SELECT ROUND(SUM(amount), 2) AS new_balance
FROM {{zone_name}}.delta_demos.mutable_ledger
WHERE account_id = 'ACC-1001';


-- ============================================================================
-- ACTION: Mutable Table — DELETE Removes Records (The Problem)
-- ============================================================================
-- We can also delete records from the mutable table. This removes the
-- audit trail entirely — a compliance nightmare.

ASSERT ROW_COUNT = 2
DELETE FROM {{zone_name}}.delta_demos.mutable_ledger
WHERE txn_id IN (4, 19);


-- Verify the mutable table now has 18 rows — 2 transactions vanished
ASSERT VALUE remaining = 18
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS remaining FROM {{zone_name}}.delta_demos.mutable_ledger;


-- ============================================================================
-- EXPLORE: Compliance Ledger — Still Pristine
-- ============================================================================
-- While the mutable table was modified and records deleted, the compliance
-- ledger remains untouched — all 20 original transactions are intact.

ASSERT VALUE ledger_count = 20
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS ledger_count FROM {{zone_name}}.delta_demos.compliance_ledger;


-- Original account balances are preserved
ASSERT VALUE balance = 9900.0 WHERE account_id = 'ACC-1001'
ASSERT ROW_COUNT = 5
SELECT account_id,
       ROUND(SUM(amount), 2) AS balance
FROM {{zone_name}}.delta_demos.compliance_ledger
GROUP BY account_id
ORDER BY account_id;


-- ============================================================================
-- ACTION: Append Batch 1 — Q1 Settlement Transactions
-- ============================================================================
-- Append-only tables grow monotonically. New transactions are added via
-- INSERT — the only operation that should touch a compliance ledger.

ASSERT ROW_COUNT = 5
INSERT INTO {{zone_name}}.delta_demos.compliance_ledger VALUES
    (21, 'ACC-1001', 'DEPOSIT',    2800.00,  'USD', 'Q1 Settlement',      '2025-01-21', 'REF-20250121-001'),
    (22, 'ACC-1002', 'DEPOSIT',    6100.00,  'USD', 'Q1 Settlement',      '2025-01-21', 'REF-20250121-002'),
    (23, 'ACC-1003', 'WITHDRAWAL', -1500.00, 'USD', 'Regulatory Fee',     '2025-01-22', 'REF-20250122-001'),
    (24, 'ACC-1005', 'DEPOSIT',    3900.00,  'USD', 'Q1 Settlement',      '2025-01-22', 'REF-20250122-002'),
    (25, 'ACC-1004', 'DEPOSIT',    8200.00,  'USD', 'Q1 Settlement',      '2025-01-23', 'REF-20250123-001');


-- ============================================================================
-- EXPLORE: Count Only Grows — The Immutability Proof
-- ============================================================================
-- The ledger grew from 20 to 25 rows. Under append-only discipline, this
-- count can never decrease. This is the fundamental invariant.

ASSERT VALUE new_count = 25
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS new_count FROM {{zone_name}}.delta_demos.compliance_ledger;


-- Updated account balances after the new batch
ASSERT VALUE balance = 12700.0 WHERE account_id = 'ACC-1001'
ASSERT VALUE balance = 13200.0 WHERE account_id = 'ACC-1002'
ASSERT VALUE balance = 12400.0 WHERE account_id = 'ACC-1003'
ASSERT VALUE balance = 22800.0 WHERE account_id = 'ACC-1004'
ASSERT VALUE balance = 7200.0 WHERE account_id = 'ACC-1005'
ASSERT ROW_COUNT = 5
SELECT account_id,
       ROUND(SUM(amount), 2) AS balance
FROM {{zone_name}}.delta_demos.compliance_ledger
GROUP BY account_id
ORDER BY account_id;


-- ============================================================================
-- LEARN: DESCRIBE HISTORY — The Complete Audit Trail
-- ============================================================================
-- DESCRIBE HISTORY shows every committed transaction on the ledger.
-- For the compliance ledger, we should see only CREATE TABLE and INSERT
-- operations — never UPDATE or DELETE. This is the audit evidence.

ASSERT ROW_COUNT >= 3
DESCRIBE HISTORY {{zone_name}}.delta_demos.compliance_ledger;


-- ============================================================================
-- EXPLORE: Divergence — Compare Mutable vs Immutable
-- ============================================================================
-- The two tables started identical but have diverged. The compliance ledger
-- has 25 rows (20 + 5 appended) with all original records intact. The
-- mutable table has 18 rows (20 - 2 deleted) with modified amounts.

ASSERT VALUE ledger_rows = 25 WHERE table_type = 'compliance_ledger'
ASSERT VALUE ledger_rows = 18 WHERE table_type = 'mutable_ledger'
ASSERT ROW_COUNT = 2
SELECT 'compliance_ledger' AS table_type,
       COUNT(*) AS ledger_rows
FROM {{zone_name}}.delta_demos.compliance_ledger
UNION ALL
SELECT 'mutable_ledger' AS table_type,
       COUNT(*) AS ledger_rows
FROM {{zone_name}}.delta_demos.mutable_ledger;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify compliance ledger has 25 rows after INSERT
ASSERT VALUE final_count = 25
SELECT COUNT(*) AS final_count FROM {{zone_name}}.delta_demos.compliance_ledger;

-- Verify mutable ledger has 18 rows after UPDATE + DELETE
ASSERT VALUE mutable_count = 18
SELECT COUNT(*) AS mutable_count FROM {{zone_name}}.delta_demos.mutable_ledger;

-- Verify total net position across all compliance accounts
ASSERT VALUE total_net = 68300.0
SELECT ROUND(SUM(amount), 2) AS total_net FROM {{zone_name}}.delta_demos.compliance_ledger;

-- Verify compliance ledger still has 3 transaction types
ASSERT VALUE txn_types = 3
SELECT COUNT(DISTINCT txn_type) AS txn_types FROM {{zone_name}}.delta_demos.compliance_ledger;

-- Verify all 5 accounts are represented
ASSERT VALUE account_count = 5
SELECT COUNT(DISTINCT account_id) AS account_count FROM {{zone_name}}.delta_demos.compliance_ledger;

-- Verify original 20 transactions were not modified (sum of first 20)
ASSERT VALUE original_net = 48800.0
SELECT ROUND(SUM(amount), 2) AS original_net FROM {{zone_name}}.delta_demos.compliance_ledger WHERE txn_id <= 20;
