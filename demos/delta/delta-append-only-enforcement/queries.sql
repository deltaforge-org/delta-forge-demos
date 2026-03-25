-- ============================================================================
-- Delta Append-Only Enforcement — Educational Queries
-- ============================================================================
-- WHAT: The delta.appendOnly table property tells Delta to reject any
--       commit that contains "remove" file actions — blocking UPDATE and
--       DELETE at the protocol level.
-- WHY:  Financial regulators require immutable transaction ledgers. A
--       configuration flag alone is not enough — the enforcement must be
--       provable. This demo systematically tests the boundary between
--       allowed and forbidden operations.
-- HOW:  We compare two identical tables: one append-only (compliance_ledger)
--       and one mutable (mutable_ledger). Mutations succeed on the mutable
--       table but fail on the append-only table, proving the guard works.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Both Ledgers Start Identical
-- ============================================================================
-- Both tables were loaded with the same 20 financial transactions.
-- Let's confirm they match before testing enforcement.

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
-- Auditors need per-account net positions. Both ledgers should show
-- identical balances at this point.

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
-- ACTION: UPDATE on Mutable Ledger — This Should Succeed
-- ============================================================================
-- The mutable_ledger has no append-only restriction. We apply a 5% fee
-- adjustment to ACC-1001 deposits. This is the kind of retroactive
-- modification that regulators want to prevent on official ledgers.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.mutable_ledger
SET amount = ROUND(amount * 1.05, 2)
WHERE account_id = 'ACC-1001' AND txn_type = 'DEPOSIT';


-- Verify the mutable table was modified
ASSERT VALUE new_balance = 10530.0
ASSERT ROW_COUNT = 1
SELECT ROUND(SUM(amount), 2) AS new_balance
FROM {{zone_name}}.delta_demos.mutable_ledger
WHERE account_id = 'ACC-1001';


-- ============================================================================
-- LEARN: UPDATE on Compliance Ledger — This MUST Be Rejected
-- ============================================================================
-- The compliance_ledger has delta.appendOnly = true. The same UPDATE that
-- succeeded on the mutable table MUST fail here. Delta rejects the commit
-- because UPDATE requires rewriting data files (remove + add), which
-- violates the append-only constraint.
--
-- Expected: error containing "append-only" or "only supported operation"

ASSERT ROW_COUNT = 0
UPDATE {{zone_name}}.delta_demos.compliance_ledger
SET amount = ROUND(amount * 1.05, 2)
WHERE account_id = 'ACC-1001' AND txn_type = 'DEPOSIT';


-- ============================================================================
-- LEARN: DELETE on Mutable Ledger — This Should Succeed
-- ============================================================================
-- Remove two transfer transactions from the mutable ledger. This is the
-- kind of record tampering that append-only mode prevents.

ASSERT ROW_COUNT = 2
DELETE FROM {{zone_name}}.delta_demos.mutable_ledger
WHERE txn_id IN (4, 19);


-- Verify the mutable table now has 18 rows
ASSERT VALUE remaining = 18
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS remaining FROM {{zone_name}}.delta_demos.mutable_ledger;


-- ============================================================================
-- LEARN: DELETE on Compliance Ledger — This MUST Be Rejected
-- ============================================================================
-- The same DELETE that succeeded on the mutable table MUST fail on the
-- compliance ledger. Deleting financial records would be a regulatory
-- violation — the protocol enforces this automatically.

ASSERT ROW_COUNT = 0
DELETE FROM {{zone_name}}.delta_demos.compliance_ledger
WHERE txn_id IN (4, 19);


-- ============================================================================
-- EXPLORE: Compliance Ledger Is Untouched
-- ============================================================================
-- After both rejected mutations, the compliance ledger should still have
-- exactly 20 rows with the original balances — nothing changed.

ASSERT VALUE ledger_count = 20
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS ledger_count FROM {{zone_name}}.delta_demos.compliance_ledger;


-- The original account balances are preserved
ASSERT VALUE balance = 9900.0 WHERE account_id = 'ACC-1001'
ASSERT ROW_COUNT = 5
SELECT account_id,
       ROUND(SUM(amount), 2) AS balance
FROM {{zone_name}}.delta_demos.compliance_ledger
GROUP BY account_id
ORDER BY account_id;


-- ============================================================================
-- ACTION: INSERT on Compliance Ledger — This IS Allowed
-- ============================================================================
-- Append-only does NOT block INSERTs. New transactions can always be added.
-- This is the correct pattern: the ledger grows monotonically.

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
-- The ledger grew from 20 to 25 rows. It can never shrink. This is the
-- fundamental guarantee that auditors verify.

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
