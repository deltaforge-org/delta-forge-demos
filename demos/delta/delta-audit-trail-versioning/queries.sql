-- ============================================================================
-- Delta Audit Trail — Native Version-Based Compliance — Educational Queries
-- ============================================================================
-- WHAT: Uses Delta's transaction log and time travel as a tamper-proof audit
--       trail for financial compliance, instead of manual audit columns.
-- WHY:  Regulators require immutable event histories. Delta's native versioning
--       provides this without application-level bookkeeping — every commit is
--       a numbered, timestamped checkpoint that can be queried at will.
-- HOW:  DESCRIBE HISTORY shows every commit. VERSION AS OF reconstructs past
--       states. MERGE ingests late-arriving data. Together they form a complete
--       compliance system.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Living Audit Trail
-- ============================================================================
-- Every event in the system — opens, deposits, withdrawals, transfers,
-- freezes, and closures — lives in a single append-only event table.
-- The 6 event types capture the full lifecycle of each account.

ASSERT ROW_COUNT = 6
ASSERT VALUE event_count = 18 WHERE event_type = 'deposit'
ASSERT VALUE event_count = 10 WHERE event_type = 'open'
SELECT event_type,
       COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.compliance_events
GROUP BY event_type
ORDER BY event_count DESC;


-- ============================================================================
-- EXPLORE: Tracing a Single Account Through Delta Versions
-- ============================================================================
-- Follow ACCT-1001 (Meridian Holdings) from opening through all activity.
-- Each row was written in a different Delta version, creating an immutable
-- chain of custody. No row can be altered without creating a new version.

ASSERT ROW_COUNT = 5
ASSERT VALUE event_type = 'open' WHERE event_id = 1
ASSERT VALUE balance = 305000.00 WHERE event_id = 38
SELECT event_id, account_id, event_type, amount, balance, officer, event_date
FROM {{zone_name}}.delta_demos.compliance_events
WHERE account_id = 'ACCT-1001'
ORDER BY event_date;


-- ============================================================================
-- LEARN: Delta's Transaction Log IS the Audit Trail
-- ============================================================================
-- DESCRIBE HISTORY reveals every commit to this table — who wrote it, when,
-- and what operation was performed. This is the native audit log that
-- regulators can inspect. No application code needed.
--
-- Expected versions:
--   V0: CREATE TABLE
--   V1: INSERT 20 rows (account openings + deposits)
--   V2: INSERT 12 rows (transactions)
--   V3: INSERT 5 rows (compliance events)
--   V4: INSERT 5 rows (late-arriving batch)

-- Non-deterministic: commit timestamps are set at write time
ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.delta_demos.compliance_events;


-- ============================================================================
-- LEARN: Time Travel — Reconstruct Past State for Auditors
-- ============================================================================
-- VERSION AS OF lets auditors see the exact state of the table at any commit.
-- At Version 1, only the initial 20 rows (account openings) existed.
-- At the current version, all 42 events are present.
-- This proves no historical data was altered — only appended.

ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.compliance_events VERSION AS OF 1;


-- ============================================================================
-- LEARN: Cross-Version Comparison — What Changed Between Audits
-- ============================================================================
-- An auditor reviewing quarterly activity needs to know: "What events were
-- added between the January snapshot (V1) and now?" This query counts
-- events that exist in the current version but not in V1.

ASSERT VALUE new_events = 22
SELECT COUNT(*) AS new_events
FROM {{zone_name}}.delta_demos.compliance_events current_state
WHERE current_state.event_id NOT IN (
    SELECT event_id FROM {{zone_name}}.delta_demos.compliance_events VERSION AS OF 1
);


-- ============================================================================
-- EXPLORE: Branch Activity — Geographic Compliance View
-- ============================================================================
-- Regulators often audit by branch. This shows total activity and monetary
-- flow per branch — useful for detecting unusual concentrations.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_events = 13 WHERE branch = 'downtown'
ASSERT VALUE total_events = 12 WHERE branch = 'midtown'
SELECT branch,
       COUNT(*) AS total_events,
       COUNT(DISTINCT account_id) AS accounts,
       SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) AS total_amount
FROM {{zone_name}}.delta_demos.compliance_events
GROUP BY branch
ORDER BY total_events DESC;


-- ============================================================================
-- EXPLORE: Officer Accountability
-- ============================================================================
-- Every event is tied to an officer. This breakdown supports internal audits
-- and segregation-of-duties reviews.

ASSERT VALUE actions = 12 WHERE officer = 'j.chen'
ASSERT VALUE accounts_handled = 3 WHERE officer = 'j.chen'
ASSERT ROW_COUNT = 5
SELECT officer,
       COUNT(*) AS actions,
       COUNT(DISTINCT account_id) AS accounts_handled
FROM {{zone_name}}.delta_demos.compliance_events
GROUP BY officer
ORDER BY actions DESC;


-- ============================================================================
-- LEARN: Point-in-Time Account Balances via Time Travel
-- ============================================================================
-- Reconstruct the opening-day balance for every account using VERSION AS OF 1.
-- This is the January snapshot an auditor would certify.

ASSERT ROW_COUNT = 10
ASSERT VALUE latest_balance = 250000.00 WHERE account_id = 'ACCT-1001'
ASSERT VALUE latest_balance = 1200000.00 WHERE account_id = 'ACCT-1008'
SELECT account_id,
       MAX(balance) AS latest_balance
FROM {{zone_name}}.delta_demos.compliance_events VERSION AS OF 1
GROUP BY account_id
ORDER BY account_id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total rows: 42 events across all versions
ASSERT VALUE cnt = 42
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.compliance_events;

-- Verify distinct accounts: 10 commercial accounts
ASSERT VALUE cnt = 10
SELECT COUNT(DISTINCT account_id) AS cnt FROM {{zone_name}}.delta_demos.compliance_events;

-- Verify event type count: 6 distinct event types
ASSERT VALUE cnt = 6
SELECT COUNT(DISTINCT event_type) AS cnt FROM {{zone_name}}.delta_demos.compliance_events;

-- Verify events with monetary amounts: 30 events have non-null amount
ASSERT VALUE cnt = 30
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.compliance_events WHERE amount IS NOT NULL;

-- Verify Version 1 snapshot: 20 rows at the opening state
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.compliance_events VERSION AS OF 1;

-- Verify total monetary flow: sum of all amounts
ASSERT VALUE total = 6935000.00
SELECT SUM(amount) AS total FROM {{zone_name}}.delta_demos.compliance_events;
