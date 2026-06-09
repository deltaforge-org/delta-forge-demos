-- ============================================================================
-- Delta Constraints & Table Properties — Educational Queries
-- ============================================================================
-- WHAT: TBLPROPERTIES are key-value metadata stored in the Delta transaction
--       log, used for CHECK constraints and behavioral configuration.
-- WHY:  CHECK constraints (delta.constraints.*) enforce data invariants at
--       write time, while delta.appendOnly prevents mutations — ensuring
--       audit logs remain immutable and financial data stays valid.
-- HOW:  These properties are recorded in "metaData" actions in the Delta log.
--       Every writer must evaluate CHECK constraints before committing.
--       appendOnly tables reject any commit containing "remove" actions.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Inspect the Invoices Table
-- ============================================================================
-- The invoices table was created with three CHECK constraints:
--   delta.constraints.amount_positive  = 'amount > 0'
--   delta.constraints.tax_non_negative = 'tax >= 0'
--   delta.constraints.total_positive   = 'total > 0'
-- Every row in this table must satisfy all three expressions.

ASSERT ROW_COUNT = 10
SELECT id, customer, amount, tax, total, status, created_date
FROM {{zone_name}}.delta_demos.invoices
ORDER BY id
LIMIT 10;


-- ============================================================================
-- LEARN: How CHECK Constraints Protect Data Integrity
-- ============================================================================
-- Let's verify that all constraint expressions hold. In a production Delta
-- table, any INSERT or UPDATE that produces a row with amount <= 0, tax < 0,
-- or total <= 0 would be rejected at the protocol level — the transaction
-- would fail and no data would be written.
--
-- We also check that tax = 10% of amount and total = amount + tax, which
-- are arithmetic consistency rules the data was designed to follow.

-- Verify all CHECK constraints hold: zero violations across all rules
ASSERT VALUE amount_violations = 0
ASSERT VALUE tax_violations = 0
ASSERT VALUE total_violations = 0
ASSERT VALUE tax_rate_mismatches = 0
ASSERT VALUE total_formula_mismatches = 0
ASSERT VALUE total_invoices = 30
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) AS total_invoices,
    COUNT(*) FILTER (WHERE amount <= 0) AS amount_violations,
    COUNT(*) FILTER (WHERE tax < 0) AS tax_violations,
    COUNT(*) FILTER (WHERE total <= 0) AS total_violations,
    COUNT(*) FILTER (WHERE ABS(tax - amount * 0.10) > 0.01) AS tax_rate_mismatches,
    COUNT(*) FILTER (WHERE ABS(total - (amount + tax)) > 0.01) AS total_formula_mismatches
FROM {{zone_name}}.delta_demos.invoices;


-- ============================================================================
-- LEARN: The Append-Only Property (delta.appendOnly)
-- ============================================================================
-- The event_log table was created with TBLPROPERTIES ('delta.appendOnly' = 'true').
-- This means:
--   - INSERT is allowed (appending new events)
--   - UPDATE and DELETE are forbidden (no mutation of existing records)
--   - This is enforced by the Delta protocol: any commit with "remove" file
--     actions is rejected
--
-- Append-only is ideal for audit logs, compliance records, and event streams
-- where immutability is a business requirement.

-- Verify event log contains at least 10 entries (append-only, so count can only grow)
ASSERT ROW_COUNT = 10
SELECT id, event_type, payload, created_at
FROM {{zone_name}}.delta_demos.event_log
ORDER BY id
LIMIT 10;


-- ============================================================================
-- EXPLORE: Event Type Distribution in the Immutable Log
-- ============================================================================
-- Since the event log is append-only, these counts can only grow over time —
-- no events can be deleted or modified. This property makes the log suitable
-- for compliance auditing and forensic analysis.

ASSERT ROW_COUNT = 8
ASSERT VALUE event_count = 10 WHERE event_type = 'user.login'
ASSERT VALUE event_count = 9 WHERE event_type = 'user.logout'
ASSERT VALUE event_count = 8 WHERE event_type = 'order.created'
SELECT event_type,
       COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.event_log
GROUP BY event_type
ORDER BY event_count DESC;


-- ============================================================================
-- EXPLORE: Invoice Status and Revenue Breakdown
-- ============================================================================
-- The constraints ensure every invoice has a positive amount and total,
-- so aggregations are guaranteed to produce meaningful results — there
-- are no negative amounts or zero-total invoices polluting the sums.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_revenue = 5456.0 WHERE status = 'paid'
ASSERT VALUE invoice_count = 18 WHERE status = 'paid'
ASSERT VALUE total_revenue = 1260.05 WHERE status = 'pending'
ASSERT VALUE total_revenue = 737.0 WHERE status = 'overdue'
SELECT status,
       COUNT(*) AS invoice_count,
       ROUND(SUM(total), 2) AS total_revenue,
       ROUND(AVG(amount), 2) AS avg_amount
FROM {{zone_name}}.delta_demos.invoices
GROUP BY status
ORDER BY total_revenue DESC;


-- ============================================================================
-- LEARN: Tracing an Order Lifecycle in the Event Log
-- ============================================================================
-- The append-only event log captures the full lifecycle of orders. Because
-- no events can be modified or deleted, you get a guaranteed-complete history.
-- Let's trace order 1001 from creation through delivery.

ASSERT ROW_COUNT = 4
SELECT id, event_type, payload, created_at
FROM {{zone_name}}.delta_demos.event_log
WHERE payload LIKE '%1001%'
ORDER BY created_at;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify invoice count
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.invoices;

-- Verify all amounts are positive
ASSERT VALUE amount_violations = 0
SELECT COUNT(*) FILTER (WHERE amount <= 0) AS amount_violations FROM {{zone_name}}.delta_demos.invoices;

-- Verify all tax values are non-negative
ASSERT VALUE tax_violations = 0
SELECT COUNT(*) FILTER (WHERE tax < 0) AS tax_violations FROM {{zone_name}}.delta_demos.invoices;

-- Verify all totals are positive
ASSERT VALUE total_violations = 0
SELECT COUNT(*) FILTER (WHERE total <= 0) AS total_violations FROM {{zone_name}}.delta_demos.invoices;

-- Verify tax rate consistency (10% of amount)
ASSERT VALUE tax_rate_mismatches = 0
SELECT COUNT(*) FILTER (WHERE ABS(tax - amount * 0.10) > 0.01) AS tax_rate_mismatches FROM {{zone_name}}.delta_demos.invoices;

-- Verify total equals amount plus tax
ASSERT VALUE total_formula_mismatches = 0
SELECT COUNT(*) FILTER (WHERE ABS(total - (amount + tax)) > 0.01) AS total_formula_mismatches FROM {{zone_name}}.delta_demos.invoices;

-- Verify event log count
ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.delta_demos.event_log;

-- Verify paid invoice revenue (proves constraint-clean data yields correct aggregates)
ASSERT VALUE total_revenue = 5456.0 WHERE status = 'paid'
ASSERT VALUE invoice_count = 18 WHERE status = 'paid'
SELECT status, COUNT(*) AS invoice_count, ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.invoices
GROUP BY status;
