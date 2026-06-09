# Delta Constraints & Table Properties

Demonstrates CHECK constraints for data quality and TBLPROPERTIES for
table-level behavior settings.

## Data Story

An accounting system enforces that all invoice amounts are positive and
tax is non-negative via CHECK constraints. A separate event log table
uses append-only mode to ensure immutability for audit compliance.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `invoices` | Delta Table | 30 | Invoices with CHECK constraints |
| `event_log` | Delta Table | 50 | Append-only audit log |

## Constraints & Properties

**invoices:**
- `amount_positive`: amount > 0
- `tax_non_negative`: tax >= 0
- `total_positive`: total > 0

**event_log:**
- `delta.appendOnly = 'true'` — prevents UPDATE and DELETE

## Verification

7 automated PASS/FAIL checks verify constraint satisfaction, arithmetic
consistency, and row counts.
