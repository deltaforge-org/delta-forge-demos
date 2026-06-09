# Delta Change Data Feed — Track Row-Level Changes

Demonstrates Change Data Feed (CDF) for tracking row-level changes across
table versions after tier upgrades, new accounts, closures, and balance
adjustments.

## Data Story

A fintech platform manages 40 customer accounts. High-balance customers are
upgraded to a 'gold' tier, 8 new customers join, 3 low-balance accounts are
closed, and 5 premium gold customers receive a 20% balance bonus. With CDF
enabled, every change is tracked at the row level for compliance auditing.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `customer_accounts` | Delta Table (CDF) | 45 (final) | Customer accounts with change tracking |

## Schema

**customer_accounts:** `id INT, name VARCHAR, email VARCHAR, tier VARCHAR, balance DOUBLE, status VARCHAR, created_date VARCHAR`

## Version History

- **V0:** INSERT 40 customer accounts (silver/bronze tiers)
- **V1:** UPDATE 10 customers → 'gold' tier (top balances)
- **V2:** INSERT 8 new customers
- **V3:** DELETE 3 closed accounts (low balance)
- **V4:** UPDATE 5 gold customer balances +20%

## Verification

8 automated PASS/FAIL checks verify: 45 total rows, 10 gold tier, closed
accounts removed, 8 new customers present, premium balance adjustments,
silver count, all active, and unchanged balances for non-adjusted customers.
