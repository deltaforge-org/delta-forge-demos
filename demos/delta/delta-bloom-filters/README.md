# Delta Bloom Filters — Probabilistic Index for Fast Lookups

Demonstrates how bloom filter indexes on Delta tables accelerate point
lookups by skipping data files that cannot contain a given value.

## Data Story

A payment processing system uses bloom filter indexes on transaction IDs
for fast point lookups. With thousands of transactions spread across
multiple data files, bloom filters tell the engine which files can't
contain a given txn_id, skipping irrelevant files entirely and
dramatically reducing I/O for exact-match queries.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `transaction_log` | Delta Table | 60 (final) | Payment transactions with bloom filter on txn_id |

## Schema

**transaction_log:** `id INT, txn_id VARCHAR, user_id VARCHAR, merchant VARCHAR, amount DOUBLE, category VARCHAR, status VARCHAR, txn_date VARCHAR`

## Patterns Demonstrated

1. **Bloom filter configuration** — TBLPROPERTIES for data skipping on indexed columns
2. **Point lookups** — exact-match queries on txn_id benefit from bloom filters
3. **Multi-batch inserts** — online purchases, in-store purchases, and refunds
4. **Status updates** — 5 transactions disputed after initial insert
5. **Category distribution** — 5 categories: electronics, groceries, dining, travel, entertainment

## Verification

8 automated PASS/FAIL checks verify total row count, per-batch counts,
disputed status updates, unique transaction IDs, specific txn_id lookup
with exact amount, and distinct category count.
