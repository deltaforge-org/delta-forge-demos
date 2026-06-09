# Delta VACUUM & CDC Interaction — Retention & Change Tracking

Demonstrates how VACUUM and Change Data Feed (CDC) coexist in a Delta table,
ensuring orphaned files are cleaned up without disrupting change tracking.

## Data Story

An order management system tracks the full lifecycle of orders from placement
through delivery using Change Data Feed. Orders transition through pending,
processing, shipped, and delivered states, while cancelled orders are purged.
VACUUM must preserve CDC files within the retention period so downstream
consumers can replay changes. This demo shows how VACUUM and CDC coexist
without data loss.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `order_lifecycle` | Delta Table (CDF enabled) | 35 (final) | Order lifecycle with CDC tracking |

## Schema

**order_lifecycle:** `id INT, order_id VARCHAR, customer VARCHAR, product VARCHAR, amount DOUBLE, status VARCHAR, updated_by VARCHAR, updated_at VARCHAR`

## Operations Demonstrated

1. **CREATE** — Delta table with `delta.enableChangeDataFeed = true`
2. **INSERT** — 40 orders (all status='pending')
3. **UPDATE** — 15 orders to 'processing' (ids 1-15)
4. **UPDATE** — 10 orders to 'shipped' (ids 1-10)
5. **UPDATE** — 5 orders to 'delivered' (ids 1-5)
6. **DELETE** — 5 cancelled orders removed (ids 36-40)
7. **VACUUM** — cleanup orphaned files, CDC data preserved

## Final State

| Status | Count | IDs |
|--------|-------|-----|
| pending | 20 | 16-35 |
| processing | 5 | 11-15 |
| shipped | 5 | 6-10 |
| delivered | 5 | 1-5 |
| **Total** | **35** | |

## Verification

8 automated PASS/FAIL checks verify post-vacuum data integrity: total row count,
status distribution (pending, processing, shipped, delivered), cancelled order
removal, total amount preservation, and individual order data integrity.
