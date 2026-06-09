# Delta RESTORE — Rollback to Previous Versions

Demonstrates RESTORE operations for rolling back a Delta table to a
previous version after an accidental destructive operation.

## Data Story

A warehouse manages 30 products. After price increases and discontinuation
marking, an operator accidentally DELETEs all discontinued items instead of
archiving them. RESTORE TO VERSION recovers the table to its pre-delete
state, and the recovered items are reactivated with a clearance discount.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `product_inventory` | Delta Table | 30 (final) | Product catalog with version rollback |

## Schema

**product_inventory:** `id INT, name VARCHAR, category VARCHAR, price DOUBLE, qty INT, status VARCHAR`

## Version History

- **V0:** INSERT 30 products (all active)
- **V1:** UPDATE Electronics +10% price
- **V2:** UPDATE 5 items to 'discontinued'
- **V3:** DELETE discontinued items (accident!)
- **V4:** RESTORE TO VERSION 2 (undo delete)
- **V5:** UPDATE recovered items → active + 25% clearance discount

## Verification

8 automated PASS/FAIL checks verify full recovery: 30 rows restored, all
active, recovered items present, price increases preserved, clearance
discounts applied, unchanged items intact, and all categories represented.
