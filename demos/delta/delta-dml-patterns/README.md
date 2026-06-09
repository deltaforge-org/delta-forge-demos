# Delta DML Patterns — Complex DELETE & UPDATE

Demonstrates advanced DML patterns on an order management system with
multi-predicate DELETEs and bulk UPDATEs across 60 orders.

## Data Story

An order management system tracks orders across four regions. Advanced DML
operations are used to purge old cancelled orders, bulk-fulfill pending
shipments, apply electronics discounts, and archive completed orders —
all while maintaining data integrity.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `order_history` | Delta Table | 47 (final) | Order tracking with complex DML operations |

## Schema

**order_history:** `id INT, customer VARCHAR, product VARCHAR, qty INT, price DOUBLE, status VARCHAR, region VARCHAR, order_date VARCHAR`

## Operations Demonstrated

1. **CREATE DELTA TABLE** — explicit schema with LOCATION
2. **INSERT INTO VALUES** — 60 orders across 4 regions (us-east, us-west, eu-west, ap-south)
3. **DELETE with multi-predicate** — purge cancelled orders before 2024-06-01 (8 removed)
4. **UPDATE with multi-predicate** — bulk fulfillment: pending us-east orders to shipped (6 updated)
5. **UPDATE with IN clause** — 10% price discount for electronics products (10 updated)
6. **DELETE with multi-predicate** — archive completed orders before 2024-01-01 (5 removed)

## Verification

8 automated PASS/FAIL checks verify row count, deletion completeness,
bulk update results, price discounts, region diversity, and data integrity.
