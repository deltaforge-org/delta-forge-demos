# Delta Basics — Create, Insert, Update, Delete

Teaches the four essential Delta table operations using a small products
catalog with known, verifiable values.

## Data Story

An office supply company manages a product catalog. Products are added,
prices are updated (10% increase for Electronics), out-of-stock items are
deactivated then removed, and new products are introduced.

## Table

| Object          | Type        | Rows       | Purpose                              |
|-----------------|-------------|------------|--------------------------------------|
| `crud_products` | Delta Table | 22 (final) | Product catalog with CRUD operations |

## Schema

**crud_products:** `id INT, name VARCHAR, category VARCHAR, price DOUBLE, stock INT, is_active BOOLEAN`

## Operations Demonstrated

1. **CREATE DELTA TABLE** — explicit schema with LOCATION
2. **INSERT INTO VALUES** — 20 hand-picked products across 4 categories
3. **UPDATE with WHERE** — 10% price increase for Electronics
4. **UPDATE with WHERE** — deactivate zero-stock products
5. **DELETE with WHERE** — remove inactive products (3 deleted)
6. **INSERT INTO…SELECT** — add 5 new products

## Verification

12 automated PASS/FAIL checks verify row counts, specific prices,
category counts, deleted product absence, and total stock.
