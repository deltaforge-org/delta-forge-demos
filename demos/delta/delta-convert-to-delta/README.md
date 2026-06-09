# Delta Convert to Delta — Migrating from Raw Parquet

Simulates a Parquet-to-Delta migration workflow where legacy flat-file data
is loaded into a Delta table, gaining ACID transactions, schema enforcement,
and time travel capabilities.

## Data Story

A legacy reporting system stored order data as flat Parquet files without
transactional guarantees. The team migrates 40 historical records into a
Delta table, then adds 10 new records post-migration. After migration,
UPDATE operations standardize legacy payment codes ('cc' and 'pp') and
DELETE removes 5 duplicate records — operations impossible with raw Parquet.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `legacy_data` | Delta Table | 45 (final) | Migrated order data with post-migration modifications |

## Schema

**legacy_data:** `id INT, customer_name VARCHAR, order_total DOUBLE, product_category VARCHAR, payment_method VARCHAR, order_status VARCHAR, created_date VARCHAR, migrated_flag INT`

## Operations Demonstrated

1. **CREATE DELTA TABLE** — explicit schema with LOCATION
2. **INSERT INTO VALUES** — 40 migrated legacy records (migrated_flag=1)
3. **GRANT ADMIN** — grant permissions
4. **INSERT INTO VALUES** — 10 new post-migration records (migrated_flag=0)
5. **UPDATE with WHERE** — standardize 'cc' to 'credit_card'
6. **UPDATE with WHERE** — standardize 'pp' to 'paypal'
7. **DELETE with WHERE** — remove 5 duplicate legacy records
8. **OPTIMIZE** — compact files after migration operations

## Verification

8 automated PASS/FAIL checks verify total row count (45), migrated vs new
record counts, payment method standardization, category diversity, exact
total revenue, and absence of deleted duplicates.
