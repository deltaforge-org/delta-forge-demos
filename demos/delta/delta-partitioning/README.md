# Delta Partitioning — Organize & Query by Region

Demonstrates Delta table partitioning with PARTITIONED BY for efficient
data organization and partition-aware DML operations.

## Data Story

A national retailer tracks orders across four regions. Each region gets
its own data partition. The South region runs a 15% discount promotion,
and small orders under $50 in the West region are cancelled.

## Table

| Object               | Type        | Rows       | Partitions               | Purpose                |
|----------------------|-------------|------------|--------------------------|------------------------|
| `partitioned_orders` | Delta Table | 76 (final) | North, South, East, West | Partitioned order data |

## Schema

**partitioned_orders:** `id INT, customer VARCHAR, product VARCHAR, amount DOUBLE, order_date VARCHAR, region VARCHAR`

Partitioned by: `region`

## Operations

1. **CREATE DELTA TABLE PARTITIONED BY** — 4-region partition scheme
2. **INSERT** — 80 orders (20 per region)
3. **UPDATE within partition** — 15% discount for South
4. **DELETE across partition** — remove orders < $50 in West (4 deleted)

## Verification

7 automated PASS/FAIL checks verify per-partition row counts, discount
calculations, and deleted order removal.
