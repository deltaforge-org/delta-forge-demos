# Delta Dynamic Partition Pruning — Smart Query Filtering

Demonstrates dynamic partition pruning with partitioned Delta tables joined
against dimension tables for efficient query filtering.

## Data Story

A sales analytics system tracks transactions across four geographic regions
using a partitioned fact table. A separate dimension table holds per-region
performance targets. When queries filter on the dimension table (e.g.,
WHERE target_amount > 50000), the engine dynamically prunes fact table
partitions that cannot match — scanning only us-east and us-west instead
of all four regions. This significantly reduces I/O and query time.

## Tables

| Object | Type | Rows | Partitions | Purpose |
|--------|------|------|------------|---------|
| `sales_facts` | Delta Table | 55 (final) | us-east, us-west, eu-west, ap-south | Partitioned fact table |
| `region_targets` | Delta Table | 4 | — | Dimension/lookup table |

## Schema

**sales_facts:** `id INT, product_id INT, region VARCHAR, quarter VARCHAR, amount DOUBLE, qty INT, channel VARCHAR, sale_date VARCHAR`

Partitioned by: `region`

**region_targets:** `region VARCHAR, target_amount DOUBLE, target_qty INT`

## Operations

1. **CREATE DELTA TABLE PARTITIONED BY** — 4-region partition scheme for sales facts
2. **INSERT** — 60 sales (15 per region) + 4 region targets
3. **UPDATE within partition** — 10% discount for ap-south region
4. **DELETE across partitions** — remove 5 cancelled orders (qty = 0)
5. **JOIN with partition pruning** — dimension filter prunes irrelevant partitions

## Verification

8 automated PASS/FAIL checks verify row counts, partition pruning via joins,
discount calculations, deletion correctness, and data distribution.
