# Delta Advanced Schema Evolution — Add, Widen & Restructure

Demonstrates advanced Delta Lake schema evolution on a product catalog table
using multiple ALTER TABLE ADD COLUMN phases, backfill UPDATEs, and targeted
category-based UPDATEs.

## Data Story

An e-commerce product catalog starts with 5 basic columns (id, name, category,
price, stock). As business needs grow, new columns are added for weight,
discount percentage, and supplier info. Old products get NULLs in new columns
until selectively backfilled. The schema grows organically, showing real-world
evolution patterns.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `product_catalog` | Delta Table | 50 | Product catalog with evolving schema |

## Schema Evolution

| Phase | Columns | Rows | Action |
|-------|---------|------|--------|
| 1 | id, name, category, price, stock | 30 | Initial catalog |
| 2 | + weight_kg, discount_pct (ALTER TABLE) | 30 (NULLs) | Add columns |
| 3 | all 7 columns | 45 (15 new) | New products with weight/discount |
| 4 | backfill weight for ids 1-10 | 45 | UPDATE backfill |
| 5 | set discount for Electronics ids 1-5 | 45 | Targeted UPDATE |
| 6 | + supplier (ALTER TABLE) | 45 (NULLs) | Add column |
| 7 | all 8 columns | 50 (5 new) | Newest products with supplier |

## Verification

8 automated PASS/FAIL checks verify row counts, NULL patterns, column counts,
price integrity, and full-column population for newest products.
