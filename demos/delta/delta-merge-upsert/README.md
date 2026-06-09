# Delta MERGE — CDC Upsert with BY SOURCE

Full three-way CDC upsert using MERGE INTO with WHEN MATCHED, WHEN NOT MATCHED, and WHEN NOT MATCHED BY SOURCE to atomically update, insert, and delete in a single statement.

## Data Story

A product catalog receives a daily supplier feed. The feed contains updated prices for existing products and new items entering the catalog. Products that disappear from the feed with low stock are discontinued and removed.

## Tables

| Object          | Type  | Rows | Purpose                          |
|-----------------|-------|------|----------------------------------|
| upsert_products | Delta | 15   | Current product catalog (target) |
| product_feed    | Delta | 12   | Daily supplier feed (source)     |

## Operations Demonstrated

1. **WHEN MATCHED** — Update price and stock from the feed (8 products)
2. **WHEN NOT MATCHED** — Insert new products from the feed (4 products)
3. **WHEN NOT MATCHED BY SOURCE** — Delete discontinued low-stock products (3 products)
4. Conditional BY SOURCE predicate (`AND target.in_stock <= 5`) to protect well-stocked items

## Verification

- Final product count: 15 - 3 + 4 = 16
- Discontinued products (ids 13-15) removed
- Well-stocked non-feed products (ids 9-12) survive
- Price and stock updates verified
