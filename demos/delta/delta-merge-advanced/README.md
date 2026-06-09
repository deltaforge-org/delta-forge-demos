# Delta MERGE — Advanced Patterns & Conditional Logic

Demonstrates advanced MERGE INTO patterns with conditional matching logic:
matched updates, matched deletes for zero-stock items, and not-matched inserts
from a staging table.

## Data Story

An e-commerce warehouse maintains 40 products in a master inventory. A
supplier sends 30 updates: 12 price/quantity adjustments, 3 out-of-stock
notices (qty=0), and 15 brand-new products. A single MERGE statement handles
all three cases — updating existing items, removing discontinued stock, and
adding new products — in one atomic operation.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `inventory_master` | Delta Table | 52 (final) | Product catalog after merge |
| `inventory_updates` | Delta Table | 30 | Staging table with supplier updates |

## Schema

**inventory_master:** `id INT, sku VARCHAR, name VARCHAR, category VARCHAR, price DOUBLE, qty INT, supplier VARCHAR, last_updated VARCHAR`

**inventory_updates:** Same schema as master (staging source)

## MERGE Logic

```
MERGE INTO master USING updates ON sku
├── WHEN MATCHED AND qty > 0 → UPDATE price, qty, timestamp
├── WHEN MATCHED AND qty = 0 → DELETE (out of stock)
└── WHEN NOT MATCHED → INSERT new product
```

## Verification

8 automated PASS/FAIL checks verify: 52 final rows, 3 deleted items gone,
updated prices, updated quantities, 15 new products inserted, unchanged items
intact, category distribution, and merge timestamps.
