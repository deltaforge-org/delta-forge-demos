# Sales Schema Evolution

## Overview

See how DeltaForge handles CSV files whose columns change over time. Five
quarterly sales files span 2024-Q1 to 2025-Q1.  Columns are **added** and
**retired** across the series, yet a single external table queries all of them
seamlessly — missing columns from older files surface as `NULL`.

## Schema Evolution Timeline

| File | Period | Columns added | Columns retired |
| ---- | ------ | ------------- | --------------- |
| `sales_2024_q1.csv` | Q1 2024 | *(base)* `id, product_name, quantity, unit_price, sale_date, region` | — |
| `sales_2024_q2.csv` | Q2 2024 | `sales_rep` | — |
| `sales_2024_q3.csv` | Q3 2024 | `discount_pct` | — |
| `sales_2024_q4.csv` | Q4 2024 | `territory` | `region` |
| `sales_2025_q1.csv` | Q1 2025 | `channel` | `discount_pct` |

**Unified schema** (what `SELECT *` returns):

```text
id, product_name, quantity, unit_price, sale_date,
region,        -- NULL for Q4 2024 and Q1 2025 rows
sales_rep,     -- NULL for Q1 2024 rows
discount_pct,  -- NULL for Q1 2024, Q2 2024, and Q1 2025 rows
territory,     -- NULL for Q1–Q3 2024 rows
channel        -- NULL for all 2024 rows
```

## What This Demo Sets Up

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `csv` | CSV-backed external tables |
| Table | `external.csv.sales` | All 15 records across 5 quarterly files |

### Naming Convention

All objects use 3-part fully qualified names: `external.format.table`

- **Zone** = `external` (shared across all demos)
- **Schema** = `csv` (the file format)
- **Table** = `sales`

## Sample Queries

```sql
-- All 15 rows — notice NULL in columns not yet added or already retired
SELECT * FROM external.csv.sales ORDER BY id;

-- See region → territory handoff: region is NULL after Q3, territory is NULL before Q4
SELECT id, sale_date, region, territory
FROM external.csv.sales
ORDER BY id;

-- Revenue by sales rep (only Q2 2024 onwards have a rep)
SELECT sales_rep, SUM(quantity * unit_price) AS revenue
FROM external.csv.sales
WHERE sales_rep IS NOT NULL
GROUP BY sales_rep
ORDER BY revenue DESC;

-- Average discount when it was tracked (Q3–Q4 2024 only)
SELECT ROUND(AVG(discount_pct) * 100, 1) AS avg_discount_pct
FROM external.csv.sales
WHERE discount_pct IS NOT NULL;

-- Channel mix in 2025
SELECT channel, COUNT(*) AS orders
FROM external.csv.sales
WHERE channel IS NOT NULL
GROUP BY channel;

-- Identify which rows came from which era by what is NULL
SELECT
    id,
    sale_date,
    CASE
        WHEN sales_rep  IS NULL THEN 'pre-rep era (Q1 2024)'
        WHEN territory  IS NULL THEN 'region era (Q2–Q3 2024)'
        WHEN discount_pct IS NULL AND channel IS NULL THEN 'territory era (Q4 2024)'
        ELSE 'channel era (2025+)'
    END AS data_generation
FROM external.csv.sales
ORDER BY id;
```

## Data Format

- **Format**: CSV with comma delimiter
- **Headers**: First row of each file contains column names
- **Location**: `data/` directory (5 files, 3 records each)
