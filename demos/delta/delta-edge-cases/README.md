# Delta Edge Cases — Empty, Wide & Minimal Tables

Demonstrates Delta table behavior at the boundaries: single-row tables,
wide tables with many columns, and empty schema-only tables.

## Data Story

An operations team manages a singleton config table (updated through 4
versions), a monthly KPI dashboard with 30 metric columns across 20
months, and an empty staging table ready for incoming data.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `config_singleton` | Delta Table | 1 | Single-row config, 3 updates |
| `wide_metrics` | Delta Table | 20 | 30-column monthly KPI data |
| `empty_staging` | Delta Table | 0 | Schema-only empty table |

## Schema

**config_singleton:** `config_key VARCHAR, config_value VARCHAR, version INT, updated_by VARCHAR, updated_at VARCHAR`
**wide_metrics:** `id INT, name VARCHAR, m01_revenue DOUBLE, ... m28_engagement_rate DOUBLE` (30 columns total)
**empty_staging:** `id INT, source_system VARCHAR, raw_data VARCHAR, status VARCHAR, received_at VARCHAR`

## Patterns Demonstrated

1. **Singleton table** — single row updated multiple times (version tracking)
2. **Wide table** — 30 columns covering revenue, cost, satisfaction, uptime, etc.
3. **Empty table** — schema defined but no data (COUNT=0, MAX=NULL)
4. **Version tracking** — update history via version + updated_by columns
5. **Aggregate queries** — SUM, MAX, AVG across many columns

## Verification

10 automated PASS/FAIL checks verify singleton row count/version/updater/value,
wide table row count/revenue spot-check/max/profit sum, and empty table
row count/NULL aggregation behavior.
