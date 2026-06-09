# Parquet Supply Chain — Order Analytics

Demonstrates Parquet reading features using 14 quarterly order files (2012–2016,
73,089 rows) from the WideWorldImporters supply chain database. Files are
organized in year-based subdirectories, exercising recursive scanning and
file-level filtering. Parquet's self-describing schema provides automatic type
detection with no configuration needed.

## Data Story

A supply chain company exports quarterly order batches as Parquet files organized
in year-based directories (2012–2016). The data team needs to read all quarters
into one unified table for trend analysis, extract a single year for focused
queries, sample data for profiling, and drill into a specific quarter.

## Parquet Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **Recursive scanning** | `all_orders` reads 14 files from `orders/2012/` through `orders/2016/` |
| **file_filter** | `orders_2015` matches `Orders_2015*`; `orders_q1_2014` matches `Orders_2014-03*` |
| **max_rows** | `orders_sample` limited to 100 rows per file (~1,400 total) |
| **row_group_filter** | Enables predicate pushdown via Parquet min/max statistics |
| **file_metadata** | `df_file_name` + `df_row_number` on all tables |
| **Self-describing schema** | No schema specification — types from Parquet metadata |
| **Multi-file reading** | 14 files across 5 year directories |
| **Columnar reading** | Queries select few columns; Parquet skips unneeded column chunks |
| **Predicate pushdown** | WHERE clauses leverage row group statistics for fast filtering |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `all_orders` | External Table | 73,089 | Unified view via recursive scanning |
| `orders_2015` | External Table | 23,636 | Year filter via file_filter |
| `orders_sample` | External Table | 1,400 | Data profiling via max_rows |
| `orders_q1_2014` | External Table | 5,210 | Quarter drill-down via file_filter |

## Schema

**all_orders (18 data columns + 2 metadata):**
`OrderID INT32, CustomerID INT32, SalespersonPersonID INT32, PickedByPersonID INT32, ContactPersonID INT32, BackorderOrderID INT32, OrderDate BYTE_ARRAY, ExpectedDeliveryDate BYTE_ARRAY, CustomerPurchaseOrderNumber INT32, IsUndersupplyBackordered BOOLEAN, Comments BYTE_ARRAY, DeliveryInstructions BYTE_ARRAY, InternalComments BYTE_ARRAY, PickingCompletedWhen BYTE_ARRAY, LastEditedBy INT32, LastEditedWhen BYTE_ARRAY, InsertedDate_DW BYTE_ARRAY, UpdatedDate_DW BYTE_ARRAY, df_file_name VARCHAR, df_row_number BIGINT`

## Data Files

14 Parquet files organized in year-based subdirectories:

| Directory | Files | Rows | Period |
|-----------|-------|------|--------|
| `orders/2012/` | 1 | 2,784 | Dec 2012 — Feb 2013 |
| `orders/2013/` | 4 | 19,712 | Mar 2013 — Feb 2014 |
| `orders/2014/` | 4 | 21,341 | Mar 2014 — Feb 2015 |
| `orders/2015/` | 4 | 23,636 | Mar 2015 — Feb 2016 |
| `orders/2016/` | 1 | 5,616 | Mar 2016 — May 2016 |
| **Total** | **14** | **73,089** | **~3.5 years** |

Each file covers one fiscal quarter and contains 18 columns: order IDs (5
foreign keys), dates (order, expected delivery, picking completed), flags
(undersupply backorder), text fields (comments, delivery instructions), and
audit columns (last edited, insert/update timestamps).

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Total row count | 73,089 | Sum of all 14 files |
| Source files | 14 distinct df_file_name | Recursive scanning |
| 2015 file filter | 23,636 rows, 4 files | file_filter `Orders_2015*` |
| Max rows sample | 1,400 (100 × 14) | max_rows option |
| Q1 2014 quarter | 5,210 rows | file_filter `Orders_2014-03*` |
| File metadata | 73,089 non-NULL df_file_name | file_metadata config |
| Schema columns | 20 (18 data + 2 metadata) | Self-describing schema |
| OrderID column | exists | Schema spot-check |
| Sample coverage | 14 files | All files sampled |

## How to Verify

Run **Query #12 (Summary)** to see PASS/FAIL for all 10 checks:

```sql
SELECT check_name, result FROM (...) ORDER BY check_name;
```

All checks should return `PASS`.
