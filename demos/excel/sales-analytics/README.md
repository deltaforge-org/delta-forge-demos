# Excel Sales Analytics — Superstore Orders

Demonstrates all Excel reading features using 4 years of Superstore sales data
(2014–2017, 9,994 orders across 21 columns). Five tables exercise different
Excel options from the same underlying data.

## Data Story

A retail chain exports annual sales reports as Excel workbooks (2014–2017). The
analytics team needs to read all 4 years into one unified table for trend
analysis, extract a single year for focused queries, read a subset of columns
using cell ranges, and handle data cleansing (whitespace trimming, null markers).

## Excel Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **sheet_name** | `all_orders` selects the "Orders" sheet by name |
| **has_header=true** | `all_orders`, `orders_2017`, `orders_range`, `orders_trimmed` |
| **has_header=false** | `orders_no_header` reads without headers (column_0, column_1, ...) |
| **skip_rows** | `orders_no_header` skips 1 row (the header row treated as data) |
| **max_rows** | `orders_no_header` limited to 100 rows per file |
| **range** | `orders_range` reads cell range A1:K500 (11 columns, 499 rows/file) |
| **trim_whitespace** | `orders_trimmed` trims string cell whitespace |
| **null_values** | `orders_trimmed` interprets "", "N/A", "-" as NULL |
| **empty_cell_handling** | `orders_trimmed` uses AsNull for empty cells |
| **infer_schema_rows** | `all_orders` samples 1,000 rows for type detection |
| **file_filter** | `orders_2017` reads only `sales-data-2017*` from the directory |
| **Multi-file reading** | `all_orders` reads all 4 XLSX files from one directory |
| **file_metadata** | `df_file_name` + `df_row_number` system columns on all tables |
| **Type inference** | Excel dates → Date, numbers → Double/Int64, text → Utf8 |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `all_orders` | External Table | 9,994 | Unified view of all 4 files |
| `orders_2017` | External Table | 3,312 | Single file via file_filter |
| `orders_range` | External Table | ~1,996 | Cell range A1:K500 (4 files × 499 rows) |
| `orders_trimmed` | External Table | 9,994 | Whitespace trimming + custom null handling |
| `orders_no_header` | External Table | 400 | Auto-generated column names (100/file × 4) |

## Schema

**all_orders:** `Row ID BIGINT, Order ID VARCHAR, Order Date DATE, Ship Date DATE, Ship Mode VARCHAR, Customer ID VARCHAR, Customer Name VARCHAR, Segment VARCHAR, Country VARCHAR, City VARCHAR, State VARCHAR, Postal Code BIGINT, Region VARCHAR, Product ID VARCHAR, Category VARCHAR, Sub-Category VARCHAR, Product Name VARCHAR, Sales DOUBLE, Quantity BIGINT, Discount DOUBLE, Profit DOUBLE, df_file_name VARCHAR, df_row_number BIGINT`

**orders_range (A1:K500):** `Row ID BIGINT, Order ID VARCHAR, Order Date DATE, Ship Date DATE, Ship Mode VARCHAR, Customer ID VARCHAR, Customer Name VARCHAR, Segment VARCHAR, Country VARCHAR, City VARCHAR, State VARCHAR, df_file_name VARCHAR, df_row_number BIGINT`

**orders_no_header:** `column_0 VARCHAR, column_1 VARCHAR, ..., column_20 VARCHAR, df_file_name VARCHAR, df_row_number BIGINT`

## Data Files

4 Superstore sales XLSX files, each containing one "Orders" sheet:

| File | Year | Rows | Size |
|------|------|------|------|
| `sales-data-2014.xlsx` | 2014 | 1,993 | 296 KB |
| `sales-data-2015.xlsx` | 2015 | 2,102 | 311 KB |
| `sales-data-2016.xlsx` | 2016 | 2,587 | 376 KB |
| `sales-data-2017.xlsx` | 2017 | 3,312 | 574 KB |
| **Total** | | **9,994** | **1.56 MB** |

21 columns per file: Row ID, Order ID, Order Date, Ship Date, Ship Mode,
Customer ID, Customer Name, Segment, Country, City, State, Postal Code,
Region, Product ID, Category, Sub-Category, Product Name, Sales, Quantity,
Discount, Profit.

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Total row count | 9,994 | Sum of all 4 files |
| Source files | 4 distinct df_file_name | Multi-file reading |
| 2017 file rows | 3,312 | file_filter single-file |
| Range columns | 11 (A–K) | range option |
| Trimmed count | 9,994 | Same data, different options |
| No-header columns | column_0, column_1, ... | has_header=false |
| File metadata | 9,994 non-NULL df_file_name | file_metadata config |
| Sales type | DOUBLE | Type inference |
| Regions | 4 (Central, East, South, West) | Data spot-check |
| No-header rows | 400 (100 × 4 files) | max_rows option |

## How to Verify

Run **Query #13 (Summary)** to see PASS/FAIL for all 10 checks:

```sql
SELECT check_name, result FROM (...) ORDER BY check_name;
```

All checks should return `PASS`.
