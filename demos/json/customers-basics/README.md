# JSON Customers — Basics

Demonstrates JSON fundamentals by parsing a CRM customer export file into a
queryable table with column mappings and type inference.

## Data Story

A CRM system exports its customer database as a single JSON array file. Each
element is a flat object with short field names (`first`, `last`, `created_at`).
The analyst needs clean column names (`first_name`, `last_name`, `signup_date`)
and proper types (timestamps, not strings) for reporting.

## JSON Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **JSON array format** | Single file containing a `[{...}, {...}]` array |
| **include_paths** | Select only the 7 needed fields from each object |
| **column_mappings** | `$.first` → `first_name`, `$.last` → `last_name`, `$.created_at` → `signup_date` |
| **infer_types** | Automatic type detection (timestamps, integers, strings) |
| **max_depth** | Set to 1 (flat data, no nested objects) |
| **file_metadata** | `df_file_name` and `df_row_number` injected per row |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `customers` | External Table | 200 | CRM customer records with mapped column names |

## Schema

**customers:** `id INT, email VARCHAR, first_name VARCHAR, last_name VARCHAR, company VARCHAR, signup_date TIMESTAMP, country VARCHAR, df_file_name VARCHAR, df_row_number BIGINT`

## Data File

`customers.json` — JSON array of 200 customer objects. Each object:

```json
{
  "id": 1,
  "email": "isidro_von@hotmail.com",
  "first": "Torrey",
  "last": "Veum",
  "company": "Hilll, Mayert and Wolf",
  "created_at": "2014-12-25T04:06:27.981Z",
  "country": "Switzerland"
}
```

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Row count | 200 | JSON array length |
| first_name populated | 200 non-NULL | column_mappings applied |
| last_name populated | 200 non-NULL | column_mappings applied |
| signup_date populated | 200 non-NULL | column_mappings + infer_types |
| Customer 1 | Torrey Veum, Switzerland | Data file spot-check |
| File metadata | All rows have df_file_name | file_metadata config |

## How to Verify

Run **Query #10 (Summary)** to see PASS/FAIL for all 7 checks:

```sql
SELECT check_name, result FROM (...) ORDER BY check_name;
```

All checks should return `PASS`.
