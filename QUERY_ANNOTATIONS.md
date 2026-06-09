# Query Annotation Format

This document describes the comment-based annotation format used in `queries.sql`
files to define assertions that the CLI (`delta-forge-cli demo-test`) automatically
validates.

## Query Block Structure

Each query is delimited by a separator line:

```sql
-- ============================================================================
-- 1. Title of the Query
-- ============================================================================
-- Annotations go here (Expected, pipe-delimited values, etc.)

SELECT ...;
```

The parser extracts:

- **Title** from the first non-empty comment line (strips leading `N.` numbering)
- **Annotations** from subsequent comment lines before the SQL
- **SQL** from all non-comment, non-empty lines

---

## Annotation Types

### 1. Row Count — `Expected: N rows`

Asserts the query returns exactly N rows.

```sql
-- Expected: 15 rows
-- Expected: 34 vertices, 156 edge rows
```

The parser finds the first number in the comment and checks for the word "row".

#### Inequality Operators

Use `>=` or `<=` for range assertions:

```sql
-- Expected: >= 22 rows (some files contain 2 messages)
-- Expected: <= 100 rows
```

Text alternatives also work: `at least`, `at most`.

**JSON output:**

```json
{
  "expected_row_count": 22,
  "status": "PASS"
}
```

---

### 2. Pipe-Delimited Expected Values

Assert specific cell values by row label. The label matches the first column(s)
of the result, and values are checked against subsequent columns.

```sql
-- Expected results:
--   Widget A  | 58 units | 1,805.42
--   Widget B  | 23 units | 1,204.77
--   Gadget X  | 35 units |   542.50
```

**Matching rules:**

- Label is matched case-insensitively against the first column
- Compound keys: `Alice-2024` matches first two columns joined with `-`
- Values are fuzzy-matched (ignores commas, whitespace, ±0.01 for numbers)
- Named values with `column: value` syntax look up by column name

**JSON output (per check):**

```json
{
  "label": "Widget A",
  "expected": "58 units | 1,805.42",
  "actual": "58 | 1805.42",
  "matches": true
}
```

---

### 3. Self-Verifying Queries (PASS/FAIL in SQL)

Queries that embed their own assertions using SQL `CASE WHEN ... THEN 'PASS' ELSE 'FAIL' END`.
No comment annotations needed — the CLI auto-detects these by scanning the SQL for
`THEN 'PASS'` and `'FAIL` patterns.

```sql
SELECT 'total_rows' AS check_name,
       COUNT(*) AS actual,
       15 AS expected,
       CASE WHEN COUNT(*) = 15 THEN 'PASS' ELSE 'FAIL' END AS result
FROM my_table;
```

**Detection:** The CLI scans columns named `result`, `status`, or `check_result`
for any cell value starting with `FAIL`. If none of those columns exist, the last
column is scanned as a fallback (covers unnamed `SELECT 'label', CASE...END`).

**Best practices:**

- Name the check column `result`, `status`, or `check_result`
- Include a `check_name` or `check_id` column for readable failure messages
- Put diagnostic info in the FAIL message: `'FAIL (got ' || CAST(n AS VARCHAR) || ')'`

**JSON output:**

```json
{
  "is_self_verifying": true,
  "self_verify_failures": ["total_rows: FAIL (got 14)"],
  "status": "FAIL"
}
```

---

### 4. Verification Summary Queries

The final query in a demo often aggregates all PASS/FAIL checks via `UNION ALL`.
Title the query with "VERIFY" or "All Checks" so the CLI flags it as a summary.

```sql
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

SELECT 'row_count' AS check_name,
       CASE WHEN COUNT(*) = 15 THEN 'PASS' ELSE 'FAIL' END AS result
FROM my_table
UNION ALL
SELECT 'isbn_nulls',
       CASE WHEN COUNT(*) FILTER (WHERE isbn IS NULL) = 3 THEN 'PASS' ELSE 'FAIL' END
FROM my_table;
```

The CLI marks `is_verification_summary: true` in the JSON output, making it easy
for an agent to find the single query that summarizes all checks.

---

### 5. Queries Without Assertions (SKIP)

Queries with no annotations and no self-verification logic are marked `SKIP`.
These are exploratory/browse queries (e.g., `SELECT * FROM ...`) that demonstrate
functionality but don't assert specific values.

```sql
-- ============================================================================
-- EXPLORE: Browse All Data
-- ============================================================================

SELECT * FROM my_table ORDER BY id;
```

**JSON output:**

```json
{
  "status": "SKIP",
  "is_self_verifying": false
}
```

---

## Status Values

| Status  | Meaning                                                |
|---------|--------------------------------------------------------|
| `PASS`  | All assertions passed (row count, values, self-verify) |
| `FAIL`  | One or more assertions failed                          |
| `SKIP`  | No assertions defined — exploratory query              |
| `ERROR` | SQL execution error (syntax error, missing table)      |

---

## JSON Report Structure

The CLI outputs a JSON report to stdout. Each query produces a `ValidationResult`.

### Smart Row Sampling

To keep the JSON output small (token-efficient for AI agents), the CLI uses
**dynamic row sampling** instead of dumping all rows:

| Scenario                                | Sample content            |
|-----------------------------------------|---------------------------|
| PASS + self-verifying or value checks   | Empty (assertions prove)  |
| PASS + row count only, 20 rows or fewer | All rows (small enough)   |
| PASS + row count only, over 20 rows     | First 3 + last 3 rows     |
| SKIP (no assertions)                    | First 5 rows              |
| FAIL + self-verifying                   | FAIL rows + first 5 rows  |
| FAIL + other                            | First 10 rows             |
| ERROR                                   | Empty                     |

A 1500-row passing query emits **0 rows** in JSON. A failing self-verifying
query emits only the rows containing "FAIL" plus a small head sample.

### Example output

```json
{
  "query_index": 0,
  "query_title": "TOTAL ROW COUNT",
  "sql": "SELECT COUNT(*) ...",
  "status": "PASS",
  "row_count": 1,
  "expected_row_count": 15,
  "execution_time_ms": 42,
  "columns": ["check_name", "actual", "expected", "result"],
  "value_checks": [],
  "is_self_verifying": true,
  "is_verification_summary": false,
  "error_message": null
}
```

A failing query includes the relevant rows for debugging:

```json
{
  "status": "FAIL",
  "row_count": 15,
  "sample_rows": [
    ["isbn_nulls", "5", "3", "FAIL"]
  ],
  "self_verify_failures": ["isbn_nulls: FAIL"],
  "error_message": "Self-verification failed: isbn_nulls: FAIL"
}
```

The top-level report includes summary counts:

```json
{
  "demo_name": "books-schema-evolution",
  "total_queries": 14,
  "passed": 11,
  "failed": 0,
  "skipped": 2,
  "errors": 0
}
```

---

## Writing New Demo Queries

### Recommended structure

1. **Data integrity checks** — row counts, referential integrity
2. **Exploratory queries** — `SELECT *`, aggregations (these get `SKIP`)
3. **Feature-specific queries** — demonstrate the demo's core feature
4. **Verification summary** — final `UNION ALL` of all PASS/FAIL checks

### Checklist

- Use `-- Expected: N rows` for queries with a known row count
- Use pipe-delimited values for queries with known exact results
- Use `CASE WHEN ... THEN 'PASS' ELSE 'FAIL' END AS result` for data checks
- Include a `check_name` column alongside `result` for readable failures
- End with a `-- VERIFY: All Checks` summary query
- Put diagnostic info in FAIL messages: `'FAIL (got ' || CAST(x AS VARCHAR) || ')'`
- Use `>=` row counts when the exact count may vary (e.g., multi-message EDI files)
