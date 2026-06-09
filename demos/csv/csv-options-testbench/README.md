# CSV Advanced Options Testbench

Verifies that every CSV parsing option available in the DeltaForge GUI is correctly wired through to the SQL backend. Each table exercises a specific option with data designed so that incorrect parsing produces obviously wrong results.

## Tables

| Table | Option Tested | Expected Result |
|-------|--------------|-----------------|
| `opt_delimiter` | `delimiter = '\|'` | 5 rows with 4 columns (not 1 merged column) |
| `opt_null_value` | `null_value = 'N/A'` | 2 NULL scores (ids 2, 4) |
| `opt_comment` | `comment_char = '#'` | 3 data rows (comment lines skipped) |
| `opt_skip_rows` | `skip_starting_rows = '3'` | 5 rows with correct column names |
| `opt_max_rows` | `max_rows = '5'` | Exactly 5 rows (not 10) |
| `opt_trim` | `trim_whitespace = 'true'` | `LENGTH(name) = 5` for 'Alice' |
| `opt_quoted` | `delimiter = ';'` + `quote = '"'` | 4 rows, semicolons in quotes preserved |
| `opt_combined` | All of the above together | 5 rows, 2 NULLs, trimmed names |

## How to Verify

Run the **Summary** query (query #9) to see PASS/FAIL for each option:

```sql
SELECT 'delimiter' AS option, CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result FROM external.csv.opt_delimiter
UNION ALL
SELECT 'null_value', ... FROM external.csv.opt_null_value
-- etc.
ORDER BY option;
```

## Not Yet Implemented

The following GUI options are displayed but do not yet have backend support:

- **skip_ending_rows** — Skip trailing rows at end of file
- **skip_empty_rows** — Skip blank lines
- **column_widths** — Fixed-width column parsing mode
- **strip_control_chars** — Remove control characters from values
- **expected_column_count** — Validate column count per row
- **error_handling** — Skip bad rows instead of stopping (stop/skip_row/replace_with_null)
- **include_file_line_number** — Add source line number column
