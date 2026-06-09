# XML Books — Schema Evolution

Demonstrates how DeltaForge handles XML schema evolution across multiple files with different element structures. A single external table reads all 5 files and produces a unified schema where missing elements become SQL NULL.

## Data Story

A bookstore exports its catalog as XML every year. Over 5 years the export format evolves:

| File | Year | Books | Changes |
|------|------|-------|---------|
| `01_catalog_2000.xml` | 2000 | bk101–bk103 | Base schema: @id, author, title, genre, price, publish_date, description |
| `02_catalog_2001.xml` | 2001 | bk104–bk106 | Adds `isbn`, `language` |
| `03_catalog_2002.xml` | 2002 | bk107–bk109 | Adds `publisher`, `rating` |
| `04_catalog_2003.xml` | 2003 | bk110–bk112 | Adds `edition`, `pages`; drops `description` |
| `05_catalog_2004.xml` | 2004 | bk113–bk115 | Adds `@format` attribute, `series` |

## Union Schema (11 columns)

| Column | Source | Present In |
|--------|--------|-----------|
| `@id` | attribute | All files |
| `@format` | attribute | File 5 only |
| `author` | element | All files |
| `title` | element | All files |
| `genre` | element | All files |
| `price` | element | All files |
| `publish_date` | element | All files |
| `description` | element | Files 1–3 (NULL in 4–5) |
| `isbn` | element | Files 2–5 (NULL in 1) |
| `language` | element | Files 2–5 (NULL in 1) |
| `publisher` | element | Files 3–5 (NULL in 1–2) |
| `rating` | element | Files 3–5 (NULL in 1–2) |
| `edition` | element | Files 4–5 (NULL in 1–3) |
| `pages` | element | Files 4–5 (NULL in 1–3) |
| `series` | element | File 5 only (NULL in 1–4) |

## How to Verify

Run the **Summary** query (#14) to see PASS/FAIL for each evolution check:

```sql
SELECT 'total_rows' AS check_name,
       CASE WHEN COUNT(*) = 15 THEN 'PASS' ELSE 'FAIL' END AS result
FROM external.xml.books_evolved
UNION ALL
SELECT 'isbn_nulls', ...
-- etc.
ORDER BY check_name;
```

## What This Tests

1. **XML flatten config persistence** — The engine discovers all paths across 5 files and saves the `xml_flatten_config` JSON blob to the catalog database
2. **Schema union** — The provider produces a single schema that is the union of all element paths
3. **NULL filling** — Books from older files get NULL for columns added in later files
4. **Attribute extraction** — `@id` and `@format` attributes are correctly extracted as columns
5. **Multi-file reading** — All 5 files in the directory are discovered and read
