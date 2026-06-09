# JSON Music Catalog — Nested Arrays & Explode

Demonstrates nested JSON flattening with array explosion using a digital music
catalog containing 347 albums and 3,503 tracks across 3 genre-based files.

## Data Story

A digital music distributor receives catalog feeds from labels as JSON files,
split by genre: rock, jazz, and pop. Each album object contains a flat `vendor`
object and a `details[]` array of tracks. The analytics team needs two views:

1. **Per-track** — one row per track for track-level analytics (duration, genre,
   composer), with album and vendor fields duplicated on each row
2. **Per-album** — one row per album with track count and vendor metadata kept
   as a JSON blob for downstream API responses

## JSON Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **Nested object flattening** | `$.vendor.id` → `vendor_id`, `$.vendor.name` → `vendor_name` |
| **explode_paths** | `$.details` array → one row per track (3,503 rows) |
| **json_paths** | `$.vendor` kept as JSON blob in album_summary (not flattened) |
| **column_mappings** | `$.details.name` → `track_name`, `$.details.milliseconds` → `duration_ms`, etc. |
| **default_array_handling: count** | Track count per album in summary table |
| **Multi-file reading** | 3 catalog files in one directory |
| **file_metadata** | `df_file_name` identifies source catalog file |
| **include_paths** | Selective field extraction from nested structures |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `album_tracks` | External Table | 3,503 | Exploded: one row per track |
| `album_summary` | External Table | 347 | One row per album, tracks counted |

## Schema

**album_tracks:** `album_id INT, album_name VARCHAR, sku VARCHAR, status VARCHAR, price INT, taxable BOOLEAN, vendor_id INT, vendor_name VARCHAR, track_id INT, track_name VARCHAR, composer VARCHAR, genre_id INT, duration_ms INT, file_bytes INT, track_price DOUBLE, df_file_name VARCHAR, df_row_number BIGINT`

**album_summary:** `album_id INT, album_name VARCHAR, sku VARCHAR, status VARCHAR, price INT, taxable BOOLEAN, requires_shipping BOOLEAN, vendor VARCHAR (JSON), details INT (count), df_file_name VARCHAR, df_row_number BIGINT`

## Data Files

| File | Albums | Tracks | Genres |
|------|--------|--------|--------|
| `catalog_rock.json` | 180 | 2,096 | Rock, Metal, Alternative, Punk |
| `catalog_jazz.json` | 31 | 370 | Jazz, Blues, Bossa Nova, R&B |
| `catalog_pop.json` | 136 | 1,037 | Pop, Latin, Soundtrack, Classical |
| **Total** | **347** | **3,503** | 25 genre IDs |

Each album object structure:

```json
{
  "id": 1,
  "name": "For Those About To Rock We Salute You",
  "vendor_id": 1,
  "requires_shipping": false,
  "sku": "ALBUM-1",
  "taxable": true,
  "status": "available",
  "price": 1004,
  "vendor": { "id": 1, "name": "AC/DC" },
  "details": [
    {
      "track_id": 14,
      "name": "Spellbound",
      "album_id": 1,
      "genre_id": 1,
      "composer": "Angus Young, Malcolm Young, Brian Johnson",
      "milliseconds": 270863,
      "bytes": 8817038,
      "unit_price": 0.99
    }
  ]
}
```

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Exploded track count | 3,503 | Sum of tracks across 3 files |
| Album count | 347 | Sum of albums across 3 files |
| Vendor flattened | 3,503 non-NULL vendor_name | Nested flattening |
| Vendor JSON blob | 347 non-NULL | json_paths preserves subtree |
| Source files | 3 distinct df_file_name | Multi-file reading |
| AC/DC album exists | > 0 rows | Data spot-check |
| Positive duration | 0 tracks with duration_ms <= 0 | Data integrity |

## How to Verify

Run **Query #12 (Summary)** to see PASS/FAIL for all 8 checks:

```sql
SELECT check_name, result FROM (...) ORDER BY check_name;
```

All checks should return `PASS`.
