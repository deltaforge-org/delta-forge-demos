# JSON Country Factbook — Deep Nesting & Schema Evolution

Demonstrates deep JSON nesting (3+ levels), schema evolution with NULL filling,
and selective path extraction using CIA World Factbook data for 10 African
countries.

## Data Story

An intelligence analyst imports CIA World Factbook country profiles for African
nations. Each country is a single deeply nested JSON document spanning 12–13
top-level sections (Introduction, Geography, People and Society, Government,
Economy, etc.). Not all countries have the same sections — Terrorism and Space
are present for some but absent for others, creating a natural schema evolution
scenario. The analyst needs:

1. A **flattened overview table** with key country facts (name, capital,
   population, area, government type) plus optional fields that gracefully
   become NULL when absent
2. An **economy-focused table** extracting GDP, growth rates, inflation, and
   sector composition from paths nested 4 levels deep

## JSON Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **Deep nesting (3+ levels)** | `$.Government.Country name.conventional short form.text` (4 levels) |
| **include_paths** | Selective extraction from 13 top-level sections |
| **exclude_paths** | Skip `$.Introduction.Background` (verbose HTML text) |
| **column_mappings** | Deep paths → friendly names (`population`, `capital`, `gdp_ppp_2023`) |
| **preserve_original** | `_json_source` keeps full JSON document for audit |
| **Schema evolution** | Terrorism (8/10 countries), Space (7/10) → NULL filling |
| **Multi-file reading** | 10 .json files in one directory |
| **file_metadata** | `df_file_name` reveals country code (eg.json, sf.json...) |
| **max_depth** | Set to 5 to control flattening of complex documents |
| **One-object-per-file** | Each file is a single JSON object (not an array) |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `countries` | External Table | 10 | Flattened overview with preserved original JSON |
| `country_economy` | External Table | 10 | Economy-focused extraction (GDP, sectors, trade) |

## Schema

**countries:** `country_name VARCHAR, capital VARCHAR, government_type VARCHAR, independence VARCHAR, location VARCHAR, area VARCHAR, climate VARCHAR, terrain VARCHAR, population VARCHAR, languages VARCHAR, religions VARCHAR, terrorist_groups VARCHAR, space_agencies VARCHAR, space_program VARCHAR, _json_source VARCHAR, df_file_name VARCHAR, df_row_number BIGINT`

**country_economy:** `country_name VARCHAR, economic_overview VARCHAR, gdp_ppp_2023 VARCHAR, gdp_growth_2023 VARCHAR, gdp_per_capita_2023 VARCHAR, gdp_official VARCHAR, inflation_2023 VARCHAR, sector_agriculture VARCHAR, sector_industry VARCHAR, sector_services VARCHAR, agricultural_products VARCHAR, industries VARCHAR, unemployment_2023 VARCHAR, exports_2023 VARCHAR, imports_2023 VARCHAR, df_file_name VARCHAR, df_row_number BIGINT`

## Data Files

10 country JSON files, each containing a single deeply nested JSON object:

| File | Country | Sections | Terrorism | Space |
|------|---------|----------|-----------|-------|
| `eg.json` | Egypt | 13 | Yes | Yes |
| `sf.json` | South Africa | 13 | Yes | Yes |
| `ni.json` | Nigeria | 13 | Yes | Yes |
| `ke.json` | Kenya | 13 | Yes | Yes |
| `et.json` | Ethiopia | 13 | Yes | Yes |
| `mo.json` | Morocco | 12 | Yes | No |
| `gh.json` | Ghana | 12 | No | Yes |
| `dj.json` | Djibouti | 12 | Yes | No |
| `cg.json` | DRC | 12 | Yes | No |
| `rw.json` | Rwanda | 12 | No | Yes |

Each file is a single JSON object (not an array) with 12–13 top-level sections,
nested 3–5 levels deep. Example path structure:

```json
{
  "Government": {
    "Country name": {
      "conventional short form": {
        "text": "Egypt"
      }
    },
    "Capital": {
      "name": {
        "text": "Cairo"
      }
    }
  },
  "Economy": {
    "Real GDP (purchasing power parity)": {
      "Real GDP (purchasing power parity) 2023": {
        "text": "$1.912 trillion (2023 est.)"
      }
    }
  }
}
```

## Schema Evolution

Not all countries have the same sections:

- **Terrorism section:** Present in 8 of 10 countries. Ghana and Rwanda lack
  this section → `terrorist_groups` column is NULL for those rows.
- **Space section:** Present in 7 of 10 countries. Morocco, Djibouti, and DRC
  lack this section → `space_agencies` and `space_program` are NULL.

This mirrors the XML books-schema-evolution demo pattern where newer files add
fields that older files don't have, producing NULL filling in the union schema.

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Country count | 10 | Number of .json files |
| Economy count | 10 | Same 10 files, different extraction |
| Egypt found | 1 row | Deep nesting at 4 levels |
| Ghana terrorism NULL | NULL | Schema evolution |
| Egypt terrorism present | non-NULL | Schema evolution |
| Space data present | 7 countries | Optional section count |
| preserve_original | 10 non-NULL _json_source | preserve_original config |
| country_name populated | 10 non-NULL | column_mappings applied |
| File metadata | 10 distinct df_file_name | Multi-file reading |
| Egypt capital | Cairo | Data spot-check |
| Egypt GDP populated | non-NULL gdp_ppp_2023 | Economy deep nesting |
| Source files | 10 distinct | Multi-file verification |

## How to Verify

Run **Query #14 (Summary)** to see PASS/FAIL for all 12 checks:

```sql
SELECT check_name, result FROM (...) ORDER BY check_name;
```

All checks should return `PASS`.
