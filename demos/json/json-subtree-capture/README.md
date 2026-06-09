# JSON Subtree Capture — json_paths

Demonstrates how `json_paths` captures complex JSON subtrees as serialized string
columns instead of flattening them into individual columns. Two tables contrast
the captured (JSON blob) approach against the fully flattened approach.

## Data Story

A real-estate platform maintains property listings in JSON. Each listing has
top-level fields (title, type, bedrooms, sqft) plus two complex subtrees:

- **location** — nested address (street/unit/city/state/zip), geo coordinates,
  neighborhood, walk score, and transit score
- **pricing** — list price, price per sqft, HOA, annual tax, tax history array,
  and mortgage estimate (monthly payment, down payment, rate)

Flattening these subtrees into individual columns would create 15+ columns per
subtree. Instead, `json_paths` captures each subtree as a single JSON string
column — preserving the full nested structure for downstream consumers.

| File | Listings | Types |
|------|----------|-------|
| `01_listings_residential.json` | 3 (LST-001 to LST-003) | condo, house |
| `02_listings_commercial.json` | 2 (LST-004 to LST-005) | commercial, house |
| **Total** | **5** | **3 unique** |

## JSON Structure

```
listing
├── id
├── title
├── type
├── bedrooms / bathrooms / sqft / year_built / status
├── location                    ← json_paths target
│   ├── address
│   │   ├── street / unit / city / state / zip
│   ├── geo
│   │   ├── lat / lng
│   ├── neighborhood
│   ├── walk_score / transit_score
├── pricing                     ← json_paths target
│   ├── list_price / price_per_sqft / hoa_monthly / tax_annual
│   ├── tax_history[]
│   │   └── { year, amount }
│   └── mortgage_estimate
│       ├── monthly_payment / down_payment_pct / rate_pct
└── tags[]
```

## Tables

### `listings_captured` — Subtrees as JSON strings (json_paths)

| Column | Source | Notes |
|--------|--------|-------|
| `listing_id` | `$.id` | Flattened normally |
| `title` | `$.title` | Flattened normally |
| `property_type` | `$.type` | Flattened normally |
| `bedrooms` | `$.bedrooms` | Flattened normally |
| `bathrooms` | `$.bathrooms` | Flattened normally |
| `sqft` | `$.sqft` | Flattened normally |
| `year_built` | `$.year_built` | Flattened normally |
| `status` | `$.status` | Flattened normally |
| `location_json` | `$.location` | Captured as JSON via json_paths |
| `pricing_json` | `$.pricing` | Captured as JSON via json_paths |
| `tags` | `$.tags` | Array → JSON string |

### `listings_flattened` — Same data, fully flattened (no json_paths)

| Column | Source | Notes |
|--------|--------|-------|
| `listing_id` | `$.id` | Flattened normally |
| `title` | `$.title` | Flattened normally |
| `property_type` | `$.type` | Flattened normally |
| `street` | `$.location.address.street` | Deep path flattened |
| `city` | `$.location.address.city` | Deep path flattened |
| `state` | `$.location.address.state` | Deep path flattened |
| `neighborhood` | `$.location.neighborhood` | Deep path flattened |
| `list_price` | `$.pricing.list_price` | Deep path flattened |
| `tax_annual` | `$.pricing.tax_annual` | Deep path flattened |
| `monthly_payment` | `$.pricing.mortgage_estimate.monthly_payment` | Deep path flattened |

## How to Verify

Run the **Summary** query (#13) to see PASS/FAIL for all 8 checks:

```sql
SELECT check_name, result FROM (...) ORDER BY check_name;
```

## What This Tests

1. **json_paths** — Complex subtrees captured as single JSON string columns
2. **Multiple json_paths** — Two subtrees captured per row (location + pricing)
3. **json_paths + include_paths** — Captured subtrees coexist with normally flattened fields
4. **json_paths + column_mappings** — Captured columns renamed (location → location_json)
5. **Deep nesting preserved** — Nested address/geo/mortgage_estimate structure retained
6. **Arrays preserved** — tax_history array kept intact in captured JSON
7. **Contrast with flattened** — Same data shown both ways for comparison
