# XML Subtree Capture — xml_paths & nested_output_format

Demonstrates how `xml_paths` captures complex XML subtrees as serialized strings
instead of flattening them into individual columns, and how `nested_output_format`
controls whether the output is JSON or raw XML.

## Data Story

A hardware manufacturer maintains a product catalog in XML. Each product has
top-level fields (name, category, price) plus two complex subtrees:

- **specifications** — weight, dimensions (nested length/width/height),
  operating temperature (with min/max attributes), voltage, and certifications
- **supplier** — company name, contact person (name/email/phone), full address,
  and lead time

Flattening these subtrees into individual columns would create 15+ columns per
subtree. Instead, `xml_paths` captures each subtree as a single string column —
either as a JSON object or as a raw XML fragment.

| File | Products | Suppliers |
|------|----------|-----------|
| `01_products.xml` | 3 (PRD-001 to PRD-003) | TechParts Inc., NetCore Systems |
| `02_products.xml` | 2 (PRD-004 to PRD-005) | EuroPower GmbH, NetCore Systems |
| **Total** | **5** | **3 unique** |

## XML Structure

```
catalog (@last_updated)
└── product (@id, @status)
    ├── name
    ├── category
    ├── price (@currency)
    ├── specifications              ← xml_paths target
    │   ├── weight (@unit)
    │   ├── dimensions
    │   │   ├── length (@unit)
    │   │   ├── width (@unit)
    │   │   └── height (@unit)
    │   ├── operating_temp (@min, @max, @unit)
    │   ├── voltage
    │   └── certifications
    ├── supplier                    ← xml_paths target
    │   ├── company
    │   ├── contact
    │   │   ├── name
    │   │   ├── email
    │   │   └── phone
    │   ├── address
    │   │   ├── street
    │   │   ├── city
    │   │   ├── state
    │   │   ├── zip
    │   │   └── country
    │   └── lead_time_days
    └── tags
```

## Tables

### `products_json` — Subtrees as JSON (nested_output_format: "json")

| Column | Source | Notes |
|--------|--------|-------|
| `product_id` | `@id` | Flattened normally |
| `status` | `@status` | Flattened normally |
| `product_name` | `name` | Flattened normally |
| `category` | `category` | Flattened normally |
| `price` | `price` | Flattened normally |
| `currency` | `price/@currency` | Attribute extraction |
| `specs_json` | `specifications` | Captured as JSON via xml_paths |
| `supplier_json` | `supplier` | Captured as JSON via xml_paths |
| `tags` | `tags` | Flattened normally |

### `products_xml` — Subtrees as XML (nested_output_format: "xml")

| Column | Source | Notes |
|--------|--------|-------|
| `product_id` | `@id` | Flattened normally |
| `status` | `@status` | Flattened normally |
| `product_name` | `name` | Flattened normally |
| `category` | `category` | Flattened normally |
| `price` | `price` | Flattened normally |
| `currency` | `price/@currency` | Attribute extraction |
| `specs_xml` | `specifications` | Captured as XML fragment via xml_paths |
| `supplier_xml` | `supplier` | Captured as XML fragment via xml_paths |
| `tags` | `tags` | Flattened normally |

## How to Verify

Run the **Summary** query (#13) to see PASS/FAIL for all 7 checks:

```sql
SELECT check_name, result FROM (...) ORDER BY check_name;
```

## What This Tests

1. **xml_paths** — Complex subtrees captured as single string columns
2. **nested_output_format: "json"** — Subtrees serialized as JSON objects
3. **nested_output_format: "xml"** — Subtrees serialized as XML fragments
4. **Multiple xml_paths** — Two subtrees captured per row (specifications + supplier)
5. **xml_paths + include_paths** — Captured subtrees coexist with normally flattened fields
6. **xml_paths + column_mappings** — Captured columns renamed (specifications → specs_json)
7. **Deep nesting preserved** — Nested dimensions/address structure retained in output
