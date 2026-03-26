# Graph Hybrid Mode — Columns + JSON Extras

Demonstrates the **hybrid** property storage mode for graph tables — the
balanced approach where frequently queried properties are columns while
optional/extensible properties live in a JSON extras column.

## Data Story

The same 5-person social graph, stored with a hybrid strategy. Core properties
that are frequently filtered or joined on (name, age for vertices; weight,
relationship_type for edges) are dedicated columns. Less common or variable
properties (department, city, skills, frequency, context) live in a JSON
`extras` column.

```
Priya(30,Engineering,NYC) -----> Marcus(25,Marketing,LA)
  |   ^                            |
  |   |                            | friend
  |   |                            |
  |   +--- Wei(32,Finance,NYC)     |
  |         ^                      |
  v         | colleague            v
Sofia(35,HR,Chicago) ----------> James(28,Engineering,SF)
         manager
```

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `persons_hybrid` | Delta Table | 5 | Vertex nodes — core columns + JSON extras |
| `friendships_hybrid` | Delta Table | 6 | Directed edges — core columns + JSON extras |

## Schema

**persons_hybrid:** `id BIGINT, name STRING, age INT, extras STRING`

**friendships_hybrid:** `src BIGINT, dst BIGINT, weight DOUBLE, relationship_type STRING, extras STRING`

## Column vs JSON Split

| Table | Core Columns | JSON Extras |
|-------|-------------|-------------|
| persons_hybrid | id, name, age | department, city, skills, level, active |
| friendships_hybrid | src, dst, weight, relationship_type | since_year, frequency, context, rating |

## Access Patterns Demonstrated

| Pattern | Example | Benefit |
|---------|---------|---------|
| Column-only query | `WHERE age > 28` | Full pushdown, fastest |
| JSON-only extraction | `json_get_str(extras, '$.city')` | Flexible, schema-free |
| Mixed filtering | `WHERE relationship_type = 'colleague' AND json_get_str(extras, '$.context') = 'work'` | Best of both worlds |

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Person count | 5 | Static insert |
| Edge count | 6 | Static insert |
| Priya name (column) | "Priya" | Direct column access |
| Priya dept (extras) | "Engineering" | JSON extraction |
| Mentor type (column) | 1 edge | Direct column filter |
| Work context (extras) | 4 edges | JSON extraction filter |

## How to Verify

Run **Query #11 (Summary)** to see PASS/FAIL for all 10 checks. All should return `PASS`.

## What Makes This Demo Different

- **Hybrid mode** — columns for frequent queries + JSON for extensible properties
- **Mixed access patterns** — demonstrates column pushdown alongside JSON extraction
- **Balanced trade-off** — performance of flattened mode with flexibility of JSON mode
- **Graph configuration:** `property_mode='hybrid'` with core columns + extras
