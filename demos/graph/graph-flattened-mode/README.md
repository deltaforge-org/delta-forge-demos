# Graph Flattened Mode — Direct Column Access

Demonstrates the **flattened** property storage mode for graph tables — the
fastest mode where all vertex and edge properties are stored as individual
columns with full predicate pushdown.

## Data Story

A 5-person social graph with directed friendships. Priya, Marcus, Sofia, James,
and Wei work across Engineering, Marketing, HR, and Finance departments.
Six directed edges carry weight, relationship type, interaction frequency,
and context metadata — all as dedicated columns.

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
| `persons_flattened` | Delta Table | 5 | Vertex nodes — all properties as columns |
| `friendships_flattened` | Delta Table | 6 | Directed edges — all properties as columns |

## Schema

**persons_flattened:** `id BIGINT, name STRING, age INT, department STRING, city STRING, level STRING, active BOOLEAN`

**friendships_flattened:** `src BIGINT, dst BIGINT, weight DOUBLE, relationship_type STRING, since_year INT, frequency STRING, context STRING, rating INT`

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Person count | 5 | Static insert |
| Edge count | 6 | Static insert |
| Priya out-degree | 2 (Marcus, Sofia) | Graph structure |
| Engineering count | 2 (Priya, James) | Static data |
| Work context edges | 4 | Static data |
| NYC persons | 2 (Priya, Wei) | Static data |
| Max edge weight | 1.0 (Priya->Marcus mentor) | Static data |

## How to Verify

Run **Query #11 (Summary)** to see PASS/FAIL for all 10 checks. All should return `PASS`.

## What Makes This Demo Different

- **Flattened mode** — all properties as columns for maximum query performance
- **Predicate pushdown** — WHERE clauses filter at the storage layer
- **No JSON extraction** — direct column access for all graph properties
- **Graph configuration:** `property_mode='flattened'` with vertex ID `id` and edge source/destination `src`/`dst`
