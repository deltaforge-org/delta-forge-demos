# Graph JSON Mode — Schema-Free Properties

Demonstrates the **JSON** property storage mode for graph tables — the most
flexible mode where all vertex and edge properties are stored in a single
JSON string column, enabling schema-free evolution.

## Data Story

The same 5-person social graph as the flattened demo, but with a fundamentally
different storage strategy. Only structural fields (vertex ID, edge src/dst)
are columns — everything else lives inside a `props` JSON column. This enables
adding new properties without ALTER TABLE and supports heterogeneous vertex
types in a single table.

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
| `persons_json` | Delta Table | 5 | Vertex nodes — id + JSON props column |
| `friendships_json` | Delta Table | 6 | Directed edges — src, dst + JSON props column |

## Schema

**persons_json:** `id BIGINT, props STRING`

**friendships_json:** `src BIGINT, dst BIGINT, props STRING`

## JSON Property Structure

**Person props:**
```json
{
  "name": "Priya", "age": 30, "department": "Engineering",
  "city": "NYC", "skills": ["rust", "python"],
  "level": "senior", "active": true
}
```

**Friendship props:**
```json
{
  "weight": 1.0, "relationship_type": "mentor",
  "since_year": 2020, "frequency": "daily",
  "context": "work", "rating": 5
}
```

## JSON Functions Demonstrated

| Function | Purpose |
|----------|---------|
| `json_get_str(col, '$.path')` | Extract string value |
| `json_get_int(col, '$.path')` | Extract integer value |
| `json_get_float(col, '$.path')` | Extract float value |

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Person count | 5 | Static insert |
| Edge count | 6 | Static insert |
| Priya name from JSON | "Priya" | json_get_str extraction |
| Priya age from JSON | 30 | json_get_int extraction |
| Skills is array | Starts with `[` | JSON array format |
| Max weight from JSON | 1.0 | json_get_float extraction |

## How to Verify

Run **Query #11 (Summary)** to see PASS/FAIL for all 10 checks. All should return `PASS`.

## What Makes This Demo Different

- **JSON mode** — all properties in a single `props` column for maximum flexibility
- **Schema-free** — add new properties without ALTER TABLE
- **Array support** — skills stored as native JSON arrays
- **Graph configuration:** `property_mode='json'`, `json_column='props'`
