# H3 GPS Fleet Tracker

Demonstrates H3 hexagonal spatial indexing with Delta tables for GPS fleet
tracking across 5 world cities.

## Data Story

A fleet of IoT devices transmits GPS coordinates from San Francisco, Manhattan,
Paris, London, and Tokyo. Each city contributes 2,000 pings (10,000 total).
We convert lat/lng to H3 hexagonal cell IDs at resolution 9 (~201 m edge
hexagons, ~105,000 m² area), then join against city boundaries — also expanded to H3 cells via
polygon polyfill — achieving O(1) spatial joins instead of O(n) point-in-polygon
tests.

## Tables & Views

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `landmarks` | Delta Table | 10 | Famous world landmarks with known coordinates |
| `regions` | Delta Table | 5 | City boundaries as WKT polygons |
| `gps_points` | Delta Table | 10,000 | GPS pings across 5 cities (deterministic) |
| `points_h3` | View | 10,000 | GPS points enriched with H3 cell IDs |
| `region_cells` | View | ~1,500+ | Regions expanded to H3 cell coverage |

## Schema

**landmarks:** `id INT, name VARCHAR, city VARCHAR, country VARCHAR, lat DOUBLE, lng DOUBLE`

**regions:** `region_id INT, region_name VARCHAR, country VARCHAR, polygon_wkt VARCHAR, timezone VARCHAR`

**gps_points:** `id BIGINT, lat DOUBLE, lng DOUBLE, device_id VARCHAR, city VARCHAR`

**points_h3 (view):** gps_points columns + `h3_cell UBIGINT`

**region_cells (view):** region columns + `h3_cell UBIGINT` (one row per covering cell)

## H3 Functions Demonstrated (21 total)

| Category | Functions |
|----------|-----------|
| **Coordinate conversion** | `h3_latlng_to_cell`, `h3_cell_to_lat`, `h3_cell_to_lng` |
| **Validation & properties** | `h3_is_valid_cell`, `h3_is_pentagon`, `h3_is_res_class_iii`, `h3_get_resolution` |
| **Grid topology** | `h3_hex_ring`, `h3_hex_disk`, `h3_grid_distance`, `h3_grid_path` |
| **Hierarchy** | `h3_cell_to_parent`, `h3_cell_to_children`, `h3_cell_to_center_child` |
| **Metrics** | `h3_cell_area`, `h3_cell_area_km2`, `h3_edge_length` |
| **Geometry** | `h3_cell_to_boundary`, `h3_polyfill` |
| **String conversion** | `h3_cell_to_string`, `h3_string_to_cell` |

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Hex ring at k=1 | 6 cells | H3 hexagon geometry |
| Hex ring at k=2 | 12 cells | H3 hexagon geometry |
| Hex disk at k=1 | 7 cells (center + 6) | H3 hexagon geometry |
| Children count (res 9→10) | 7 children | H3 hexagon subdivision |
| Res 9 cell area | ~105,000 m² (50K–200K) | H3 documentation |
| Res 9 edge length | ~201 m (100–250) | H3 documentation |
| Resolution class (res 9) | Class III (odd) | H3 alternating class rule |
| SF City Hall in SF polygon | Match (>0) | Geographic truth |
| Statue of Liberty in SF polygon | No match (0) | Geographic truth |
| Cell → string → cell | Identity | Round-trip invariant |

## How to Verify

Run **Query #30 (Summary)** to see PASS/FAIL for all 21 checks:

```sql
SELECT check_name, result
FROM (... summary query ...)
ORDER BY check_name;
```

All checks should return `PASS`.

## What Makes This Demo Different

- **Delta tables** — first demo using `CREATE DELTA TABLE` with inline data generation
  (no data files; all data created via `INSERT INTO ... SELECT FROM generate_series()`)
- **Computed views** — H3 cell assignment is lazy (computed on read, not stored)
- **Known values** — every query uses H3 constants from the official H3 library,
  verified by the delta-forge test suite
