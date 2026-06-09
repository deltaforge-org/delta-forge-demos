# H3 Point-in-Polygon — 1M Row Performance

Demonstrates blazing-fast point-in-polygon queries using H3 hexagonal spatial
indexing with 1,000,000 driver GPS positions across 12 pricing zones in 8 world
cities.

## What is H3?

H3 is Uber's open-source **hexagonal hierarchical spatial index**. It divides
the entire surface of the Earth into hexagonal cells at 16 resolutions (0–15).
Every point on Earth maps to exactly one hexagonal cell at each resolution.

At **resolution 9** (used in this demo), each hexagon has:
- **Edge length:** ~201 meters (about 2 city blocks)
- **Area:** ~105,000 m² (roughly a city block)

A cell ID is a single 64-bit integer — fast to store, compare, and join on.

## Why Hexagons?

Squares and triangles have problems for spatial analysis:
- **Squares** have two kinds of neighbors (edge and corner) at different distances
- **Triangles** have inconsistent orientation

Hexagons are the most regular polygon that tiles a plane:
- Every hexagon has **exactly 6 neighbors**, all at the same distance
- **No orientation bias** — no preferred direction
- **Consistent area** — unlike square grids near the poles, H3 hexagons are
  roughly equal-area worldwide

## The Point-in-Polygon Problem

**Traditional approach:**
For each of 1,000,000 driver positions, test whether the coordinate falls
inside each polygon using ray-casting or winding-number algorithms. Every
polygon vertex must be checked → **O(points × polygon_vertices)**.

**With H3:**
1. Convert each driver's (lat, lng) to an H3 cell ID → one function call
2. Pre-expand each zone polygon into a set of H3 cell IDs → `h3_polyfill()`
3. JOIN on cell ID equality → standard hash join, **O(1) per row**

No trigonometry. No winding numbers. No ray casting. Just an integer match.

## Data Story

A ride-sharing company operates across 8 cities worldwide. Every second,
1,000,000 active drivers report their GPS position. The system must instantly
determine which **pricing zone** each driver is in:

- **Airport zones** (25% surcharge): SFO, JFK, CDG, Heathrow, Narita
- **Downtown zones** (10–15% surcharge): SF, Manhattan, Paris, London,
  Tokyo Shibuya, Sydney CBD, LA Downtown

## Tables & Views

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `zones` | Delta Table | 12 | Pricing zones as WKT polygons |
| `driver_positions` | Delta Table | 1,000,000 | GPS pings from drivers |
| `driver_cells` | View | 1,000,000 | Drivers + H3 cell ID (computed on read) |
| `zone_cells` | View | ~5,000+ | Zones expanded to H3 cells via polyfill |

## Schema

**zones:** `zone_id INT, zone_name VARCHAR, zone_type VARCHAR, city VARCHAR, country VARCHAR, polygon_wkt VARCHAR, surcharge_pct DOUBLE`

**driver_positions:** `id BIGINT, lat DOUBLE, lng DOUBLE, driver_id VARCHAR, city VARCHAR`

**driver_cells (view):** driver_positions columns + `h3_cell UBIGINT`

**zone_cells (view):** zone columns + `h3_cell UBIGINT` (one row per covering cell)

## Driver Distribution

| City | Drivers | ID Range |
|------|---------|----------|
| San Francisco | 150,000 | 1 – 150,000 |
| New York | 150,000 | 150,001 – 300,000 |
| Paris | 150,000 | 300,001 – 450,000 |
| London | 150,000 | 450,001 – 600,000 |
| Tokyo | 150,000 | 600,001 – 750,000 |
| Sydney | 100,000 | 750,001 – 850,000 |
| Los Angeles | 100,000 | 850,001 – 950,000 |
| Global scatter | 50,000 | 950,001 – 1,000,000 |

## Query Progression

The 15 queries build understanding step-by-step:

1. **What is an H3 cell?** — Convert a coordinate to a cell ID
2. **How big is one cell?** — Area and edge length at resolution 9
3. **How does polyfill work?** — Zone polygon → H3 cell count
4. **Total driver count** — Verify 1,000,000
5. **All 12 zones** — Display zone metadata
6. **Drivers per city** — Verify distribution
7. **H3 cells per zone** — Airport (small) vs downtown (large)
8. **The million-row spatial join** — Drivers per zone
9. **Unmatched drivers** — Outside all zones
10. **Single-point lookup** — Is this coordinate in a zone?
11. **Surge pricing impact** — Drivers x surcharge by tier
12. **Busiest zones ranked** — Real-time dashboard query
13. **Airport vs downtown density** — Drivers per H3 cell
14. **Coverage by city** — Match rate per city
15. **Summary PASS/FAIL** — 10 automated checks

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Total drivers | 1,000,000 | Sum of 7 city clusters + global scatter |
| Zone count | 12 | 5 airports + 7 downtowns |
| SFO point in SFO zone | Match | Geographic truth (37.62, -122.38 inside polygon) |
| NYC point outside SFO zone | No match | Geographic truth (40.71 not near SF) |
| Matched + unmatched = 1M | Identity | Accounting invariant |
| All zones have cells | > 0 per zone | Polyfill of valid polygon |
| Cell area (res 9) | 50K–200K m² | H3 documentation |
| Edge length (res 9) | 100–250 m | H3 documentation |
| String round-trip | Identity | cell → hex → cell |
| SF driver count | 150,000 | Generation parameter |

## How to Verify

Run **Query #15 (Summary)** to see PASS/FAIL for all 10 checks:

```sql
SELECT check_name, result FROM (...) ORDER BY check_name;
```

All checks should return `PASS`.

## What Makes This Demo Different

- **1 million rows** — large enough to demonstrate real H3 performance gains
- **Educational** — progressive queries explain H3 concepts before using them
- **Real-world scenario** — ride-share geofencing is a production H3 use case
- **Delta tables** — data generated inline via `generate_series()`, no files needed
- **Known values** — every check uses verifiable geographic or H3 constants
