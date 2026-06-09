# Graph Stress Test — 1M-Node Performance Benchmark

Extreme-scale graph performance benchmark with 1,000,000 vertex nodes and
5,000,000+ directed edges. Designed to stress-test graph rendering, layout
algorithms, Cypher query engine, and memory handling at massive scale.

## Data Story

A massive enterprise with 1,000,000 employees across 20 departments, 15 global
cities, and 200 project teams. Six types of connections link them: intra-department
colleagues, cross-department collaboration, mentorship, project-team bonds,
city-local social ties, and random weak ties. All data is deterministically
generated via `generate_series()` for full reproducibility.

## Tables & Views

| Object | Type | Rows | Purpose |
| ------ | ---- | ---- | ------- |
| `st_departments` | Delta Table | 20 | Department lookup (name, floor, budget, region) |
| `st_people` | Delta Table | 1,000,000 | Vertex nodes (deterministic generation) |
| `st_edges` | Delta Table | 5,000,000+ | Directed edges with weight and type (6 batches) |
| `st_people_stats` | View | 1,000,000 | Per-person degree centrality metrics |
| `st_dept_matrix` | View | ~400 | Cross-department connection matrix |

## Schema

**st_departments:** `dept_id INT, dept_name STRING, floor_num INT, budget_k INT, region STRING`

**st_people:** `id BIGINT, name STRING, age INT, department STRING, city STRING, project_team STRING, title STRING, hire_year INT, level STRING, salary_band STRING, active BOOLEAN`

**st_edges:** `id BIGINT, src BIGINT, dst BIGINT, weight DOUBLE, relationship_type STRING, since_year INT`

## Data Generation

- **People:** `generate_series(1, 1000000)` with golden-ratio quasi-random age distribution
- **Batch 1 — Intra-department:** ~800K same-department pairings (weight: 0.5-1.0)
- **Batch 2 — Cross-department:** ~1M cross-department collaboration (weight: 0.2-0.7)
- **Batch 3 — Mentorship:** ~500K senior-to-junior pairings (weight: 0.6-1.0)
- **Batch 4 — Project-team:** ~1M same-team connections (weight: 0.3-0.7)
- **Batch 5 — City-local:** ~800K same-city social bonds (weight: 0.1-0.5)
- **Batch 6 — Weak ties:** ~1M random long-range connections (weight: 0.05-0.3)

## Part 1: Raw Query Performance (Queries 1–38)

Aggregation, analytics, and Cypher algorithm benchmarks. These return
summary/tabular data — no graph visualization rendering.

### SQL Queries (1–21)

| # | Analysis | Description |
| --- | -------- | ----------- |
| 1 | Node count | Verify 1M nodes |
| 2 | Edge count | Verify 5M+ edges |
| 3 | Department distribution | 20-department breakdown |
| 4 | Relationship types | Edge category statistics |
| 5 | City distribution | 15-city breakdown |
| 6 | Top 25 most connected | Degree centrality (heavy aggregation) |
| 7 | Degree histogram | Degree distribution buckets |
| 8 | Department cross-pollination | 20x20 department matrix |
| 9 | Intra-department density | Internal cohesion metrics |
| 10 | City network | 15x15 city connection matrix |
| 11 | Bridge nodes | People connecting 10+ departments |
| 12 | 2-hop neighborhood | Multi-hop traversal from node 1 |
| 13 | Mentor network | Level-to-level mentorship patterns |
| 14 | Project team cohesion | Within vs across team connections |
| 15 | Reciprocal connections | Bidirectional edge detection (heavy self-join) |
| 16 | Yearly growth | Edge creation timeline |
| 17 | Level-to-level flow | Seniority connection patterns |
| 18 | Region-to-region flow | Geographic connection patterns |
| 19 | PageRank approximation | SQL-based simplified PageRank |
| 20 | Graph statistics | Full dataset summary |
| 21 | Verification | PASS/FAIL checks |

### Cypher Algorithm Queries (22–38)

| # | Algorithm | Description |
| --- | --------- | ----------- |
| 22 | Node count | Full scan of 1M nodes |
| 23 | Edge count | Full scan of 5M+ edges |
| 24 | Filtered nodes | Property filter on 1M nodes |
| 25 | Directed relationships | Pattern match with edge properties |
| 26 | 2-hop paths | Multi-hop Cypher traversal |
| 27 | Variable-length paths | `[*1..2]` reachability |
| 28 | Degree centrality | `algo.degree()` on full graph |
| 29 | PageRank | `algo.pageRank()` — 5 iterations on 5M+ edges |
| 30 | Connected components | `algo.connectedComponents()` on 1M nodes |
| 31 | Louvain communities | `algo.louvain()` modularity clustering |
| 32 | Betweenness centrality | `algo.betweenness()` bridge detection |
| 33 | Triangle count | `algo.triangleCount()` clustering coefficient |
| 34 | Shortest path | `algo.shortestPath()` Dijkstra across 1M nodes |
| 35 | BFS traversal | `algo.bfs()` breadth-first on 1M nodes |
| 36 | SCC | `algo.scc()` strongly connected components |
| 37 | Closeness centrality | `algo.closeness()` central node detection |
| 38 | Minimum spanning tree | `algo.mst()` on full graph |

## Part 2: Graph Visualization Stress Test (Queries 39–50)

These queries return **actual node + edge data** designed to be rendered in the
graph visualizer. Run them progressively to find the breaking point where the
renderer lags, freezes, or crashes.

### Progressive Scale Ladder

| # | Scale | Nodes | Edges (approx) | Expected Behavior |
| --- | ----- | ----- | --------------- | ----------------- |
| 39 | Warm-up | 100 | ~500 | Should render fine |
| 40 | Small | 500 | ~2,500 | Should render fine |
| 41 | Medium | 1,000 | ~5,000 | May start to lag |
| 42 | Large | 5,000 | ~25,000 | Noticeable layout time |
| 43 | Very large | 10,000 | ~50,000 | Significant lag expected |
| 44 | Extreme | 50,000 | ~250,000 | May freeze or crash |
| 45 | Survival | 100,000 | ~500,000 | Likely crash |
| 46 | Full blast | 1,000,000 | 5,000,000+ | Almost certainly crash |

### Cypher Visualization Tests

| # | Scale | What it returns | Expected Behavior |
| --- | ----- | --------------- | ----------------- |
| 47 | 100 nodes | Nodes only | Should render fine |
| 48 | 1,000 nodes | Nodes only | May lag |
| 49 | 10,000 nodes | Nodes only | Likely lag/crash |
| 50 | 1,000,000 nodes | All nodes | Crash test |

## Known Verification Values

| Check | Expected | Source |
| ----- | -------- | ------ |
| Node count | 1,000,000 | generate_series(1, 1000000) |
| Department count | 20 | Static insert |
| Edge count | >= 4,000,000 | Deterministic generation (6 batches) |
| City count | 15 | Modular assignment |
| Project teams | 200 | Modular assignment |
| Active people | >= 900,000 | ~95% active rate |
| Weight range | 0.0–1.0 | Generation formula |
| No self-loops | 0 | WHERE src != dst filter |

## Performance Notes

- **Setup time:** Expect several minutes for data generation depending on hardware
- **Edge batches:** 6 separate INSERT statements to avoid overwhelming memory
- **Cypher algorithms:** PageRank, Louvain, and Betweenness are the heaviest — expect significant computation time at 1M scale
- **Rendering:** The graph visualizer will need to handle extreme node/edge counts — this is the primary stress test target
