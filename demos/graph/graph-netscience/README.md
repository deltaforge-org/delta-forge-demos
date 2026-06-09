# NetScience — Coauthorship Network of Network Scientists

A coauthorship network of 1,461 scientists working in network theory and experiment (Newman, 2006). Edges represent coauthorship with non-uniform weights reflecting collaboration strength. The network has clear research-group community structure (modularity ~0.95) and multiple connected components.

## Data Story

Mark Newman compiled this dataset to map the landscape of network science itself. Each node is a scientist who published at least one paper on networks; each edge connects coauthors, with weight proportional to collaboration strength. The result reveals tightly-knit research groups with sparse connections between them, producing exceptionally high modularity. Many authors are isolated (single-author papers or no network-science coauthors), creating multiple connected components — a realistic feature absent from toy graphs.

## Data Source

M. E. J. Newman, "Finding community structure in very large networks," Physical Review E 70, 066111, 2004.

- Newman's network data: http://www-personal.umich.edu/~mejn/netdata/
- KONECT: http://konect.cc/networks/dimacs10-netscience/

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| vertices | Delta Table | 1,461 | Authors (IDs 0-1460) |
| edges | Delta Table | 5,484 | Coauthorships (2,742 undirected, stored bidirectionally) |

## Schema

**vertices:** `vertex_id BIGINT`

**edges:** `src BIGINT, dst BIGINT, weight DOUBLE`

## Graph Properties

- **Vertices:** 1,461 (network scientists)
- **Edges:** 2,742 undirected (5,484 rows, bidirectional)
- **Weighted:** Yes, non-uniform (coauthorship strength)
- **Connected:** No (multiple connected components, isolated nodes)
- **Self-loops:** None
- **Modularity:** ~0.95 (very clear community structure)

## Known Reference Values

| Metric | Expected Value | Source |
|--------|---------------|--------|
| Vertex count | 1,461 | Newman dataset |
| Edge count | 2,742 undirected (5,484 rows) | Newman dataset |
| Connected components | > 1 (many isolated authors) | Network structure |
| Modularity | ~0.95 | Published benchmarks |
| Weight range | Non-uniform (varies by collaboration) | Dataset |
| Communities | Many well-separated research groups | Louvain / published |

## Algorithms Demonstrated

| Algorithm | Query | Description |
|-----------|-------|-------------|
| Degree distribution | #6 | Coauthor count per scientist |
| Weighted degree | #8 | Total collaboration strength |
| PageRank | #10 | Influence ranking |
| Degree centrality | #11 | Normalized degree |
| Betweenness centrality | #12 | Bridge scientist identification |
| Closeness centrality | #13 | Proximity to all others |
| Community detection | #14 | Recover research groups |
| Connected components | #15 | Identify disconnected subgraphs |
| Shortest path | #16 | Distance between authors |

## How to Verify

Run **Query #17 (Verification Summary)** to see PASS/FAIL for all structural checks.
