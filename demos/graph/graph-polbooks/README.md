# Political Books — Co-Purchasing Network with Ground-Truth Communities

105 books about US politics sold on Amazon with 441 undirected co-purchasing edges. Each book belongs to one of three ground-truth communities (liberal, neutral, conservative), making this a classic benchmark for community detection with known labels.

## Data Story

Valdis Krebs compiled a network of books about US politics sold on Amazon.com. Edges connect books that are frequently co-purchased by the same buyers. The books naturally cluster into three political categories — liberal, neutral, and conservative — providing ground-truth labels for evaluating community detection algorithms. This dataset demonstrates how purchasing behavior reveals ideological structure without any explicit political labeling.

## Data Source

V. Krebs, unpublished; compiled by M. E. J. Newman.

- Newman's network data: http://www-personal.umich.edu/~mejn/netdata/

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| vertices | Delta Table | 105 | Political books (IDs 0–104) |
| edges | Delta Table | 882 | Co-purchases (441 undirected, stored bidirectionally) |

## Schema

**vertices:** `vertex_id BIGINT`

**edges:** `src BIGINT, dst BIGINT, weight DOUBLE`

## Graph Properties

- **Vertices:** 105 (political books)
- **Edges:** 441 undirected (882 rows, bidirectional)
- **Weighted:** All weights = 1.0 (effectively unweighted)
- **Connected:** Yes (single main component)
- **Self-loops:** None

## Known Reference Values

| Metric | Expected Value | Source |
|--------|---------------|--------|
| Vertex count | 105 | Krebs / Newman |
| Edge count | 441 undirected | Krebs / Newman |
| Ground-truth communities | 3 (liberal, neutral, conservative) | Krebs / Newman |
| Max degree | 25 | Degree count |
| Avg clustering coefficient | ~0.488 | Published benchmarks |

## Algorithms Demonstrated

| Algorithm | Query | Description |
|-----------|-------|-------------|
| Degree distribution | #6 | Co-purchase count per book |
| PageRank | #10 | Influence ranking |
| Degree centrality | #11 | Normalized degree |
| Betweenness centrality | #12 | Bridge node identification |
| Closeness centrality | #13 | Proximity to all others |
| Community detection | #14 | Recover political leanings |
| Connected components | #15 | Verify connectivity |
| Shortest path | #16 | Distance between books |

## How to Verify

Run **Query #17 (Verification Summary)** to see PASS/FAIL for all structural checks.
