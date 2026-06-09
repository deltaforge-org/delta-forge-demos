# Email-Eu-core — European Institution Email Network

A directed email communication network from a European research institution (SNAP dataset, Leskovec et al.). 1,005 members connected by 25,571 directed email edges. Unlike the other graph demos, this dataset is directed (not symmetric) and contains self-loops. It features 42 ground-truth department communities, 105,461 triangles, and a diameter of 7.

## Data Story

Researchers at Stanford compiled this dataset from email logs of a large European research institution. Each node represents a member of the institution, and each directed edge indicates that at least one email was sent from person A to person B. The ground-truth communities correspond to the 42 departments in the institution. The directed nature of the graph means that communication is often asymmetric — a junior researcher may email a department head without receiving a reply. Self-loops (emails to oneself, e.g., reminders or notes) are present and realistic. The network has a small diameter of 7, reflecting the "small world" property of institutional communication.

## Data Source

J. Leskovec, J. Kleinberg, C. Faloutsos, "Graph evolution: Densification and shrinking diameters," ACM Transactions on Knowledge Discovery from Data (TKDD), 2007.

- SNAP: https://snap.stanford.edu/data/email-Eu-core.html
- H. Yin, A. R. Benson, J. Leskovec, D. F. Gleich, "Local higher-order graph clustering," KDD 2017.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| vertices | Delta Table | 1,005 | Institution members (IDs 0-1004) |
| edges | Delta Table | 25,571 | Directed email edges |

## Schema

**vertices:** `vertex_id BIGINT`

**edges:** `src BIGINT, dst BIGINT, weight DOUBLE`

## Graph Properties

- **Vertices:** 1,005 (institution members)
- **Edges:** 25,571 directed (NOT symmetric)
- **Weighted:** All weights = 1.0 (effectively unweighted)
- **Connected:** 10 weakly connected components (largest: 986 nodes, 98.1%)
- **Self-loops:** Yes (emails to self)
- **Diameter:** 7
- **Triangles:** 105,461
- **Avg clustering coefficient:** ~0.3994
- **Ground-truth communities:** 42 departments

## Known Reference Values

| Metric | Expected Value | Source |
|--------|---------------|--------|
| Vertex count | 1,005 | SNAP dataset |
| Edge count | 25,571 directed | SNAP dataset |
| Weakly connected components | 10 | SNAP dataset |
| Largest component | 986 nodes (98.1%) | SNAP dataset |
| Triangle count | 105,461 | SNAP dataset |
| Diameter | 7 | SNAP dataset |
| Avg clustering coefficient | ~0.3994 | SNAP dataset |
| Ground-truth communities | 42 departments | SNAP dataset |

## Algorithms Demonstrated

| Algorithm | Query | Description |
|-----------|-------|-------------|
| Out-degree distribution | #6 | Emails sent per member |
| In-degree distribution | #7 | Emails received per member |
| Total degree | #8 | Combined communication activity |
| PageRank | #10 | Influence ranking (directed) |
| Degree centrality | #11 | Normalized degree |
| Betweenness centrality | #12 | Communication bridge identification |
| Closeness centrality | #13 | Proximity to all others |
| Community detection | #14 | Recover department structure |
| Connected components | #15 | Identify weakly connected subgraphs |
| Shortest path | #16 | Directed distance between members |

## How to Verify

Run **Query #17 (Verification Summary)** to see PASS/FAIL for all structural checks.
