# Graph Cypher Queries — Pattern Matching & Algorithms

Demonstrates **Cypher query language** support in Delta Forge — declarative
graph pattern matching and 15+ built-in graph algorithm procedures.

## Data Story

The same 5-person social graph as the other graph demos, but queried using
Cypher instead of SQL JOINs. Cypher's `MATCH (a)-[r]->(b)` syntax makes
graph traversal intuitive, and `CALL algo.*` procedures provide one-line
access to PageRank, shortest paths, community detection, and more.

```text
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
| `persons_cypher` | Delta Table | 5 | Vertex nodes — all properties as columns |
| `friendships_cypher` | Delta Table | 6 | Directed edges — standard src/dst columns |

## Schema

**persons_cypher:** `id BIGINT, name STRING, age INT, department STRING, city STRING, level STRING, active BOOLEAN`

**friendships_cypher:** `src BIGINT, dst BIGINT, weight DOUBLE, relationship_type STRING, since_year INT, frequency STRING, context STRING, rating INT`

## Cypher Syntax

| Pattern | Example | Description |
|---------|---------|-------------|
| All nodes | `MATCH (n) RETURN n` | Find every vertex |
| Directed edges | `MATCH (a)-[r]->(b) RETURN a, b` | Follow directed relationships |
| Property filter | `MATCH (a {name: 'Priya'})-[r]->(b)` | Filter by node properties |
| WHERE clause | `WHERE a.age > 28` | Conditional filtering |
| Variable paths | `MATCH (a)-[*1..2]->(b)` | Paths of length 1 to 2 |

## Graph Algorithms (CALL Procedures)

| Category | Procedure | Description |
|----------|-----------|-------------|
| Centrality | `algo.pageRank` | Influence ranking via link analysis |
| Centrality | `algo.degree` | In/out/total degree per node |
| Centrality | `algo.betweenness` | Bridge node detection |
| Centrality | `algo.closeness` | Average distance to all other nodes |
| Community | `algo.connectedComponents` | Weakly connected component grouping |
| Community | `algo.scc` | Strongly connected components (directed) |
| Community | `algo.triangleCount` | Triangle participation count |
| Community | `algo.louvain` | Modularity-based community detection |
| Pathfinding | `algo.shortestPath` | Dijkstra between two nodes |
| Pathfinding | `algo.allShortestPaths` | Distances from one node to all |
| Pathfinding | `algo.bfs` | Breadth-first search traversal |
| Pathfinding | `algo.dfs` | Depth-first search traversal |
| Pathfinding | `algo.mst` | Minimum spanning tree |
| Similarity | `algo.knn` | K-nearest neighbors |
| Similarity | `algo.similarity` | Pairwise Jaccard/Adamic-Adar similarity |

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Person count | 5 | Static insert |
| Edge count | 6 | Static insert |
| Priya out-degree | 2 (Marcus, Sofia) | Graph structure |
| Sofia in-degree | 2 (Priya, Marcus) | Graph structure |
| Triangle | Priya-Marcus-Sofia | Nodes 1,2,3 |
| Connected components | 1 (all connected) | Cycle 1->3->4->5->1 |
| Shortest path 1->5 | 1->3->4->5 (dist 2.4) | Dijkstra |

## How to Verify

Run **Query #25 (Summary)** to see PASS/FAIL for all 10 checks. All should return `PASS`.

## What Makes This Demo Different

- **Cypher language** — declarative pattern matching instead of SQL JOINs
- **15+ graph algorithms** — PageRank, betweenness, Louvain, shortest path, BFS/DFS, MST, KNN
- **Named graph definition** — `CREATE GRAPH` registers vertex/edge tables for Cypher and UI
- **Same data, different lens** — compare SQL queries (flattened demo) vs Cypher queries (this demo)
