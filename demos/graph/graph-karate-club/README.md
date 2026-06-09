# Zachary's Karate Club — Classic Graph Benchmark

The most studied graph in network science. 34 members of a university karate club with 78 undirected friendship edges. The club famously split into two factions around the instructor (node 0) and president (node 33), providing ground-truth community structure for algorithm verification.

## Data Story

In 1977, Wayne Zachary studied a karate club at an American university for two years. During this time, a conflict arose between the instructor and the club president, causing the club to split into two groups. Zachary correctly predicted all but one member's faction choice using only the friendship network — making this the canonical example of community detection in network science.

## Data Source

W. W. Zachary, "An information flow model for conflict and fission in small groups," Journal of Anthropological Research, 33(4):452-473, 1977.

- Wikipedia: https://en.wikipedia.org/wiki/Zachary%27s_karate_club
- Newman's network data: http://www-personal.umich.edu/~mejn/netdata/

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| vertices | Delta Table | 34 | Club members (IDs 0–33) |
| edges | Delta Table | 78 | Friendships (canonical src < dst; graph is UNDIRECTED) |

## Schema

**vertices:** `vertex_id BIGINT`

**edges:** `src BIGINT, dst BIGINT, weight DOUBLE`

## Graph Properties

- **Vertices:** 34 (club members)
- **Edges:** 78 undirected (canonical src < dst; engine doubles to 156 CSR edges at build time)
- **Weighted:** All weights = 1.0 (effectively unweighted)
- **Connected:** Yes (1 component)
- **Self-loops:** None

## Known Reference Values

All values verified using NetworkX 3.5 on the exact edge data in this demo.

| Metric | Expected Value | Source |
|--------|---------------|--------|
| Vertex count | 34 | Zachary (1977) |
| Edge count | 78 canonical rows (UNDIRECTED) | Zachary (1977) |
| Connected components | 1 | NetworkX verified |
| Ground-truth communities | 2 (instructor vs president) | Zachary (1977) |
| Louvain communities | typically 4 (modularity ~0.42) | NetworkX verified |
| Betweenness centrality (node 0) | 0.4376 | NetworkX verified |
| Betweenness centrality (node 33) | 0.3041 | NetworkX verified |
| Betweenness centrality (node 32) | 0.1452 | NetworkX verified |
| Closeness centrality (node 0) | 0.5690 | NetworkX verified |
| Closeness centrality (node 2) | 0.5593 | NetworkX verified |
| Closeness centrality (node 33) | 0.5500 | NetworkX verified |
| Highest out-degree | Node 33 (17) | NetworkX verified |
| 2nd highest out-degree | Node 0 (16) | NetworkX verified |
| Degree centrality total (node 33) | in=17, out=17, total=17 | NetworkX verified (undirected) |
| Degree centrality total (node 0) | in=16, out=16, total=16 | NetworkX verified (undirected) |
| PageRank (node 33, d=0.85) | 0.1009 | NetworkX verified |
| PageRank (node 0, d=0.85) | 0.0970 | NetworkX verified |
| Shortest path 0 to 33 | 0 -> 8 -> 33 (distance 2) | NetworkX verified |
| 2-hop reachability from node 0 | 26 nodes | NetworkX verified |
| Node 0 neighbors | [1,2,3,4,5,6,7,8,10,11,12,13,17,19,21,31] | NetworkX verified |
| Strongly connected components | 1 SCC (size 34) | NetworkX verified |
| Triangle count (total unique) | 45 | NetworkX verified |
| Triangles (node 0) | 18 | NetworkX verified |
| Triangles (node 33) | 15 | NetworkX verified |
| All paths from node 0 max dist | 3.0 | NetworkX verified |
| BFS depths from node 0 | 0:1, 1:16, 2:9, 3:8 | NetworkX verified |
| MST edges / total weight | 33 / 33.0 | NetworkX verified |
| KNN node 0 top-1 (Jaccard) | Node 1 = 0.3889 | NetworkX verified |
| Similarity 0 vs 33 (Jaccard) | 0.1379 (4 common neighbors) | NetworkX verified |

**Note:** Graph is created as UNDIRECTED with canonical (src < dst) edge
storage. The engine materializes the reverse direction internally when
building the CSR, so `MATCH (a)-[r]->(b)` traverses both directions
automatically. For UNDIRECTED graphs `algo.degree()` reports
`in_degree = out_degree = total_degree = friend count` (no doubling).

## Algorithms Demonstrated

All 15 graph algorithms supported by DeltaForge are covered (queries 10-24).

| Algorithm | Query | YIELD columns | Description |
|-----------|-------|---------------|-------------|
| Degree distribution | #6 | (Cypher COUNT) | Friendship count per member |
| PageRank | #10 | node_id, score, rank | Influence ranking |
| Degree centrality | #11 | node_id, in/out/total_degree | Raw connection counts |
| Betweenness centrality | #12 | node_id, centrality, rank | Bridge node identification |
| Closeness centrality | #13 | node_id, closeness, rank | Proximity to all others |
| Community detection | #14 | node_id, community_id | Recover faction split (Louvain) |
| Connected components | #15 | node_id, component_id | Weak connectivity (WCC) |
| Shortest path | #16 | node_id, step, distance | Dijkstra between two nodes |
| Strongly connected | #17 | node_id, component_id | Strong connectivity (SCC) |
| Triangle count | #18 | node_id, triangle_count | Clustering structure |
| All shortest paths | #19 | node_id, distance | Dijkstra from source to all |
| BFS traversal | #20 | node_id, depth | Breadth-first layer structure |
| DFS traversal | #21 | node_id, discovery/finish_time | Depth-first ordering |
| Minimum spanning tree | #22 | sourceId, targetId, weight | Lightest connecting tree |
| KNN similarity | #23 | node_id, similarity, rank | K-nearest by Jaccard |
| Pairwise similarity | #24 | score | Jaccard between two nodes |

## How to Verify

Run **Query #25 (Verification Summary)** to see PASS/FAIL for all structural checks.
