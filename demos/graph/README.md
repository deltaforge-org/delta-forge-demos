# Graph Analytics Demos

SQL and Cypher demos for property graph analytics. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **Cypher Queries** -- Pattern matching, variable-length paths, advanced traversal
- **Algorithms** -- PageRank, community detection (Louvain), betweenness/closeness/degree centrality, BFS, DFS, triangle count, KNN similarity, weighted shortest paths
- **Real-World Datasets** -- Zachary's Karate Club, EU email network, NetScience co-authorship, political books, LDBC Social Network Benchmark
- **Storage Modes** -- Flattened SQL, hybrid, JSON-based graph representations
- **Graph Mutations** -- Node and edge creation, deletion, and updates
- **SQL + Cypher Mix** -- Combine graph traversal with relational joins in a single query

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
