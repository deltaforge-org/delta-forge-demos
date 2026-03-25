-- ############################################################################
-- ############################################################################
--
--   ZACHARY'S KARATE CLUB — CLASSIC GRAPH BENCHMARK
--   34 Vertices / 78 Undirected Edges (156 rows) / Weight = 1.0
--
-- ############################################################################
-- ############################################################################
--
-- The most studied graph in network science (Zachary, 1977). A university
-- karate club split into two factions around the instructor (node 0) and
-- president (node 33). Decades of published results provide golden values
-- for community detection, centrality, and structural metrics.
--
-- PART 1: DATA INTEGRITY CHECKS (queries 1–4)
-- PART 2: CYPHER — GRAPH EXPLORATION (queries 5–9)
-- PART 3: CYPHER — GRAPH ALGORITHMS (queries 10–24)
-- PART 4: VERIFICATION SUMMARY (query 25)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY CHECKS
-- ############################################################################


-- ============================================================================
-- 1. VERTEX & EDGE COUNTS — Verify data loaded correctly
-- ============================================================================
-- 34 vertices, 156 edge rows (78 undirected edges x 2)

-- Verify vertex count
ASSERT VALUE row_count = 34
SELECT COUNT(*) AS row_count FROM {{zone_name}}.karate.vertices;

-- Verify edge count (78 undirected edges x 2)
ASSERT VALUE row_count = 156
SELECT COUNT(*) AS row_count FROM {{zone_name}}.karate.edges;


-- ============================================================================
-- 2. GRAPH CONFIG — Verify graph definition
-- ============================================================================

SHOW GRAPH;


-- ============================================================================
-- 3. REFERENTIAL INTEGRITY — All edges have valid endpoints
-- ============================================================================

ASSERT VALUE orphan_edges = 0
SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.karate.edges e
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.karate.vertices v WHERE v.vertex_id = e.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.karate.vertices v WHERE v.vertex_id = e.dst);


-- ============================================================================
-- 4. SELF-LOOP CHECK — No member should be friends with themselves
-- ============================================================================

ASSERT VALUE self_loops = 0
SELECT COUNT(*) AS self_loops
FROM {{zone_name}}.karate.edges
WHERE src = dst;


-- ############################################################################
-- PART 2: CYPHER — GRAPH EXPLORATION
-- ############################################################################


-- ============================================================================
-- 5. BROWSE VERTICES — List all 34 club members
-- ============================================================================

ASSERT ROW_COUNT = 34
USE {{zone_name}}.karate.karate_club
MATCH (v)
RETURN v.id AS member_id, v.name AS name, v.role AS role
ORDER BY member_id;


-- ============================================================================
-- 6. DEGREE DISTRIBUTION — How many friends does each member have?
-- ============================================================================
-- Counts outgoing edges per node. Since edges are stored bidirectionally,
-- out-degree equals the undirected degree.
-- Known (NetworkX-verified): Node 33 = 17, Node 0 = 16, Node 32 = 12,
-- Node 2 = 10, Node 1 = 9.

-- All 34 nodes have at least one edge
ASSERT ROW_COUNT = 34
ASSERT VALUE degree = 17 WHERE member_id = 33
ASSERT VALUE degree = 16 WHERE member_id = 0
USE {{zone_name}}.karate.karate_club
MATCH (a)-[r]->(b)
RETURN a.id AS member_id, a.name AS name, COUNT(r) AS degree
ORDER BY degree DESC, member_id ASC;


-- ============================================================================
-- 7. TOP HUBS — The two faction leaders
-- ============================================================================
-- Expected top-5 (NetworkX-verified): 33(17), 0(16), 32(12), 2(10), 1(9).

ASSERT ROW_COUNT = 5
ASSERT VALUE degree = 17 WHERE member_id = 33
ASSERT VALUE degree = 16 WHERE member_id = 0
ASSERT VALUE degree = 12 WHERE member_id = 32
ASSERT VALUE degree = 10 WHERE member_id = 2
ASSERT VALUE degree = 9 WHERE member_id = 1
USE {{zone_name}}.karate.karate_club
MATCH (a)-[r]->(b)
RETURN a.id AS member_id, a.name AS name, COUNT(r) AS degree
ORDER BY degree DESC
LIMIT 5;


-- ============================================================================
-- 8. NEIGHBORHOOD OF NODE 0 — Instructor's faction
-- ============================================================================
-- The instructor (node 0) has 16 direct friends (NetworkX-verified):
-- [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 17, 19, 21, 31]
-- Note: Node 33 (president) is NOT a direct neighbor of node 0.

ASSERT ROW_COUNT = 16
USE {{zone_name}}.karate.karate_club
MATCH (a)-[r]->(b)
WHERE a.id = 0
RETURN b.id AS friend_id, b.name AS friend_name
ORDER BY friend_id;


-- ============================================================================
-- 9. TWO-HOP REACHABILITY FROM NODE 0 — How far does influence reach?
-- ============================================================================
-- Expected: 26 distinct nodes reachable within 2 hops (NetworkX-verified).
-- Includes node 0 itself (reachable via 2-hop cycles, e.g. 0→1→0).
-- 8 nodes are NOT reachable in 2 hops: [14, 15, 18, 20, 22, 23, 26, 29].

ASSERT VALUE reachable_in_2_hops = 26
USE {{zone_name}}.karate.karate_club
MATCH (a)-[*1..2]->(b)
WHERE a.id = 0
RETURN COUNT(DISTINCT b.id) AS reachable_in_2_hops;


-- ############################################################################
-- PART 3: CYPHER — GRAPH ALGORITHMS
-- ############################################################################


-- ============================================================================
-- 10. PAGERANK — Identify most influential members
-- ============================================================================
-- NetworkX-verified top-5 (damping=0.85):
--   Node 33 = 0.100918, Node 0 = 0.097002, Node 32 = 0.071692,
--   Node 2 = 0.057078,  Node 1 = 0.052878
-- Note: may require >20 iterations to fully converge on directed graphs.

ASSERT ROW_COUNT = 10
ASSERT VALUE rank = 1 WHERE node_id = 33
ASSERT VALUE rank = 2 WHERE node_id = 0
USE {{zone_name}}.karate.karate_club
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 11. DEGREE CENTRALITY — Raw connection counts
-- ============================================================================
-- Graph is DIRECTED with bidirectional edges, so in_degree = out_degree.
-- NetworkX-verified top-5:
--   Node 33: in=17, out=17, total=34
--   Node  0: in=16, out=16, total=32
--   Node 32: in=12, out=12, total=24
--   Node  2: in=10, out=10, total=20
--   Node  1: in= 9, out= 9, total=18

ASSERT ROW_COUNT = 10
ASSERT VALUE total_degree = 34 WHERE node_id = 33
ASSERT VALUE total_degree = 32 WHERE node_id = 0
ASSERT VALUE total_degree = 24 WHERE node_id = 32
ASSERT VALUE in_degree = 17 WHERE node_id = 33
ASSERT VALUE in_degree = 16 WHERE node_id = 0
USE {{zone_name}}.karate.karate_club
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 12. BETWEENNESS CENTRALITY — Bridge nodes
-- ============================================================================
-- NetworkX-verified (normalized, Brandes algorithm):
--   Node 0 = 0.4376, Node 33 = 0.3041, Node 32 = 0.1452,
--   Node 2 = 0.1437, Node 31 = 0.1383
-- Node 0 has highest betweenness: it bridges many shortest paths.

ASSERT ROW_COUNT = 10
ASSERT VALUE rank = 1 WHERE node_id = 0
ASSERT VALUE rank = 2 WHERE node_id = 33
USE {{zone_name}}.karate.karate_club
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 10;


-- ============================================================================
-- 13. CLOSENESS CENTRALITY — How close is each member to all others?
-- ============================================================================
-- NetworkX-verified top-5:
--   Node 0 = 0.5690, Node 2 = 0.5593, Node 33 = 0.5500,
--   Node 31 = 0.5410, Node 8 = 0.5156

ASSERT ROW_COUNT = 10
ASSERT VALUE rank = 1 WHERE node_id = 0
ASSERT VALUE rank = 2 WHERE node_id = 2
USE {{zone_name}}.karate.karate_club
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 10;


-- ============================================================================
-- 14. COMMUNITY DETECTION — Can we recover the two factions?
-- ============================================================================
-- Ground truth: 2 factions (instructor vs president), but Louvain optimises
-- modularity and typically splits the network into 4-6 sub-communities.
-- Louvain is non-deterministic — community count varies by node ordering.
--
-- Deterministic invariants (independent of run):
--   • 3-6 communities (verified across NetworkX, igraph, Delta Forge)
--   • All 34 members assigned (sum of members = 34)

-- Non-deterministic: Louvain is stochastic — community count varies by node ordering
ASSERT WARNING ROW_COUNT >= 3
-- Non-deterministic: Louvain is stochastic — community count varies by node ordering
ASSERT WARNING ROW_COUNT <= 6
USE {{zone_name}}.karate.karate_club
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 15. CONNECTED COMPONENTS — Is the graph fully connected?
-- ============================================================================
-- Expected: 1 connected component (all 34 members reachable from any node).

ASSERT ROW_COUNT = 1
ASSERT VALUE members = 34
USE {{zone_name}}.karate.karate_club
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 16. SHORTEST PATH — Distance between the two faction leaders
-- ============================================================================
-- Nodes 0 and 33 are NOT directly connected (no direct friendship edge).
-- Shortest distance = 2 hops. Four equally valid paths exist:
--   0 -> 8 -> 33,  0 -> 13 -> 33,  0 -> 19 -> 33,  0 -> 31 -> 33
-- The intermediate node depends on graph traversal order.

-- 3 steps: source (distance=0), intermediate (distance=1), target (distance=2)
ASSERT ROW_COUNT = 3
ASSERT VALUE distance = 0 WHERE step = 0
ASSERT VALUE distance = 2 WHERE step = 2
ASSERT VALUE node_id = 0 WHERE step = 0
ASSERT VALUE node_id = 33 WHERE step = 2
USE {{zone_name}}.karate.karate_club
CALL algo.shortestPath({source: 0, target: 33})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 17. STRONGLY CONNECTED COMPONENTS — All nodes mutually reachable?
-- ============================================================================
-- Expected: 1 SCC containing all 34 nodes (NetworkX-verified).
-- Because edges are bidirectional, every node can reach every other node
-- following directed edges, making the entire graph one SCC.

ASSERT ROW_COUNT = 1
ASSERT VALUE members = 34
USE {{zone_name}}.karate.karate_club
CALL algo.scc()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 18. TRIANGLE COUNT — Clustering structure
-- ============================================================================
-- NetworkX-verified: 45 unique triangles total.
-- Top-5 by triangle participation:
--   Node 0 = 18, Node 33 = 15, Node 32 = 13, Node 1 = 12, Node 2 = 11
-- Each triangle counted once per participating node; divide total by 3
-- for unique triangle count.

ASSERT ROW_COUNT = 10
ASSERT VALUE triangle_count = 18 WHERE node_id = 0
ASSERT VALUE triangle_count = 15 WHERE node_id = 33
ASSERT VALUE triangle_count = 13 WHERE node_id = 32
USE {{zone_name}}.karate.karate_club
CALL algo.triangleCount()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC
LIMIT 10;


-- ============================================================================
-- 19. ALL SHORTEST PATHS FROM NODE 0 — Distance to every member
-- ============================================================================
-- NetworkX-verified: All 33 other nodes reachable, max distance = 3.0.
-- 16 nodes at distance 1, 9 at distance 2, 8 at distance 3.
-- Source node excluded from results (distance to self is trivial).

ASSERT ROW_COUNT = 33
ASSERT VALUE distance = 2.0 WHERE node_id = 33
USE {{zone_name}}.karate.karate_club
CALL algo.allShortestPaths({source: 0})
YIELD node_id, distance
RETURN node_id, distance
ORDER BY distance ASC, node_id ASC;


-- ============================================================================
-- 20. BFS TRAVERSAL FROM NODE 0 — Breadth-first layer structure
-- ============================================================================
-- NetworkX-verified: depth 0 = 1 node, depth 1 = 16 nodes,
-- depth 2 = 9 nodes, depth 3 = 8 nodes. Max depth = 3.

-- 4 depth levels (0, 1, 2, 3)
ASSERT ROW_COUNT = 4
ASSERT VALUE nodes_at_depth = 1 WHERE depth = 0
ASSERT VALUE nodes_at_depth = 16 WHERE depth = 1
ASSERT VALUE nodes_at_depth = 9 WHERE depth = 2
ASSERT VALUE nodes_at_depth = 8 WHERE depth = 3
USE {{zone_name}}.karate.karate_club
CALL algo.bfs({source: 0})
YIELD node_id, depth
RETURN depth, count(*) AS nodes_at_depth
ORDER BY depth;


-- ============================================================================
-- 21. DFS TRAVERSAL FROM NODE 0 — Depth-first discovery
-- ============================================================================
-- All 34 nodes discovered. Discovery/finish times are implementation-dependent
-- (vary with CSR neighbor ordering). Useful for verifying DFS traversal works.

ASSERT ROW_COUNT = 10
ASSERT VALUE discovery_time = 1 WHERE node_id = 0
USE {{zone_name}}.karate.karate_club
CALL algo.dfs({source: 0})
YIELD node_id, discovery_time, finish_time
RETURN node_id, discovery_time, finish_time
ORDER BY discovery_time
LIMIT 10;


-- ============================================================================
-- 22. MINIMUM SPANNING TREE — Lightest connecting tree
-- ============================================================================
-- NetworkX-verified: 33 edges (n-1), total weight = 33.0 (all weights = 1.0).
-- Any spanning tree is minimum since all edges have equal weight.

-- Non-deterministic: MST edge selection and ordering vary when all edge weights are equal
ASSERT WARNING ROW_COUNT = 10
-- Non-deterministic: edge direction depends on Kruskal processing order; node 0 appears as target
ASSERT WARNING VALUE weight = 1.0 WHERE targetId = 0
USE {{zone_name}}.karate.karate_club
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN sourceId, targetId, weight
ORDER BY sourceId, targetId
LIMIT 10;


-- ============================================================================
-- 23. KNN — 5 Nearest Neighbors of Node 0 (by Jaccard similarity)
-- ============================================================================
-- NetworkX-verified top-5 most similar to node 0:
--   Node 1 = 0.3889, Node 3 = 0.2941, Node 2 = 0.2381,
--   Node 7 = 0.1765, Node 13 = 0.1667

ASSERT ROW_COUNT = 5
ASSERT VALUE neighbor_id = 1 WHERE rank = 1
ASSERT VALUE neighbor_id = 3 WHERE rank = 2
ASSERT VALUE neighbor_id = 2 WHERE rank = 3
USE {{zone_name}}.karate.karate_club
CALL algo.knn({node: 0, k: 5})
YIELD neighbor_id, similarity, rank
RETURN neighbor_id, similarity, rank
ORDER BY rank;


-- ============================================================================
-- 24. SIMILARITY — Compare the two faction leaders
-- ============================================================================
-- NetworkX-verified similarity between nodes 0 and 33:
--   Jaccard = 0.1379 (4 common neighbors out of 29 union)
--   Common neighbors: [8, 13, 19, 31]
-- Despite leading rival factions, they share 4 mutual friends.

ASSERT ROW_COUNT = 1
-- Jaccard similarity: 4 common neighbors {8,13,19,31} out of 29 union = 4/29 ≈ 0.1379 (deterministic)
ASSERT VALUE score >= 0.13
ASSERT VALUE score <= 0.15
USE {{zone_name}}.karate.karate_club
CALL algo.similarity({node1: 0, node2: 33})
YIELD score
RETURN score;


-- ############################################################################
-- PART 4: VERIFICATION SUMMARY
-- ############################################################################


-- ============================================================================
-- 25. AUTOMATED VERIFICATION — PASS/FAIL against golden values
-- ============================================================================
-- All checks should return PASS. Any FAIL indicates data loading issues
-- or algorithm correctness problems.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 8
SELECT 'Vertex count = 34' AS test,
       CASE WHEN cnt = 34 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate.vertices)

UNION ALL
SELECT 'Edge row count = 156',
       CASE WHEN cnt = 156 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate.edges)

UNION ALL
SELECT 'No self-loops',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate.edges WHERE src = dst)

UNION ALL
SELECT 'All edge endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate.edges e
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.karate.vertices v WHERE v.vertex_id = e.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.karate.vertices v WHERE v.vertex_id = e.dst)
)

UNION ALL
SELECT 'Max degree >= 16 (faction leader)',
       CASE WHEN max_deg >= 16 THEN 'PASS' ELSE 'FAIL (got ' || CAST(max_deg AS VARCHAR) || ')' END
FROM (
    SELECT MAX(deg) AS max_deg FROM (
        SELECT src, COUNT(*) AS deg FROM {{zone_name}}.karate.edges GROUP BY src
    )
)

UNION ALL
SELECT 'Vertex ID range = 0–33',
       CASE WHEN min_id = 0 AND max_id = 33 THEN 'PASS'
            ELSE 'FAIL (range ' || CAST(min_id AS VARCHAR) || '–' || CAST(max_id AS VARCHAR) || ')' END
FROM (
    SELECT MIN(vertex_id) AS min_id, MAX(vertex_id) AS max_id FROM {{zone_name}}.karate.vertices
)

UNION ALL
SELECT 'All weights = 1.0 (unweighted)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' non-unit weights)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate.edges WHERE weight <> 1.0
)

UNION ALL
SELECT 'Symmetric edges (undirected)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' missing reverse edges)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate.edges e1
    WHERE NOT EXISTS (
        SELECT 1 FROM {{zone_name}}.karate.edges e2
        WHERE e2.src = e1.dst AND e2.dst = e1.src
    )
);
