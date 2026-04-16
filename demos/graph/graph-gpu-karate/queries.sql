-- ############################################################################
-- ############################################################################
--
--   GPU-ACCELERATED ZACHARY'S KARATE CLUB — ALGORITHM QUALITY VERIFICATION
--   34 Vertices / 78 Undirected Edges (156 rows) / Weight = 1.0
--
-- ############################################################################
-- ############################################################################
--
-- The gold standard for GPU algorithm correctness. Zachary's Karate Club
-- (1977) has decades of published reference values from NetworkX, igraph,
-- and SNAP. By forcing GPU execution on this small, well-understood graph,
-- we verify that GPU implementations produce the same results as CPU.
--
-- Every GPU algorithm uses ON GPU THRESHOLD 1 to force GPU execution
-- even on this small 34-node graph. Without the threshold override, the
-- engine would fall back to CPU since 34 nodes is below the default GPU
-- threshold. The MIN THRESHOLD 1 says "use GPU for any graph with >= 1 node."
--
-- PART 1: DATA INTEGRITY (queries 1–4)
-- PART 2: GPU ALGORITHMS — GOLDEN VALUE VERIFICATION (queries 5–15)
-- PART 3: GPU MATCH EXPANSION (queries 16–19)
-- PART 4: VERIFICATION SUMMARY (query 20)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY CHECKS
-- ############################################################################


-- ============================================================================
-- 1. VERTEX COUNT — 34 club members
-- ============================================================================

ASSERT VALUE row_count = 34
SELECT COUNT(*) AS row_count FROM {{zone_name}}.gpu_karate.vertices;


-- ============================================================================
-- 2. EDGE COUNT — 156 rows (78 undirected edges x 2)
-- ============================================================================

ASSERT VALUE row_count = 156
SELECT COUNT(*) AS row_count FROM {{zone_name}}.gpu_karate.edges;


-- ============================================================================
-- 3. REFERENTIAL INTEGRITY — All edges have valid endpoints
-- ============================================================================

ASSERT VALUE orphan_edges = 0
SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.gpu_karate.edges e
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.gpu_karate.vertices v WHERE v.vertex_id = e.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.gpu_karate.vertices v WHERE v.vertex_id = e.dst);


-- ============================================================================
-- 4. SELF-LOOP CHECK — No member should be friends with themselves
-- ============================================================================

ASSERT VALUE self_loops = 0
SELECT COUNT(*) AS self_loops
FROM {{zone_name}}.gpu_karate.edges
WHERE src = dst;


-- ############################################################################
-- PART 2: GPU ALGORITHMS — GOLDEN VALUE VERIFICATION
-- ############################################################################
-- Every algorithm uses ON GPU THRESHOLD 1 to force GPU execution.
-- Assertion values come from NetworkX reference implementations.


-- ============================================================================
-- 5. GPU PAGERANK — Influence ranking
-- ============================================================================
-- NetworkX-verified (damping=0.85, 20 iterations):
--   Rank 1: Node 33 (president), Rank 2: Node 0 (instructor)
-- GPU PageRank must produce identical ranking to CPU.

ASSERT ROW_COUNT = 10
ASSERT VALUE rank = 1 WHERE node_id = 33
ASSERT VALUE rank = 2 WHERE node_id = 0
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 6. GPU DEGREE CENTRALITY — Connection counts
-- ============================================================================
-- NetworkX-verified top-5:
--   Node 33: total=34, Node 0: total=32, Node 32: total=24,
--   Node 2: total=20, Node 1: total=18
-- Degree is deterministic from edge data — GPU must produce identical counts.

ASSERT ROW_COUNT = 10
ASSERT VALUE total_degree = 34 WHERE node_id = 33
ASSERT VALUE total_degree = 32 WHERE node_id = 0
ASSERT VALUE total_degree = 24 WHERE node_id = 32
ASSERT VALUE in_degree = 17 WHERE node_id = 33
ASSERT VALUE in_degree = 16 WHERE node_id = 0
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 7. GPU BETWEENNESS CENTRALITY — Bridge nodes
-- ============================================================================
-- NetworkX-verified: Node 0 has highest betweenness (bridges most paths).
-- Node 33 is rank 2. GPU betweenness must agree on the top-2 ranking.

ASSERT ROW_COUNT = 10
ASSERT VALUE rank = 1 WHERE node_id = 0
ASSERT VALUE rank = 2 WHERE node_id = 33
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 10;


-- ============================================================================
-- 8. GPU CONNECTED COMPONENTS — Single connected graph
-- ============================================================================
-- Expected: 1 component, 34 members. All nodes reachable.
-- GPU label propagation must find the same single component.

ASSERT ROW_COUNT = 1
ASSERT VALUE members = 34
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 9. GPU LOUVAIN — Community detection
-- ============================================================================
-- Ground truth: 2 factions, but Louvain typically finds 3-6 sub-communities.
-- Non-deterministic. Invariants: 3-6 communities, all 34 members assigned.

-- Non-deterministic: Louvain is stochastic — community count varies by node ordering and seed
ASSERT WARNING ROW_COUNT >= 3
ASSERT WARNING ROW_COUNT <= 6
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 10. GPU TRIANGLE COUNT — Clustering structure
-- ============================================================================
-- NetworkX-verified: 45 unique triangles total.
-- Top-3: Node 0 = 18, Node 33 = 15, Node 32 = 13.
-- GPU triangle counting must produce identical per-node counts.

ASSERT ROW_COUNT = 10
ASSERT VALUE triangle_count = 18 WHERE node_id = 0
ASSERT VALUE triangle_count = 15 WHERE node_id = 33
ASSERT VALUE triangle_count = 13 WHERE node_id = 32
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.triangleCount()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC
LIMIT 10;


-- ============================================================================
-- 11. GPU SCC — Strongly connected components
-- ============================================================================
-- Expected: 1 SCC with all 34 nodes (bidirectional edges).

ASSERT ROW_COUNT = 1
ASSERT VALUE members = 34
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.scc()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 12. GPU PAGERANK — Lower damping factor comparison
-- ============================================================================
-- Damping 0.50 (vs 0.85 standard) makes PageRank more uniform.
-- NetworkX-verified at damping=0.50: Node 33 = rank 1, Node 0 = rank 2.

ASSERT ROW_COUNT = 10
ASSERT VALUE rank = 1 WHERE node_id = 33
ASSERT VALUE rank = 2 WHERE node_id = 0
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.pageRank({dampingFactor: 0.50, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 13. GPU LOUVAIN — Higher resolution for finer communities
-- ============================================================================
-- Resolution 2.0 should produce more, smaller communities than 1.0.
-- All 34 members must still be assigned.

-- Non-deterministic: Louvain is stochastic — community count varies by node ordering
ASSERT WARNING ROW_COUNT >= 3
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.louvain({resolution: 2.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 14. CLOSENESS CENTRALITY — Proximity ranking
-- ============================================================================
-- NetworkX-verified: Node 0 = rank 1 (closeness=0.5690), Node 2 = rank 2 (0.5593).

ASSERT ROW_COUNT = 10
ASSERT VALUE rank = 1 WHERE node_id = 0
ASSERT VALUE rank = 2 WHERE node_id = 2
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 10;


-- ============================================================================
-- 15. SHORTEST PATH — Distance between faction leaders
-- ============================================================================
-- Nodes 0 and 33 are NOT directly connected. Distance = 2 hops via node 8.
-- NetworkX-verified path: [0, 8, 33].

ASSERT ROW_COUNT = 3
ASSERT VALUE distance = 0 WHERE step = 0
ASSERT VALUE distance = 1 WHERE step = 1
ASSERT VALUE distance = 2 WHERE step = 2
ASSERT VALUE node_id = 0 WHERE step = 0
-- Non-deterministic intermediate: 4 equal-length paths exist (via 8, 13, 19, or 31)
ASSERT WARNING VALUE node_id IN (8, 13, 19, 31) WHERE step = 1
ASSERT VALUE node_id = 33 WHERE step = 2
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
CALL algo.shortestPath({source: 0, target: 33})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ############################################################################
-- PART 3: GPU MATCH EXPANSION
-- ############################################################################
-- GPU MATCH on the small Karate graph verifies correct edge traversal
-- even when the graph fits entirely in a single GPU thread block.


-- ============================================================================
-- 16. GPU MATCH — Full graph edge scan
-- ============================================================================
-- All 156 directed edges via GPU expansion.

ASSERT ROW_COUNT = 156
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
MATCH (a)-[r]->(b)
RETURN a.id AS src, b.id AS dst, r.edge_type AS type, r.weight AS weight
ORDER BY src, dst;


-- ============================================================================
-- 17. GPU MATCH — Instructor's neighborhood
-- ============================================================================
-- Node 0 has 16 direct friends. GPU expansion must find all 16.

ASSERT ROW_COUNT = 16
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
MATCH (a)-[r]->(b)
WHERE a.id = 0
RETURN b.id AS friend_id, b.name AS friend_name
ORDER BY friend_id;


-- ============================================================================
-- 18. GPU MATCH — Edge type distribution
-- ============================================================================
-- 5 distinct edge types via GPU-expanded edges.

ASSERT ROW_COUNT = 5
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
MATCH (a)-[r]->(b)
RETURN r.edge_type AS type, count(r) AS count
ORDER BY count DESC;


-- ============================================================================
-- 19. GPU MATCH — Degree distribution via GPU
-- ============================================================================
-- GPU expansion aggregated by source node. Top degrees must match
-- NetworkX: Node 33 = 17, Node 0 = 16.

ASSERT ROW_COUNT = 34
ASSERT VALUE degree = 17 WHERE member_id = 33
ASSERT VALUE degree = 16 WHERE member_id = 0
ASSERT VALUE degree = 12 WHERE member_id = 32
USE {{zone_name}}.gpu_karate.gpu_karate
ON GPU THRESHOLD 1
MATCH (a)-[r]->(b)
RETURN a.id AS member_id, a.name AS name, COUNT(r) AS degree
ORDER BY degree DESC, member_id ASC;


-- ############################################################################
-- PART 4: VERIFICATION SUMMARY
-- ############################################################################


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- 20. AUTOMATED VERIFICATION — GPU quality checks
-- Cross-cutting sanity check: vertex/edge counts, self-loop freedom, orphan
-- freedom, max-degree invariant, vertex ID range, unit weights, symmetry, and
-- edge-type diversity. If all 9 rows report PASS, the GPU data substrate is
-- sound and the algorithm results above are trustworthy.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 9
SELECT 'Vertex count = 34' AS test,
       CASE WHEN cnt = 34 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.gpu_karate.vertices)

UNION ALL
SELECT 'Edge row count = 156',
       CASE WHEN cnt = 156 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.gpu_karate.edges)

UNION ALL
SELECT 'No self-loops',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.gpu_karate.edges WHERE src = dst)

UNION ALL
SELECT 'All edge endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.gpu_karate.edges e
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.gpu_karate.vertices v WHERE v.vertex_id = e.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.gpu_karate.vertices v WHERE v.vertex_id = e.dst)
)

UNION ALL
SELECT 'Max degree >= 16 (faction leader)',
       CASE WHEN max_deg >= 16 THEN 'PASS' ELSE 'FAIL (got ' || CAST(max_deg AS VARCHAR) || ')' END
FROM (
    SELECT MAX(deg) AS max_deg FROM (
        SELECT src, COUNT(*) AS deg FROM {{zone_name}}.gpu_karate.edges GROUP BY src
    )
)

UNION ALL
SELECT 'Vertex ID range = 0-33',
       CASE WHEN min_id = 0 AND max_id = 33 THEN 'PASS'
            ELSE 'FAIL (range ' || CAST(min_id AS VARCHAR) || '-' || CAST(max_id AS VARCHAR) || ')' END
FROM (
    SELECT MIN(vertex_id) AS min_id, MAX(vertex_id) AS max_id FROM {{zone_name}}.gpu_karate.vertices
)

UNION ALL
SELECT 'All weights = 1.0 (unweighted)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' non-unit weights)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.gpu_karate.edges WHERE weight <> 1.0
)

UNION ALL
SELECT 'Symmetric edges (undirected)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' missing reverse edges)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.gpu_karate.edges e1
    WHERE NOT EXISTS (
        SELECT 1 FROM {{zone_name}}.gpu_karate.edges e2
        WHERE e2.src = e1.dst AND e2.dst = e1.src
    )
)

UNION ALL
SELECT '5 edge types',
       CASE WHEN cnt = 5 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (
    SELECT COUNT(DISTINCT edge_type) AS cnt FROM {{zone_name}}.gpu_karate.edges
);
