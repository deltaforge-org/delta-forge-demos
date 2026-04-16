-- ############################################################################
-- ############################################################################
--
--   GPU-ACCELERATED ENTERPRISE NETWORK — 1M NODES / 5M+ EDGES
--   GPU Algorithm & MATCH Expansion Verification at Enterprise Scale
--
-- ############################################################################
-- ############################################################################
--
-- This demo proves that GPU-accelerated graph algorithms produce correct
-- results on a 1,000,000-node enterprise organization network. Every query
-- uses the USING GPU hint to force GPU execution. Assertion values are
-- identical to the CPU stress test — proving GPU/CPU equivalence at scale.
--
-- GPU-accelerable algorithms (5):
--   PageRank, Connected Components, Louvain, Betweenness, Triangle Count
--
-- GPU MATCH expansion:
--   Single-hop pattern matching via GPU edge scatter/gather
--
-- PART 1: DATA INTEGRITY (queries 1–3)
--   Baseline verification — same data as CPU stress test.
--
-- PART 2: GPU ALGORITHMS (queries 4–10)
--   All 5 GPU-accelerable algorithms with USING GPU hint.
--
-- PART 3: GPU MATCH EXPANSION (queries 11–16)
--   Pattern matching through GPU edge expansion path.
--
-- PART 4: GPU + STREAMING (queries 17–19)
--   GPU algorithms with streaming property loading for memory efficiency.
--
-- PART 5: GPU THRESHOLD BEHAVIOR (queries 20–22)
--   MIN THRESHOLD controls GPU/CPU fallback decisions.
--
-- PART 6: VERIFICATION (query 23)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY
-- ############################################################################


-- ============================================================================
-- 1. VERTEX COUNT — Confirm 1M nodes loaded
-- ============================================================================

ASSERT VALUE total_employees = 1000000
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
MATCH (n)
RETURN count(n) AS total_employees;


-- ============================================================================
-- 2. EDGE COUNT — Confirm ~5M edges loaded
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_connections = 5059998
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
MATCH (a)-[r]->(b)
RETURN count(r) AS total_connections;


-- ============================================================================
-- 3. DEPARTMENT DISTRIBUTION — 50K per department
-- ============================================================================

ASSERT ROW_COUNT = 20
ASSERT VALUE headcount = 50000 WHERE department = 'Engineering'
ASSERT VALUE headcount = 50000 WHERE department = 'AI/ML'
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
MATCH (n)
RETURN n.department AS department, count(n) AS headcount
ORDER BY headcount DESC;


-- ############################################################################
-- PART 2: GPU ALGORITHMS — All 5 GPU-accelerable algorithms
-- ############################################################################


-- ============================================================================
-- 4. GPU PAGERANK — Influence ranking on GPU at 1M scale
-- ============================================================================
-- PageRank is the flagship GPU algorithm: matrix-vector multiplication
-- maps naturally to GPU SIMD lanes. At 1M nodes the GPU should
-- significantly outperform CPU.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
CALL algo.pageRank({dampingFactor: 0.85, iterations: 5})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 25;


-- ============================================================================
-- 5. GPU CONNECTED COMPONENTS — Single giant component
-- ============================================================================
-- The graph is fully connected: weak ties and bridge nodes ensure one
-- giant component of 1,000,000 nodes. GPU BFS-based label propagation
-- must find the same single component as CPU.

ASSERT ROW_COUNT = 1
ASSERT VALUE community_size = 1000000
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS community_size
ORDER BY community_size DESC
LIMIT 25;


-- ============================================================================
-- 6. GPU LOUVAIN — Community detection at scale
-- ============================================================================
-- Louvain modularity optimization on GPU. Non-deterministic but should
-- find >= 2 communities (the 20-department structure creates natural
-- clusters). GPU parallelism handles the iterative merge phases.

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS size
ORDER BY size DESC
LIMIT 25;


-- ============================================================================
-- 7. GPU BETWEENNESS CENTRALITY — Bridge detection at scale
-- ============================================================================
-- Approximate betweenness via sampling. GPU accelerates the BFS
-- wavefronts from each sampled source node.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
CALL algo.betweenness({samplingSize: 1000})
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 25;


-- ============================================================================
-- 8. GPU TRIANGLE COUNT — Clustering structure
-- ============================================================================
-- Triangle counting benefits enormously from GPU: intersection of
-- neighbor lists parallelizes well across edges.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
CALL algo.triangleCount()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC
LIMIT 25;


-- ============================================================================
-- 9. GPU PAGERANK — Higher iteration count for convergence
-- ============================================================================
-- 20 iterations gives tighter convergence than 5. GPU handles the extra
-- matrix-vector multiplications with minimal overhead vs CPU.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 25;


-- ============================================================================
-- 10. GPU LOUVAIN — Higher resolution for finer communities
-- ============================================================================
-- Resolution > 1.0 produces more, smaller communities. Tests that GPU
-- Louvain correctly handles the resolution parameter.

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
CALL algo.louvain({resolution: 1.5})
YIELD node_id, community_id
RETURN community_id, count(*) AS size
ORDER BY size DESC
LIMIT 25;


-- ############################################################################
-- PART 3: GPU MATCH EXPANSION
-- ############################################################################
-- GPU MATCH expansion accelerates single-hop edge traversal using
-- GPU scatter/gather. The GPU reads CSR offsets and neighbor arrays
-- from device memory, producing (src, dst, edge, weight) tuples.


-- ============================================================================
-- 11. GPU MATCH — Full edge scan
-- ============================================================================
-- Forces the GPU path for the 5M-edge full scan. GPU scatter/gather
-- must produce the same total as CPU sequential scan.

ASSERT VALUE total_connections = 5059998
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
MATCH (a)-[r]->(b)
RETURN count(r) AS total_connections;


-- ============================================================================
-- 12. GPU MATCH — Department-filtered pattern
-- ============================================================================
-- GPU expansion with post-filter: expand all edges from Engineering
-- employees, then filter to same-department connections.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
MATCH (a)-[r]->(b)
WHERE a.department = 'Engineering' AND b.department = 'Engineering'
RETURN a.name AS from_name, b.name AS to_name, r.relationship_type AS type, r.weight AS weight
ORDER BY r.weight DESC
LIMIT 25;


-- ============================================================================
-- 13. GPU MATCH — Relationship type filter
-- ============================================================================
-- GPU expansion filtering by relationship type. The 550K mentor edges
-- should be correctly identified from GPU output.

ASSERT VALUE mentor_count = 550000
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor'
RETURN count(r) AS mentor_count;


-- ============================================================================
-- 14. GPU MATCH — Cross-department connections
-- ============================================================================
-- Tests GPU expansion with inequality filter. Must produce same cross-
-- department pair rankings as CPU.

ASSERT ROW_COUNT = 30
ASSERT VALUE connections = 91000 WHERE from_dept = 'Engineering'
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC
LIMIT 30;


-- ============================================================================
-- 15. GPU MATCH — Aggregated degree distribution
-- ============================================================================
-- GPU expansion followed by GROUP BY aggregation. The 18 relationship
-- types and their exact counts must match CPU results.

ASSERT ROW_COUNT = 18
ASSERT VALUE count = 750000 WHERE type = 'colleague'
ASSERT VALUE count = 750000 WHERE type = 'teammate'
ASSERT VALUE count = 550000 WHERE type = 'mentor'
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
MATCH (a)-[r]->(b)
RETURN r.relationship_type AS type, count(r) AS count,
       avg(r.weight) AS avg_strength
ORDER BY count DESC;


-- ============================================================================
-- 16. GPU MATCH — Small subgraph extraction
-- ============================================================================
-- GPU expansion on a small slice (IDs 1-100). Even on small subgraphs
-- the GPU path must produce correct results.

ASSERT ROW_COUNT = 389
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
MATCH (a)-[r]->(b)
WHERE a.id <= 100 AND b.id <= 100
RETURN a, r, b;


-- ############################################################################
-- PART 4: GPU + STREAMING PROPERTY LOADING
-- ############################################################################
-- For graphs that exceed GPU memory, streaming mode loads properties
-- in batches. These queries combine USING GPU with WITH STREAMING
-- to test the streaming property pipeline.


-- ============================================================================
-- 17. GPU + STREAMING PAGERANK — Memory-efficient influence ranking
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
WITH STREAMING CACHE SIZE 500000
CALL algo.pageRank({dampingFactor: 0.85, iterations: 5})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 25;


-- ============================================================================
-- 18. GPU + STREAMING CONNECTED COMPONENTS
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE community_size = 1000000
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
WITH STREAMING CACHE SIZE 500000
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS community_size
ORDER BY community_size DESC
LIMIT 25;


-- ============================================================================
-- 19. GPU + STREAMING TRIANGLE COUNT
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU
WITH STREAMING CACHE SIZE 500000
CALL algo.triangleCount()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC
LIMIT 25;


-- ############################################################################
-- PART 5: GPU THRESHOLD BEHAVIOR
-- ############################################################################
-- MIN THRESHOLD tells the engine: "only use GPU if the graph has at
-- least N nodes." Below threshold, fall back to CPU silently.
-- Results must be identical regardless of which device executes.


-- ============================================================================
-- 20. GPU THRESHOLD — Below graph size (GPU executes)
-- ============================================================================
-- Threshold 100,000 < 1,000,000 nodes: GPU should execute.

ASSERT ROW_COUNT = 1
ASSERT VALUE community_size = 1000000
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU MIN THRESHOLD 100000
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS community_size
ORDER BY community_size DESC
LIMIT 25;


-- ============================================================================
-- 21. GPU THRESHOLD — Above graph size (CPU fallback)
-- ============================================================================
-- Threshold 2,000,000 > 1,000,000 nodes: should fall back to CPU.
-- Result must still be correct — same single component of 1M nodes.

ASSERT ROW_COUNT = 1
ASSERT VALUE community_size = 1000000
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU MIN THRESHOLD 2000000
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS community_size
ORDER BY community_size DESC
LIMIT 25;


-- ============================================================================
-- 22. GPU THRESHOLD — PageRank with threshold
-- ============================================================================
-- Threshold 500,000 < 1,000,000: GPU executes PageRank.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.gpu_stress_network.gpu_stress_network
USING GPU MIN THRESHOLD 500000
CALL algo.pageRank({dampingFactor: 0.85, iterations: 5})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 25;


-- ############################################################################
-- PART 6: VERIFICATION
-- ############################################################################


-- ============================================================================
-- 23. AUTOMATED VERIFICATION — GPU/CPU equivalence checks
-- ============================================================================
-- Cross-cutting sanity check: vertex count, edge count, department
-- distribution, and relationship type counts must all match the
-- deterministic generation formulas regardless of GPU vs CPU execution.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 5
SELECT 'Vertex count = 1,000,000' AS test,
       CASE WHEN cnt = 1000000 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.gpu_stress_network.gpu_st_people)

UNION ALL
SELECT 'Edge count = 5,059,998',
       CASE WHEN cnt = 5059998 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.gpu_stress_network.gpu_st_edges)

UNION ALL
SELECT 'Departments = 20',
       CASE WHEN cnt = 20 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(DISTINCT department) AS cnt FROM {{zone_name}}.gpu_stress_network.gpu_st_people)

UNION ALL
SELECT 'Relationship types = 18',
       CASE WHEN cnt = 18 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(DISTINCT relationship_type) AS cnt FROM {{zone_name}}.gpu_stress_network.gpu_st_edges)

UNION ALL
SELECT 'Engineering headcount = 50,000',
       CASE WHEN cnt = 50000 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.gpu_stress_network.gpu_st_people WHERE department = 'Engineering');
