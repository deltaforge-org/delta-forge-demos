-- ############################################################################
-- ############################################################################
--
--   DOLPHINS SOCIAL NETWORK — COMMUNITY STRUCTURE IN THE WILD
--   62 Vertices / 159 Undirected Edges (318 rows) / Weight = 1.0
--
-- ############################################################################
-- ############################################################################
--
-- A well-studied animal social network (Lusseau et al., 2003). 62 bottlenose
-- dolphins in Doubtful Sound, New Zealand, with associations recorded over
-- several years. The network naturally splits into 2–4 communities, making it
-- a popular benchmark for community detection algorithms.
--
-- PART 1: DATA INTEGRITY CHECKS (queries 1–4)
-- PART 2: CYPHER — GRAPH EXPLORATION (queries 5–9)
-- PART 3: CYPHER — GRAPH ALGORITHMS (queries 10–16)
-- PART 4: VERIFICATION SUMMARY (query 17)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY CHECKS
-- ############################################################################


-- ============================================================================
-- 1. VERTEX & EDGE COUNTS — Verify data loaded correctly
-- ============================================================================
-- 62 vertices, 318 edge rows (159 undirected edges x 2)

-- Verify vertex count
ASSERT VALUE row_count = 62
SELECT COUNT(*) AS row_count FROM {{zone_name}}.dolphins.vertices;

-- Verify edge count (159 undirected edges x 2)
ASSERT VALUE row_count = 318
SELECT COUNT(*) AS row_count FROM {{zone_name}}.dolphins.edges;


-- ============================================================================
-- 2. GRAPH CONFIG — Verify graph definition
-- ============================================================================

ASSERT ROW_COUNT >= 1
SHOW GRAPH;


-- ============================================================================
-- 3. REFERENTIAL INTEGRITY — All edges have valid endpoints
-- ============================================================================

ASSERT VALUE orphan_edges = 0
SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.dolphins.edges e
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.dolphins.vertices v WHERE v.vertex_id = e.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.dolphins.vertices v WHERE v.vertex_id = e.dst);


-- ============================================================================
-- 4. SELF-LOOP CHECK — No dolphin should be associated with itself
-- ============================================================================

ASSERT VALUE self_loops = 0
SELECT COUNT(*) AS self_loops
FROM {{zone_name}}.dolphins.edges
WHERE src = dst;


-- ############################################################################
-- PART 2: CYPHER — GRAPH EXPLORATION
-- ############################################################################


-- ============================================================================
-- 5. BROWSE VERTICES — Verify the graph exposes all 62 dolphins (IDs 0–61)
-- ============================================================================

ASSERT VALUE min_id = 0
ASSERT VALUE max_id = 61
ASSERT VALUE total = 62
USE {{zone_name}}.dolphins.dolphins_social
MATCH (v)
RETURN MIN(v.id) AS min_id, MAX(v.id) AS max_id, COUNT(v) AS total;


-- ============================================================================
-- 6. DEGREE DISTRIBUTION — How many associations does each dolphin have?
-- ============================================================================
-- All 62 dolphins appear in at least one edge. Max degree = 12 (node 14),
-- min degree = 1 (9 leaf nodes). 318 total edge rows / 62 nodes = avg ~5.13.

ASSERT VALUE degree = 12 WHERE dolphin_id = 14
ASSERT VALUE degree = 11 WHERE dolphin_id = 37
ASSERT VALUE degree = 11 WHERE dolphin_id = 45
ASSERT VALUE degree = 6 WHERE dolphin_id = 0
ASSERT VALUE degree = 1 WHERE dolphin_id = 4
USE {{zone_name}}.dolphins.dolphins_social
MATCH (a)-[r]->(b)
RETURN a.id AS dolphin_id, COUNT(r) AS degree
ORDER BY degree DESC, dolphin_id ASC;


-- ============================================================================
-- 7. TOP HUBS — The 5 most connected dolphins
-- ============================================================================
-- Node 14 (12), 37 (11), 45 (11), 33 (10), 51 (10).

ASSERT VALUE degree = 12 WHERE dolphin_id = 14
ASSERT VALUE degree = 11 WHERE dolphin_id = 37
ASSERT VALUE degree = 11 WHERE dolphin_id = 45
ASSERT VALUE degree = 10 WHERE dolphin_id = 33
ASSERT VALUE degree = 10 WHERE dolphin_id = 51
USE {{zone_name}}.dolphins.dolphins_social
MATCH (a)-[r]->(b)
RETURN a.id AS dolphin_id, COUNT(r) AS degree
ORDER BY degree DESC, dolphin_id ASC
LIMIT 5;


-- ============================================================================
-- 8. NEIGHBORHOOD OF TOP HUB — Dolphin 14's 12 direct associates
-- ============================================================================
-- Node 14 connects to: 0, 3, 16, 24, 33, 34, 37, 38, 40, 43, 50, 52.

ASSERT RESULT SET INCLUDES (14, 0), (14, 3), (14, 16), (14, 24), (14, 33), (14, 34), (14, 37), (14, 38), (14, 40), (14, 43), (14, 50), (14, 52)
USE {{zone_name}}.dolphins.dolphins_social
MATCH (a)-[]->(c)
WHERE a.id = 14
RETURN a.id AS hub_id, c.id AS associate_id
ORDER BY associate_id;


-- ============================================================================
-- 9. TWO-HOP REACHABILITY FROM NODE 0 — How far does association reach?
-- ============================================================================
-- Node 0 has 6 direct neighbors. Through them, 27 distinct nodes are
-- reachable in 1–2 hops (44% of the graph) — excludes node 0 itself.

ASSERT VALUE reachable_in_2_hops = 27
USE {{zone_name}}.dolphins.dolphins_social
MATCH (a)-[*1..2]->(b)
WHERE a.id = 0
RETURN COUNT(DISTINCT b.id) AS reachable_in_2_hops;


-- ############################################################################
-- PART 3: CYPHER — GRAPH ALGORITHMS
-- ############################################################################


-- ============================================================================
-- 10. PAGERANK — Identify most influential dolphins
-- ============================================================================
-- PageRank scores are non-deterministic (floating-point iteration order).
-- The top node should be one of the high-degree hubs (14, 37, 45).

-- Non-deterministic: PageRank scores vary with floating-point iteration order
ASSERT WARNING VALUE rank <= 3 WHERE node_id = 14
ASSERT WARNING VALUE rank <= 3 WHERE node_id = 17
ASSERT WARNING VALUE score >= 0.02 WHERE node_id = 14
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 11. DEGREE CENTRALITY — Normalized degree
-- ============================================================================
-- Deterministic: derived directly from edge counts.
-- Node 14 should have total_degree = 12 (in=12, out=12 for undirected).

ASSERT VALUE in_degree = 12 WHERE node_id = 14
ASSERT VALUE out_degree = 12 WHERE node_id = 14
ASSERT VALUE total_degree = 24 WHERE node_id = 14
ASSERT VALUE total_degree = 22 WHERE node_id = 37
ASSERT VALUE total_degree = 22 WHERE node_id = 45
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 12. BETWEENNESS CENTRALITY — Bridge nodes
-- ============================================================================
-- Dolphins that bridge sub-communities will have the highest betweenness.
-- Betweenness is deterministic for a fixed graph topology.

-- Betweenness (normalized): node 36 is the top bridge (~0.248), node 1 second (~0.213).
-- These two bridge the main sub-communities despite not having the highest degree.
ASSERT VALUE rank = 1 WHERE node_id = 36
ASSERT VALUE rank = 2 WHERE node_id = 1
ASSERT VALUE centrality >= 0.24 WHERE node_id = 36
ASSERT VALUE centrality >= 0.20 WHERE node_id = 1
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 10;


-- ============================================================================
-- 13. CLOSENESS CENTRALITY — How close is each dolphin to all others?
-- ============================================================================
-- Closeness is deterministic for a fixed, connected graph.

-- Closeness: node 36 is closest to all others (~0.418), then node 40 (~0.404).
-- High closeness means short average paths to every other dolphin.
ASSERT VALUE rank = 1 WHERE node_id = 36
ASSERT VALUE rank = 2 WHERE node_id = 40
ASSERT VALUE closeness >= 0.40 WHERE node_id = 36
ASSERT VALUE closeness >= 0.39 WHERE node_id = 40
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 10;


-- ============================================================================
-- 14. COMMUNITY DETECTION — Can we recover the natural groups?
-- ============================================================================
-- Published results show 2-4 communities. Louvain is non-deterministic
-- (tie-breaking varies), but the number of communities is stable.

-- Non-deterministic: Louvain community assignment depends on iteration order;
-- but total members across all communities must always equal 62.
ASSERT WARNING EXPRESSION SUM(members) = 62
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 15. CONNECTED COMPONENTS — Is the graph fully connected?
-- ============================================================================
-- All 62 dolphins form a single connected component.

ASSERT VALUE members = 62
ASSERT VALUE component_id = 0
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 16. SHORTEST PATH — Distance between dolphins 0 and 61
-- ============================================================================
-- BFS: 0 → 10 → 2 → 61 (3 hops). Verify start, end, and hop count.
ASSERT VALUE distance = 0 WHERE node_id = 0
ASSERT VALUE step = 0 WHERE node_id = 0
ASSERT VALUE distance = 3 WHERE node_id = 61
ASSERT VALUE step = 3 WHERE node_id = 61
USE {{zone_name}}.dolphins.dolphins_social
CALL algo.shortestPath({source: 0, target: 61})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ############################################################################
-- PART 4: VERIFICATION SUMMARY
-- ############################################################################


-- ============================================================================
-- 17. AUTOMATED VERIFICATION — PASS/FAIL against golden values
-- ============================================================================
-- All checks should return PASS. Any FAIL indicates data loading issues
-- or algorithm correctness problems.

ASSERT NO_FAIL IN result
SELECT 'Vertex count = 62' AS test,
       CASE WHEN cnt = 62 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.vertices)

UNION ALL
SELECT 'Edge row count = 318',
       CASE WHEN cnt = 318 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges)

UNION ALL
SELECT 'No self-loops',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges WHERE src = dst)

UNION ALL
SELECT 'All edge endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges e
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.dolphins.vertices v WHERE v.vertex_id = e.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.dolphins.vertices v WHERE v.vertex_id = e.dst)
)

UNION ALL
SELECT 'Max degree = 12 (node 14)',
       CASE WHEN max_deg = 12 THEN 'PASS' ELSE 'FAIL (got ' || CAST(max_deg AS VARCHAR) || ')' END
FROM (
    SELECT MAX(deg) AS max_deg FROM (
        SELECT src, COUNT(*) AS deg FROM {{zone_name}}.dolphins.edges GROUP BY src
    )
)

UNION ALL
SELECT 'Vertex ID range = 0–61',
       CASE WHEN min_id = 0 AND max_id = 61 THEN 'PASS'
            ELSE 'FAIL (range ' || CAST(min_id AS VARCHAR) || '–' || CAST(max_id AS VARCHAR) || ')' END
FROM (
    SELECT MIN(vertex_id) AS min_id, MAX(vertex_id) AS max_id FROM {{zone_name}}.dolphins.vertices
)

UNION ALL
SELECT 'All weights = 1.0 (unweighted)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' non-unit weights)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges WHERE weight <> 1.0
)

UNION ALL
SELECT 'Symmetric edges (undirected)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' missing reverse edges)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.dolphins.edges e1
    WHERE NOT EXISTS (
        SELECT 1 FROM {{zone_name}}.dolphins.edges e2
        WHERE e2.src = e1.dst AND e2.dst = e1.src
    )
);


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total counts, hub identity, and edge symmetry.

ASSERT VALUE vertex_count = 62
ASSERT VALUE edge_count = 318
ASSERT VALUE max_degree = 12
ASSERT VALUE top_hub_id = 14
ASSERT VALUE missing_reverse = 0
ASSERT VALUE hub_14_neighbors = 12
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.dolphins.vertices) AS vertex_count,
    (SELECT COUNT(*) FROM {{zone_name}}.dolphins.edges) AS edge_count,
    (SELECT MAX(deg) FROM (SELECT COUNT(*) AS deg FROM {{zone_name}}.dolphins.edges GROUP BY src)) AS max_degree,
    (SELECT src FROM (SELECT src, COUNT(*) AS deg FROM {{zone_name}}.dolphins.edges GROUP BY src ORDER BY deg DESC LIMIT 1)) AS top_hub_id,
    (SELECT COUNT(*) FROM {{zone_name}}.dolphins.edges e1 WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.dolphins.edges e2 WHERE e2.src = e1.dst AND e2.dst = e1.src)) AS missing_reverse,
    (SELECT COUNT(*) FROM {{zone_name}}.dolphins.edges WHERE src = 14) AS hub_14_neighbors;
