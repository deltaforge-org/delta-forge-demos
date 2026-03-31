-- ############################################################################
-- ############################################################################
--
--   MANUAL CSR CACHE MANAGEMENT — GRAPH PERFORMANCE CONTROL
--   34 Vertices / 78 Undirected Edges (156 rows) / Weight = 1.0
--
-- ############################################################################
-- ############################################################################
--
-- Demonstrates NO AUTO CACHE CSR: automatic disk caching is disabled, so
-- every Cypher query rebuilds the CSR from Delta tables (slow path) until
-- the operator manually runs CREATE GRAPHCSR (fast path).
--
-- PART 1: DATA INTEGRITY CHECKS (queries 1-3)
-- PART 2: CYPHER WITHOUT CSR CACHE (queries 4-8) — slow path, rebuild each time
-- PART 3: MANUAL CSR BUILD (query 9) — CREATE GRAPHCSR
-- PART 4: CYPHER WITH CSR CACHE (queries 10-14) — fast path, identical results
-- PART 5: VERIFICATION SUMMARY (query 15)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY CHECKS
-- ############################################################################


-- ============================================================================
-- 1. VERTEX & EDGE COUNTS — Verify data loaded correctly
-- ============================================================================

ASSERT VALUE row_count = 34
SELECT COUNT(*) AS row_count FROM {{zone_name}}.karate_manual.vertices;

ASSERT VALUE row_count = 156
SELECT COUNT(*) AS row_count FROM {{zone_name}}.karate_manual.edges;


-- ============================================================================
-- 2. GRAPH CONFIG — Verify graph definition exists
-- ============================================================================
-- The graph should be created with NO AUTO CACHE CSR.

SHOW GRAPH;


-- ============================================================================
-- 3. REFERENTIAL INTEGRITY — All edges have valid endpoints
-- ============================================================================

ASSERT VALUE orphan_edges = 0
SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.karate_manual.edges e
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.karate_manual.vertices v WHERE v.vertex_id = e.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.karate_manual.vertices v WHERE v.vertex_id = e.dst);


-- ############################################################################
-- PART 2: CYPHER WITHOUT CSR CACHE (SLOW PATH)
-- ############################################################################
-- These queries rebuild the CSR from Delta tables on every execution because
-- NO AUTO CACHE CSR prevents automatic .dcsr file creation. This is the
-- expected behavior for high-frequency write workloads where operators
-- control when the cache is refreshed.


-- ============================================================================
-- 4. BROWSE VERTICES — All 34 club members (no cache)
-- ============================================================================

ASSERT ROW_COUNT = 34
USE {{zone_name}}.karate_manual.karate_manual
MATCH (v)
RETURN v.id AS member_id, v.name AS name, v.role AS role
ORDER BY member_id;


-- ============================================================================
-- 5. TOP HUBS — Degree distribution without cache
-- ============================================================================
-- NetworkX-verified top-5: 33(17), 0(16), 32(12), 2(10), 1(9).

ASSERT ROW_COUNT = 5
ASSERT VALUE degree = 17 WHERE member_id = 33
ASSERT VALUE degree = 16 WHERE member_id = 0
ASSERT VALUE degree = 12 WHERE member_id = 32
ASSERT VALUE degree = 10 WHERE member_id = 2
ASSERT VALUE degree = 9 WHERE member_id = 1
USE {{zone_name}}.karate_manual.karate_manual
MATCH (a)-[r]->(b)
RETURN a.id AS member_id, a.name AS name, COUNT(r) AS degree
ORDER BY degree DESC
LIMIT 5;


-- ============================================================================
-- 6. PAGERANK — Most influential members (no cache)
-- ============================================================================
-- NetworkX-verified (damping=0.85): Node 33 rank 1, Node 0 rank 2.

ASSERT ROW_COUNT = 10
ASSERT VALUE rank = 1 WHERE node_id = 33
ASSERT VALUE rank = 2 WHERE node_id = 0
USE {{zone_name}}.karate_manual.karate_manual
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 7. CONNECTED COMPONENTS — Single component (no cache)
-- ============================================================================
-- Expected: 1 connected component containing all 34 members.

ASSERT ROW_COUNT = 1
ASSERT VALUE members = 34
USE {{zone_name}}.karate_manual.karate_manual
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 8. SHORTEST PATH — Faction leaders (no cache)
-- ============================================================================
-- Nodes 0 and 33 are NOT directly connected. Shortest distance = 2 hops.

ASSERT ROW_COUNT = 3
ASSERT VALUE distance = 0 WHERE step = 0
ASSERT VALUE distance = 2 WHERE step = 2
ASSERT VALUE node_id = 0 WHERE step = 0
ASSERT VALUE node_id = 33 WHERE step = 2
USE {{zone_name}}.karate_manual.karate_manual
CALL algo.shortestPath({source: 0, target: 33})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ############################################################################
-- PART 3: MANUAL CSR BUILD
-- ############################################################################
-- Now explicitly build the CSR disk cache. This writes a compressed .dcsr
-- file to <edge_table_path>/_deltaforge/karate_manual.dcsr. Subsequent
-- Cypher queries will load from this file (~70ms) instead of rebuilding
-- from the Delta tables (6-14s).


-- ============================================================================
-- 9. CREATE GRAPHCSR — Build the disk cache manually
-- ============================================================================

CREATE GRAPHCSR {{zone_name}}.karate_manual.karate_manual;


-- ############################################################################
-- PART 4: CYPHER WITH CSR CACHE (FAST PATH)
-- ############################################################################
-- Re-run the same queries. Results MUST be identical — the CSR cache is a
-- performance optimization, not a semantic change. Every assertion from
-- Part 2 is repeated here to prove correctness is preserved.


-- ============================================================================
-- 10. BROWSE VERTICES — All 34 club members (with cache)
-- ============================================================================

ASSERT ROW_COUNT = 34
USE {{zone_name}}.karate_manual.karate_manual
MATCH (v)
RETURN v.id AS member_id, v.name AS name, v.role AS role
ORDER BY member_id;


-- ============================================================================
-- 11. TOP HUBS — Degree distribution with cache
-- ============================================================================
-- Identical to query 5: same golden values must hold.

ASSERT ROW_COUNT = 5
ASSERT VALUE degree = 17 WHERE member_id = 33
ASSERT VALUE degree = 16 WHERE member_id = 0
ASSERT VALUE degree = 12 WHERE member_id = 32
ASSERT VALUE degree = 10 WHERE member_id = 2
ASSERT VALUE degree = 9 WHERE member_id = 1
USE {{zone_name}}.karate_manual.karate_manual
MATCH (a)-[r]->(b)
RETURN a.id AS member_id, a.name AS name, COUNT(r) AS degree
ORDER BY degree DESC
LIMIT 5;


-- ============================================================================
-- 12. PAGERANK — Most influential members (with cache)
-- ============================================================================
-- Identical to query 6: same ranking must hold.

ASSERT ROW_COUNT = 10
ASSERT VALUE rank = 1 WHERE node_id = 33
ASSERT VALUE rank = 2 WHERE node_id = 0
USE {{zone_name}}.karate_manual.karate_manual
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 13. CONNECTED COMPONENTS — Single component (with cache)
-- ============================================================================
-- Identical to query 7: still 1 component, still 34 members.

ASSERT ROW_COUNT = 1
ASSERT VALUE members = 34
USE {{zone_name}}.karate_manual.karate_manual
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 14. SHORTEST PATH — Faction leaders (with cache)
-- ============================================================================
-- Identical to query 8: same path, same distances.

ASSERT ROW_COUNT = 3
ASSERT VALUE distance = 0 WHERE step = 0
ASSERT VALUE distance = 2 WHERE step = 2
ASSERT VALUE node_id = 0 WHERE step = 0
ASSERT VALUE node_id = 33 WHERE step = 2
USE {{zone_name}}.karate_manual.karate_manual
CALL algo.shortestPath({source: 0, target: 33})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ############################################################################
-- PART 5: VERIFICATION SUMMARY
-- ############################################################################


-- ============================================================================
-- 15. AUTOMATED VERIFICATION — PASS/FAIL against golden values
-- ============================================================================

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 5
SELECT 'Vertex count = 34' AS test,
       CASE WHEN cnt = 34 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate_manual.vertices)

UNION ALL
SELECT 'Edge row count = 156',
       CASE WHEN cnt = 156 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate_manual.edges)

UNION ALL
SELECT 'No self-loops',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate_manual.edges WHERE src = dst)

UNION ALL
SELECT 'All edge endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate_manual.edges e
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.karate_manual.vertices v WHERE v.vertex_id = e.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.karate_manual.vertices v WHERE v.vertex_id = e.dst)
)

UNION ALL
SELECT 'Symmetric edges (undirected)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' missing reverse edges)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.karate_manual.edges e1
    WHERE NOT EXISTS (
        SELECT 1 FROM {{zone_name}}.karate_manual.edges e2
        WHERE e2.src = e1.dst AND e2.dst = e1.src
    )
);
