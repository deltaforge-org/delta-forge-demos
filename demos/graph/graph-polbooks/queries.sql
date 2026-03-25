-- ############################################################################
-- ############################################################################
--
--   POLITICAL BOOKS — CO-PURCHASING NETWORK WITH GROUND-TRUTH COMMUNITIES
--   105 Vertices / 441 Undirected Edges (882 rows) / Weight = 1.0
--
-- ############################################################################
-- ############################################################################
--
-- 105 books about US politics sold on Amazon (Krebs, compiled by Newman).
-- Edges represent frequent co-purchasing. Each book belongs to one of three
-- ground-truth communities (liberal, neutral, conservative), making this a
-- classic benchmark for community detection with known labels.
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
-- 105 vertices, 882 edge rows (441 undirected edges x 2)

-- Verify vertex count
ASSERT VALUE row_count = 105
SELECT COUNT(*) AS row_count FROM {{zone_name}}.polbooks.vertices;

-- Verify edge count (441 undirected edges x 2)
ASSERT VALUE row_count = 882
SELECT COUNT(*) AS row_count FROM {{zone_name}}.polbooks.edges;


-- ============================================================================
-- 2. GRAPH CONFIG — Verify graph definition
-- ============================================================================

SHOW GRAPH;


-- ============================================================================
-- 3. REFERENTIAL INTEGRITY — All edges have valid endpoints
-- ============================================================================

ASSERT VALUE orphan_edges = 0
SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.polbooks.edges e
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.polbooks.vertices v WHERE v.vertex_id = e.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.polbooks.vertices v WHERE v.vertex_id = e.dst);


-- ============================================================================
-- 4. SELF-LOOP CHECK — No book should co-purchase with itself
-- ============================================================================

ASSERT VALUE self_loops = 0
SELECT COUNT(*) AS self_loops
FROM {{zone_name}}.polbooks.edges
WHERE src = dst;


-- ############################################################################
-- PART 2: CYPHER — GRAPH EXPLORATION
-- ############################################################################


-- ============================================================================
-- 5. BROWSE VERTICES — List all 105 books
-- ============================================================================

ASSERT ROW_COUNT = 105
USE {{zone_name}}.polbooks.political_books
MATCH (v)
RETURN v.id AS book_id
ORDER BY book_id;


-- ============================================================================
-- 6. DEGREE DISTRIBUTION — How many co-purchases does each book have?
-- ============================================================================
-- Known: Maximum degree is 25.

ASSERT ROW_COUNT = 105
USE {{zone_name}}.polbooks.political_books
MATCH (a)-[r]->(b)
RETURN a.id AS book_id, COUNT(r) AS degree
ORDER BY degree DESC, book_id ASC;


-- ============================================================================
-- 7. TOP HUBS — The most co-purchased books
-- ============================================================================
-- Expected: Top books have degree 25 or higher.

ASSERT ROW_COUNT = 5
USE {{zone_name}}.polbooks.political_books
MATCH (a)-[r]->(b)
RETURN a.id AS book_id, COUNT(r) AS degree
ORDER BY degree DESC
LIMIT 5;


-- ============================================================================
-- 8. NEIGHBORHOOD OF TOP HUB — Most connected book's co-purchases
-- ============================================================================
-- The most connected book has up to 25 co-purchasing links.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.polbooks.political_books
MATCH (a)-[r]->(b)
WITH a, COUNT(r) AS degree
ORDER BY degree DESC
LIMIT 1
MATCH (a)-[]->(c)
RETURN a.id AS hub_id, c.id AS copurchase_id
ORDER BY copurchase_id;


-- ============================================================================
-- 9. TWO-HOP REACHABILITY FROM NODE 0 — How far does co-purchasing reach?
-- ============================================================================
-- Most of the 105-node graph should be reachable within 2 hops.

ASSERT VALUE reachable_in_2_hops = 31
ASSERT ROW_COUNT = 1
USE {{zone_name}}.polbooks.political_books
MATCH (a)-[*1..2]->(b)
WHERE a.id = 0
RETURN COUNT(DISTINCT b.id) AS reachable_in_2_hops;


-- ############################################################################
-- PART 3: CYPHER — GRAPH ALGORITHMS
-- ############################################################################


-- ============================================================================
-- 10. PAGERANK — Identify most influential books
-- ============================================================================
-- Expected: The most co-purchased books should have the highest PageRank.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.polbooks.political_books
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 11. DEGREE CENTRALITY — Normalized degree
-- ============================================================================
-- The most connected books should rank highest.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.polbooks.political_books
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 12. BETWEENNESS CENTRALITY — Bridge nodes
-- ============================================================================
-- Books that bridge political communities (e.g., neutral books) will
-- have the highest betweenness.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.polbooks.political_books
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 10;


-- ============================================================================
-- 13. CLOSENESS CENTRALITY — How close is each book to all others?
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.polbooks.political_books
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 10;


-- ============================================================================
-- 14. COMMUNITY DETECTION — Can we recover the political leanings?
-- ============================================================================
-- Ground truth: 3 communities (liberal, neutral, conservative).
-- Community detection should approximate this partitioning.

-- Non-deterministic: Louvain uses random tie-breaking; community count varies by seed (ground truth: 3)
ASSERT ROW_COUNT >= 2
USE {{zone_name}}.polbooks.political_books
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 15. CONNECTED COMPONENTS — Is the graph fully connected?
-- ============================================================================
-- Expected: The graph should be connected or nearly connected.

ASSERT ROW_COUNT = 1
USE {{zone_name}}.polbooks.political_books
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC;


-- ============================================================================
-- 16. SHORTEST PATH — Distance between two books
-- ============================================================================
-- Book 0 and book 104 should be reachable through the co-purchasing network.

ASSERT ROW_COUNT = 5
USE {{zone_name}}.polbooks.political_books
CALL algo.shortestPath({source: 0, target: 104})
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
ASSERT ROW_COUNT = 8
SELECT 'Vertex count = 105' AS test,
       CASE WHEN cnt = 105 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.polbooks.vertices)

UNION ALL
SELECT 'Edge row count = 882',
       CASE WHEN cnt = 882 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.polbooks.edges)

UNION ALL
SELECT 'No self-loops',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.polbooks.edges WHERE src = dst)

UNION ALL
SELECT 'All edge endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.polbooks.edges e
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.polbooks.vertices v WHERE v.vertex_id = e.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.polbooks.vertices v WHERE v.vertex_id = e.dst)
)

UNION ALL
SELECT 'Max degree >= 25 (most connected book)',
       CASE WHEN max_deg >= 25 THEN 'PASS' ELSE 'FAIL (got ' || CAST(max_deg AS VARCHAR) || ')' END
FROM (
    SELECT MAX(deg) AS max_deg FROM (
        SELECT src, COUNT(*) AS deg FROM {{zone_name}}.polbooks.edges GROUP BY src
    )
)

UNION ALL
SELECT 'Vertex ID range = 0–104',
       CASE WHEN min_id = 0 AND max_id = 104 THEN 'PASS'
            ELSE 'FAIL (range ' || CAST(min_id AS VARCHAR) || '–' || CAST(max_id AS VARCHAR) || ')' END
FROM (
    SELECT MIN(vertex_id) AS min_id, MAX(vertex_id) AS max_id FROM {{zone_name}}.polbooks.vertices
)

UNION ALL
SELECT 'All weights = 1.0 (unweighted)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' non-unit weights)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.polbooks.edges WHERE weight <> 1.0
)

UNION ALL
SELECT 'Symmetric edges (undirected)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' missing reverse edges)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.polbooks.edges e1
    WHERE NOT EXISTS (
        SELECT 1 FROM {{zone_name}}.polbooks.edges e2
        WHERE e2.src = e1.dst AND e2.dst = e1.src
    )
);
