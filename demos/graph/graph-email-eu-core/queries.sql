-- ############################################################################
-- ############################################################################
--
--   EMAIL-EU-CORE — EUROPEAN INSTITUTION EMAIL NETWORK
--   1,005 Vertices / 25,571 Directed Edges / Weight = 1.0
--
-- ############################################################################
-- ############################################################################
--
-- A directed email communication network from a European research institution
-- (SNAP dataset, Leskovec et al.). Unlike the other graph demos, this dataset
-- is DIRECTED and contains self-loops. It has 42 ground-truth department
-- communities, 105,461 triangles, and a diameter of 7.
--
-- PART 1: DATA INTEGRITY CHECKS (queries 1–5)
-- PART 2: CYPHER — GRAPH EXPLORATION (queries 6–10)
-- PART 3: CYPHER — GRAPH ALGORITHMS (queries 11–17)
-- PART 4: VERIFICATION SUMMARY (query 18)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY CHECKS
-- ############################################################################


-- ============================================================================
-- 1. VERTEX & EDGE COUNTS — Verify data loaded correctly
-- ============================================================================
-- 1,005 vertices, 25,571 directed edge rows

-- Verify vertex count
ASSERT VALUE row_count = 1005
SELECT COUNT(*) AS row_count FROM {{zone_name}}.email_eu_core.vertices;

-- Verify edge count
ASSERT VALUE row_count = 25571
SELECT COUNT(*) AS row_count FROM {{zone_name}}.email_eu_core.edges;


-- ============================================================================
-- 2. GRAPH CONFIG — Verify graph definition
-- ============================================================================

SHOW GRAPH;


-- ============================================================================
-- 3. REFERENTIAL INTEGRITY — All edges have valid endpoints
-- ============================================================================

ASSERT VALUE orphan_edges = 0
SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.email_eu_core.edges e
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.email_eu_core.vertices v WHERE v.vertex_id = e.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.email_eu_core.vertices v WHERE v.vertex_id = e.dst);


-- ============================================================================
-- 4. SELF-LOOP COUNT — This dataset HAS self-loops (emails to self)
-- ============================================================================
-- Unlike Karate Club, self-loops are expected here. Count them.

ASSERT VALUE self_loops = 642
SELECT COUNT(*) AS self_loops
FROM {{zone_name}}.email_eu_core.edges
WHERE src = dst;


-- ============================================================================
-- 5. EMAIL TYPES — Distribution of communication types
-- ============================================================================

ASSERT ROW_COUNT = 6
USE {{zone_name}}.email_eu_core.email_eu_core
MATCH (a)-[r]->(b)
RETURN r.edge_type AS type, count(r) AS count
ORDER BY count DESC;


-- ############################################################################
-- PART 2: CYPHER — GRAPH EXPLORATION
-- ############################################################################


-- ============================================================================
-- 6. BROWSE VERTICES — Sample of institution members
-- ============================================================================

ASSERT ROW_COUNT = 20
USE {{zone_name}}.email_eu_core.email_eu_core
MATCH (v)
RETURN v.id AS member_id, v.name AS name, v.department AS department
ORDER BY member_id
LIMIT 20;


-- ============================================================================
-- 7. OUT-DEGREE DISTRIBUTION — How many people does each member email?
-- ============================================================================
-- This is a directed graph, so in-degree and out-degree differ.

ASSERT ROW_COUNT = 20
USE {{zone_name}}.email_eu_core.email_eu_core
MATCH (a)-[r]->(b)
RETURN a.id AS member_id, a.name AS name, COUNT(r) AS out_degree
ORDER BY out_degree DESC, member_id ASC
LIMIT 20;


-- ============================================================================
-- 8. IN-DEGREE DISTRIBUTION — How many people email each member?
-- ============================================================================
-- Members who receive the most emails may be managers or key contacts.

ASSERT ROW_COUNT = 20
USE {{zone_name}}.email_eu_core.email_eu_core
MATCH (a)-[r]->(b)
RETURN b.id AS member_id, b.name AS name, COUNT(r) AS in_degree
ORDER BY in_degree DESC, member_id ASC
LIMIT 20;


-- ============================================================================
-- 9. TOP HUBS — Members with highest total degree (in + out)
-- ============================================================================
-- The most active email communicators in the institution.

ASSERT ROW_COUNT = 10
ASSERT VALUE total_degree = 546 WHERE member_id = 160
SELECT member_id, in_deg + out_deg AS total_degree, in_deg, out_deg
FROM (
    SELECT
        COALESCE(o.member_id, i.member_id) AS member_id,
        COALESCE(o.out_deg, 0) AS out_deg,
        COALESCE(i.in_deg, 0) AS in_deg
    FROM (
        SELECT src AS member_id, COUNT(*) AS out_deg
        FROM {{zone_name}}.email_eu_core.edges GROUP BY src
    ) o
    FULL OUTER JOIN (
        SELECT dst AS member_id, COUNT(*) AS in_deg
        FROM {{zone_name}}.email_eu_core.edges GROUP BY dst
    ) i ON o.member_id = i.member_id
)
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 10. TWO-HOP REACHABILITY FROM TOP HUB — Communication reach
-- ============================================================================
-- How many members are within 2 directed hops of the most active sender?
-- Uses SQL on the edges table to avoid variable-length path explosion.

ASSERT ROW_COUNT = 1
ASSERT VALUE reachable_in_2_hops >= 900
ASSERT VALUE reachable_in_2_hops <= 910
SELECT hub, COUNT(DISTINCT reachable) AS reachable_in_2_hops
FROM (
    -- 1-hop: direct targets of the top hub
    SELECT th.src AS hub, e1.dst AS reachable
    FROM (SELECT src, COUNT(*) AS deg FROM {{zone_name}}.email_eu_core.edges GROUP BY src ORDER BY deg DESC LIMIT 1) th
    JOIN {{zone_name}}.email_eu_core.edges e1 ON e1.src = th.src
    UNION
    -- 2-hop: targets of targets
    SELECT th.src AS hub, e2.dst AS reachable
    FROM (SELECT src, COUNT(*) AS deg FROM {{zone_name}}.email_eu_core.edges GROUP BY src ORDER BY deg DESC LIMIT 1) th
    JOIN {{zone_name}}.email_eu_core.edges e1 ON e1.src = th.src
    JOIN {{zone_name}}.email_eu_core.edges e2 ON e2.src = e1.dst
) sub
GROUP BY hub;


-- ############################################################################
-- PART 3: CYPHER — GRAPH ALGORITHMS
-- ############################################################################


-- ============================================================================
-- 11. PAGERANK — Identify most influential members
-- ============================================================================
-- PageRank on directed graphs highlights members who receive emails
-- from other influential members.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.email_eu_core.email_eu_core
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 12. DEGREE CENTRALITY — Normalized degree
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.email_eu_core.email_eu_core
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 13. BETWEENNESS CENTRALITY — Communication bridges
-- ============================================================================
-- Members who bridge different departments will have high betweenness.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.email_eu_core.email_eu_core
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 10;


-- ============================================================================
-- 14. CLOSENESS CENTRALITY — Proximity to all other members
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.email_eu_core.email_eu_core
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 10;


-- ============================================================================
-- 15. COMMUNITY DETECTION — Recover department structure
-- ============================================================================
-- Ground truth: 42 department communities.
-- Louvain should find a comparable number of communities.

-- Non-deterministic: Louvain is a stochastic algorithm; community count varies by run and resolution setting
ASSERT WARNING ROW_COUNT >= 1
USE {{zone_name}}.email_eu_core.email_eu_core
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS members
ORDER BY members DESC
LIMIT 20;


-- ============================================================================
-- 16. CONNECTED COMPONENTS — Weakly connected components
-- ============================================================================
-- Known: 20 weakly connected components (1 giant of 986 nodes + 19 singletons
-- that appear only in self-loops and have no other neighbours).
-- Largest component has 986 nodes (98.1% of the network).

ASSERT ROW_COUNT = 20
USE {{zone_name}}.email_eu_core.email_eu_core
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS members
ORDER BY members DESC
LIMIT 20;


-- ============================================================================
-- 17. SHORTEST PATH — Distance between two active members
-- ============================================================================
-- Diameter is 7, so shortest paths are relatively short.

ASSERT ROW_COUNT = 2
USE {{zone_name}}.email_eu_core.email_eu_core
CALL algo.shortestPath({source: 0, target: 1})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ############################################################################
-- PART 4: VERIFICATION SUMMARY
-- ############################################################################


-- ============================================================================
-- 18. AUTOMATED VERIFICATION — PASS/FAIL against golden values
-- ============================================================================
-- All checks should return PASS. Any FAIL indicates data loading issues
-- or algorithm correctness problems.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 8
SELECT 'Vertex count = 1005' AS test,
       CASE WHEN cnt = 1005 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.email_eu_core.vertices)

UNION ALL
SELECT 'Edge row count = 25571',
       CASE WHEN cnt = 25571 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.email_eu_core.edges)

UNION ALL
SELECT 'Has self-loops (directed network)',
       CASE WHEN cnt > 0 THEN 'PASS (' || CAST(cnt AS VARCHAR) || ' self-loops)'
            ELSE 'FAIL (expected self-loops but found 0)' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.email_eu_core.edges WHERE src = dst)

UNION ALL
SELECT 'All edge endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.email_eu_core.edges e
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.email_eu_core.vertices v WHERE v.vertex_id = e.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.email_eu_core.vertices v WHERE v.vertex_id = e.dst)
)

UNION ALL
SELECT 'Vertex ID range = 0–1004',
       CASE WHEN min_id = 0 AND max_id = 1004 THEN 'PASS'
            ELSE 'FAIL (range ' || CAST(min_id AS VARCHAR) || '–' || CAST(max_id AS VARCHAR) || ')' END
FROM (
    SELECT MIN(vertex_id) AS min_id, MAX(vertex_id) AS max_id FROM {{zone_name}}.email_eu_core.vertices
)

UNION ALL
SELECT 'Directed graph (asymmetric edges)',
       CASE WHEN asym_count > 0 THEN 'PASS (' || CAST(asym_count AS VARCHAR) || ' edges without reverse)'
            ELSE 'FAIL (all edges symmetric — expected directed)' END
FROM (
    SELECT COUNT(*) AS asym_count FROM {{zone_name}}.email_eu_core.edges e1
    WHERE NOT EXISTS (
        SELECT 1 FROM {{zone_name}}.email_eu_core.edges e2
        WHERE e2.src = e1.dst AND e2.dst = e1.src
    )
)

UNION ALL
SELECT 'Max out-degree check',
       CASE WHEN max_deg >= 50 THEN 'PASS (max out-degree = ' || CAST(max_deg AS VARCHAR) || ')'
            ELSE 'FAIL (max out-degree = ' || CAST(max_deg AS VARCHAR) || ', expected >= 50)' END
FROM (
    SELECT MAX(deg) AS max_deg FROM (
        SELECT src, COUNT(*) AS deg FROM {{zone_name}}.email_eu_core.edges GROUP BY src
    )
)

UNION ALL
SELECT '6 edge types (including self-note)',
       CASE WHEN cnt = 6 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (
    SELECT COUNT(DISTINCT edge_type) AS cnt FROM {{zone_name}}.email_eu_core.edges
);
