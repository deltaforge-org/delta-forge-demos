-- ############################################################################
-- ############################################################################
--
--   ENTERPRISE ORGANIZATION NETWORK — 1M EMPLOYEES / 5M+ CONNECTIONS
--   Organizational Network Analytics via Cypher
--
-- ############################################################################
-- ############################################################################
--
-- This demo simulates a realistic enterprise organization with 1 million
-- employees across 20 departments, 15 cities, and 200 project teams.
-- The graph has genuine community structure: departments form tight
-- clusters, project teams are nested sub-communities, and a small
-- percentage of bridge/liaison employees connect the clusters.
--
-- PART 1: EXPLORE & ANALYZE (queries 1–16)
--   Pattern matching, property filtering, relationship analysis.
--
-- PART 2: GRAPH ALGORITHMS (queries 17–30)
--   Influence mapping, community detection, and path analysis.
--
-- PART 3: GRAPH VISUALIZATION (queries 31–42)
--   Progressive scale tests for the graph visualizer.
--
-- ############################################################################


-- ############################################################################
-- PART 1: EXPLORE & ANALYZE
-- ############################################################################


-- ============================================================================
-- 1. ORGANIZATION SIZE — Verify all 1M employees loaded
-- ============================================================================

ASSERT VALUE total_employees = 1000000
USE {{zone_name}}.graph.stress_test_network
MATCH (n)
RETURN count(n) AS total_employees;


-- ============================================================================
-- 2. TOTAL CONNECTIONS — Full edge scan at 5M+ scale
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_connections = 5059998
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
RETURN count(r) AS total_connections;


-- ============================================================================
-- 3. WORKFORCE BY DEPARTMENT — Headcount distribution
-- ============================================================================
-- HR needs a snapshot of each department. Uneven headcounts across 20
-- departments may indicate growth imbalances.

ASSERT ROW_COUNT = 20
ASSERT VALUE headcount = 50000 WHERE department = 'Engineering'
ASSERT VALUE headcount = 50000 WHERE department = 'Sales'
-- Non-deterministic: float average may vary slightly across engines
ASSERT WARNING VALUE avg_age BETWEEN 40.0 AND 41.5 WHERE department = 'Engineering'
USE {{zone_name}}.graph.stress_test_network
MATCH (n)
RETURN n.department AS department, count(n) AS headcount,
       avg(n.age) AS avg_age
ORDER BY headcount DESC;


-- ============================================================================
-- 4. GLOBAL FOOTPRINT — Employee distribution across 15 offices
-- ============================================================================

ASSERT ROW_COUNT = 15
ASSERT VALUE headcount = 66667 WHERE city = 'SF'
USE {{zone_name}}.graph.stress_test_network
MATCH (n)
RETURN n.city AS city, count(n) AS headcount
ORDER BY headcount DESC;


-- ============================================================================
-- 5. RELATIONSHIP MIX — What types of bonds exist?
-- ============================================================================
-- Understanding the connection type mix across 5M+ edges reveals
-- organizational patterns at enterprise scale.

ASSERT ROW_COUNT = 18
ASSERT VALUE count = 750000 WHERE type = 'colleague'
ASSERT VALUE count = 550000 WHERE type = 'mentor'
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
RETURN r.relationship_type AS type, count(r) AS count,
       avg(r.weight) AS avg_strength
ORDER BY count DESC;


-- ============================================================================
-- 6. ENGINEERING VETERANS — Senior engineers over 50
-- ============================================================================
-- HR is planning a mentorship program. Find experienced engineers who
-- could mentor the next generation.

ASSERT ROW_COUNT = 25
ASSERT VALUE age = 59
USE {{zone_name}}.graph.stress_test_network
MATCH (n)
WHERE n.department = 'Engineering' AND n.age > 50
RETURN n.name AS name, n.age AS age, n.city AS city
ORDER BY n.age DESC
LIMIT 25;


-- ============================================================================
-- 7. STRONGEST MENTORSHIPS — High-impact mentor bonds
-- ============================================================================
-- Which mentor-mentee pairs have bonds > 0.8? These are the mentorships
-- worth studying and replicating across the enterprise.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.stress_test_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor' AND r.weight > 0.8
RETURN mentor.name AS mentor, mentee.name AS mentee, r.weight AS strength
ORDER BY r.weight DESC
LIMIT 25;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Who connects the silos?
-- ============================================================================

ASSERT ROW_COUNT = 30
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC
LIMIT 30;


-- ============================================================================
-- 9. OFFICE COLLABORATION — Which city pairs work together?
-- ============================================================================
-- For remote work policy decisions: which office pairs collaborate most?

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
RETURN a.city AS from_city, b.city AS to_city,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC
LIMIT 25;


-- ============================================================================
-- 10. KNOWLEDGE PATHS — 2-hop information flow from employee #1
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1
RETURN a.name AS source, b.name AS relay, c.name AS reached
LIMIT 50;


-- ============================================================================
-- 11. REACHABILITY — Who can employee #1 reach within 2 hops?
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[*1..2]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
LIMIT 50;


-- ============================================================================
-- 12. RECIPROCAL RELATIONSHIPS — Mutual bonds at scale
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN a.name AS person_a, b.name AS person_b,
       r1.relationship_type AS a_to_b, r2.relationship_type AS b_to_a
LIMIT 25;


-- ============================================================================
-- 13. MENTORSHIP LEVEL FLOW — Are seniors mentoring juniors?
-- ============================================================================
-- A healthy program has senior staff (L5+) mentoring people 1-2 levels
-- below. If VPs only mentor other VPs, the program isn't reaching juniors.

ASSERT ROW_COUNT = 14
ASSERT VALUE mentorship_count = 192000 WHERE mentor_level = 'L6'
ASSERT VALUE mentorship_count = 120000 WHERE mentor_level = 'L5'
USE {{zone_name}}.graph.stress_test_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.level AS mentor_level, mentee.level AS mentee_level,
       count(r) AS mentorship_count, avg(r.weight) AS avg_strength
ORDER BY mentorship_count DESC;


-- ============================================================================
-- 14. SENIORITY FLOW — How does information flow across levels?
-- ============================================================================
-- Do senior people mostly connect to other seniors (echo chamber), or
-- do connections span levels?

ASSERT ROW_COUNT = 20
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
RETURN a.level AS from_level, b.level AS to_level,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC
LIMIT 20;


-- ============================================================================
-- 15. TEAM vs CROSS-TEAM — Is the org collaborating or siloed?
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE connections = 3125998
-- Non-deterministic: float average may vary slightly across engines
ASSERT WARNING VALUE avg_strength BETWEEN 0.7 AND 0.8
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.department = b.department
RETURN 'within_department' AS scope, count(r) AS connections,
       avg(r.weight) AS avg_strength;


-- ============================================================================
-- 16. CROSS-DEPARTMENT VOLUME — Complement to within-department
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE connections = 1934000
-- Non-deterministic: float average may vary slightly across engines
ASSERT WARNING VALUE avg_strength BETWEEN 0.35 AND 0.45
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN 'cross_department' AS scope, count(r) AS connections,
       avg(r.weight) AS avg_strength;


-- ############################################################################
-- ############################################################################
--
-- PART 2: GRAPH ALGORITHMS — Influence, Communities & Paths
--
-- ############################################################################
-- ############################################################################


-- ============================================================================
-- 17. DEGREE CENTRALITY — Most connected people at 1M scale
-- ============================================================================

ASSERT ROW_COUNT = 25
ASSERT VALUE total_degree = 194
ASSERT VALUE out_degree = 178
USE {{zone_name}}.graph.stress_test_network
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 25;


-- ============================================================================
-- 18. PAGERANK — True organizational influence at enterprise scale
-- ============================================================================
-- PageRank at 1M nodes and 5M edges: finds the people who are
-- connected to by other well-connected people. The real power structure.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.stress_test_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 5})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 25;


-- ============================================================================
-- 19. NATURAL COMMUNITIES — Connected components at scale
-- ============================================================================
-- In a healthy org, there should be one giant component. Multiple
-- components indicate truly disconnected groups.

ASSERT ROW_COUNT >= 1
ASSERT VALUE community_size = 1000000
USE {{zone_name}}.graph.stress_test_network
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS community_size
ORDER BY community_size DESC
LIMIT 25;


-- ============================================================================
-- 20. LOUVAIN COMMUNITIES — Real organizational clusters
-- ============================================================================
-- Finds dense subgroups regardless of the formal org chart. Do the
-- detected communities align with the 20 departments?

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.graph.stress_test_network
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS size
ORDER BY size DESC
LIMIT 25;


-- ============================================================================
-- 21. GATEKEEPERS — Who controls information flow at scale?
-- ============================================================================
-- Betweenness centrality at 1M nodes: finds people on many shortest paths.
-- If they leave, communication between groups breaks down. Critical
-- for succession planning and retention strategy.
-- Uses samplingSize for approximate mode (exact is O(n·m), infeasible at 1M).

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.stress_test_network
CALL algo.betweenness({samplingSize: 1000})
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 25;


-- ============================================================================
-- 22. TIGHT-KNIT GROUPS — Triangle count at scale
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.stress_test_network
CALL algo.triangle_count()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC
LIMIT 25;


-- ============================================================================
-- 23. SHORTEST PATH — Route across the enterprise
-- ============================================================================
-- If employee #1 needs to reach employee #500000 (different department,
-- different city), what's the fastest chain? Tests Dijkstra at 1M scale.

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.graph.stress_test_network
CALL algo.shortestPath({source: 1, target: 500000})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 24. SIX DEGREES — Small world property at 1M scale
-- ============================================================================
-- Most people should be reachable within 4-6 hops even in an enterprise
-- of 1 million. More suggests organizational fragmentation.

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.graph.stress_test_network
CALL algo.bfs({source: 1})
YIELD node_id, depth, parent_id
RETURN depth, count(*) AS people_at_distance
ORDER BY depth
LIMIT 20;


-- ============================================================================
-- 25. DIRECTED REACHABILITY — Strongly connected components
-- ============================================================================
-- A large SCC means good bidirectional communication. Many small SCCs
-- indicate one-way information flow (top-down only).

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.graph.stress_test_network
CALL algo.scc()
YIELD node_id, component_id
RETURN component_id, count(*) AS scc_size
ORDER BY scc_size DESC
LIMIT 25;


-- ============================================================================
-- 26. ACCESSIBILITY — Who can reach everyone fastest?
-- ============================================================================
-- High closeness = good candidate for company-wide announcements or
-- change agent roles.
-- Uses samplingSize for approximate mode (exact is O(n·m), infeasible at 1M).

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.stress_test_network
CALL algo.closeness({samplingSize: 1000})
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 25;


-- ============================================================================
-- 27. BACKBONE NETWORK — Essential connections
-- ============================================================================
-- The minimum spanning tree at 1M scale: the lightest set of edges
-- that still connects every employee. Reveals the organizational skeleton.

ASSERT ROW_COUNT = 1
ASSERT VALUE backbone_edges = 999999
USE {{zone_name}}.graph.stress_test_network
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN count(*) AS backbone_edges, sum(weight) AS total_weight;


-- ============================================================================
-- 28. ALL DISTANCES FROM EMPLOYEE #1
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.graph.stress_test_network
CALL algo.allShortestPaths({source: 1})
YIELD node_id, distance, path
RETURN node_id, distance
ORDER BY distance
LIMIT 50;


-- ============================================================================
-- 29. DEPTH-FIRST EXPLORATION — Trace influence chains
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.graph.stress_test_network
CALL algo.dfs({source: 1})
YIELD node_id, discovery_time, finish_time, parent_id
RETURN node_id, discovery_time, finish_time, parent_id
ORDER BY discovery_time
LIMIT 50;


-- ============================================================================
-- 30. NEAREST NEIGHBORS — Structural similarity at scale
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.graph.stress_test_network
CALL algo.knn({node: 1, k: 10})
YIELD neighbor_id, similarity, rank
RETURN neighbor_id, similarity, rank
ORDER BY rank;


-- ############################################################################
-- ############################################################################
--
-- PART 3: GRAPH VISUALIZATION — Progressive Scale Tests
--
-- ############################################################################
-- ############################################################################
-- Each query is a real use case — visualizing a specific organizational
-- slice. Scale increases progressively to find rendering limits.
--
--   31.  Single team (~100 nodes)        — should render instantly
--   32.  Small department slice (500)     — fast render
--   33.  Full department unit (1,000)     — smooth render
--   34.  Multi-team view (5,000)          — may need layout time
--   35.  Division-level (10,000)          — stress test begins
--   36.  Regional network (50,000)        — expect lag
--   37.  Large region (100,000)           — browser stress
--   38.  Full organization (1M + 5M)      — ultimate stress test
--
-- Node-only rendering:
--   39.  100 nodes    — node layout test
--   40.  1,000 nodes  — medium density
--   41.  10,000 nodes — large node cloud
--   42.  All 1M nodes — extreme test
-- ############################################################################


-- ============================================================================
-- 31. VIZ: SINGLE PROJECT TEAM — ~100 person team network
-- ============================================================================

ASSERT ROW_COUNT = 389
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 100 AND b.id <= 100
RETURN a, r, b;


-- ============================================================================
-- 32. VIZ: DEPARTMENT SLICE — 500 employees
-- ============================================================================

ASSERT ROW_COUNT = 3217
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 500 AND b.id <= 500
RETURN a, r, b;


-- ============================================================================
-- 33. VIZ: FULL DEPARTMENT — 1,000 employees
-- ============================================================================

ASSERT ROW_COUNT = 7377
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 1000 AND b.id <= 1000
RETURN a, r, b;


-- ============================================================================
-- 34. VIZ: MULTI-TEAM — 5,000 employees
-- ============================================================================

ASSERT ROW_COUNT = 41095
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 5000 AND b.id <= 5000
RETURN a, r, b;


-- ============================================================================
-- 35. VIZ: DIVISION — 10,000 employees, stress test begins
-- ============================================================================

ASSERT ROW_COUNT = 83323
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 10000 AND b.id <= 10000
RETURN a, r, b;


-- ============================================================================
-- 36. VIZ: REGIONAL — 50,000 employees, extreme rendering
-- ============================================================================
-- WARNING: Large result set. The visualizer may become sluggish.

ASSERT ROW_COUNT = 421693
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 50000 AND b.id <= 50000
RETURN a, r, b;


-- ============================================================================
-- 37. VIZ: LARGE REGION — 100,000 employees, browser stress
-- ============================================================================
-- WARNING: Expect significant lag or memory pressure.

ASSERT ROW_COUNT = 846096
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 100000 AND b.id <= 100000
RETURN a, r, b;


-- ============================================================================
-- 38. VIZ: FULL ORGANIZATION — All 1M employees + 5M connections
-- ============================================================================
-- WARNING: Ultimate stress test. The visualizer will likely freeze.

ASSERT ROW_COUNT = 5059998
USE {{zone_name}}.graph.stress_test_network
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 39. VIZ NODES: SINGLE TEAM — 100 employee nodes
-- ============================================================================

ASSERT ROW_COUNT = 100
USE {{zone_name}}.graph.stress_test_network
MATCH (n)
WHERE n.id <= 100
RETURN n;


-- ============================================================================
-- 40. VIZ NODES: DEPARTMENT — 1,000 employee nodes
-- ============================================================================

ASSERT ROW_COUNT = 1000
USE {{zone_name}}.graph.stress_test_network
MATCH (n)
WHERE n.id <= 1000
RETURN n;


-- ============================================================================
-- 41. VIZ NODES: DIVISION — 10,000 employee nodes
-- ============================================================================

ASSERT ROW_COUNT = 10000
USE {{zone_name}}.graph.stress_test_network
MATCH (n)
WHERE n.id <= 10000
RETURN n;


-- ============================================================================
-- 42. VIZ NODES: FULL ORGANIZATION — All 1M employee nodes
-- ============================================================================
-- WARNING: Returns 1,000,000 node objects. Ultimate node rendering test.

ASSERT ROW_COUNT = 1000000
USE {{zone_name}}.graph.stress_test_network
MATCH (n)
RETURN n;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: proves the full 1M-node / 5M-edge graph
-- loaded with the correct topology and uniform department distribution.

-- Node count, department count, and city count
ASSERT ROW_COUNT = 1
ASSERT VALUE total_people = 1000000
ASSERT VALUE dept_count = 20
ASSERT VALUE city_count = 15
SELECT
    COUNT(*)                   AS total_people,
    COUNT(DISTINCT department) AS dept_count,
    COUNT(DISTINCT city)       AS city_count
FROM {{zone_name}}.graph.st_people;

-- Total edge count across all 7 batches
ASSERT ROW_COUNT = 1
ASSERT VALUE total_edges = 5059998
SELECT COUNT(*) AS total_edges
FROM {{zone_name}}.graph.st_edges;

-- Uniform department headcount: 1M / 20 = exactly 50000 per department
ASSERT ROW_COUNT = 20
ASSERT VALUE headcount = 50000 WHERE department = 'Engineering'
SELECT department, COUNT(*) AS headcount
FROM {{zone_name}}.graph.st_people
GROUP BY department
ORDER BY department;
