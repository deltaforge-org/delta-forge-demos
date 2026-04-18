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
-- All proof values were independently computed using DuckDB against
-- the same deterministic generation formulas. Delta Forge must reproduce
-- every value exactly — proving correct execution at enterprise scale.
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
-- The graph engine must scan all 1,000,000 vertex nodes and return
-- an exact count. Proof: generate_series(1, 1000000) → exactly 1M rows.

ASSERT VALUE total_employees = 1000000
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (n)
RETURN count(n) AS total_employees;


-- ============================================================================
-- 2. TOTAL CONNECTIONS — Full edge scan at 5M+ scale
-- ============================================================================
-- Seven deterministic batches produce exactly 5,059,998 directed edges:
--   Batch 1: 1,500,000  (dept neighborhood)
--   Batch 2: 1,000,000  (team connections)
--   Batch 3:   800,000  (city social)
--   Batch 4:   550,000  (mentorship)
--   Batch 5:   400,000  (bridge nodes)
--   Batch 6:   490,000  (hub nodes)
--   Batch 7:   319,998  (weak ties)

ASSERT ROW_COUNT = 1
ASSERT VALUE total_connections = 5059998
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
RETURN count(r) AS total_connections;


-- ============================================================================
-- 3. WORKFORCE BY DEPARTMENT — Headcount distribution
-- ============================================================================
-- department = id % 20 distributes 1M people across 20 departments.
-- IDs 1..1M: residues 1..10 each get 50,000; residues 11..19 and 0 each
-- get 50,000. Uniform distribution: exactly 50,000 per department.

ASSERT ROW_COUNT = 20
ASSERT VALUE headcount = 50000 WHERE department = 'Engineering'
ASSERT VALUE headcount = 50000 WHERE department = 'Sales'
ASSERT VALUE headcount = 50000 WHERE department = 'AI/ML'
ASSERT VALUE headcount = 50000 WHERE department = 'Data Science'
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (n)
RETURN n.department AS department, count(n) AS headcount,
       avg(n.age) AS avg_age
ORDER BY headcount DESC;


-- ============================================================================
-- 4. GLOBAL FOOTPRINT — Employee distribution across 15 offices
-- ============================================================================
-- city = id % 15. For IDs 1..1M: residues 1..10 get 66,667 people each,
-- residues 0 and 11..14 get 66,666. Proves correct modular distribution.

ASSERT ROW_COUNT = 15
ASSERT VALUE headcount = 66667 WHERE city = 'SF'
ASSERT VALUE headcount = 66667 WHERE city = 'London'
ASSERT VALUE headcount = 66666 WHERE city = 'NYC'
ASSERT VALUE headcount = 66666 WHERE city = 'Austin'
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (n)
RETURN n.city AS city, count(n) AS headcount
ORDER BY headcount DESC;


-- ============================================================================
-- 5. RELATIONSHIP MIX — What types of bonds exist?
-- ============================================================================
-- The 7 edge batches produce exactly 18 relationship types. Top counts
-- come from Batch 1 (colleague=750K, teammate=750K) and Batch 4
-- (mentor=550K). Each count is deterministic from the generation logic.

ASSERT ROW_COUNT = 18
ASSERT VALUE count = 750000 WHERE type = 'colleague'
ASSERT VALUE count = 750000 WHERE type = 'teammate'
ASSERT VALUE count = 550000 WHERE type = 'mentor'
ASSERT VALUE count = 333334 WHERE type = 'project-mate'
ASSERT VALUE count = 333333 WHERE type = 'sprint-partner'
ASSERT VALUE count = 333333 WHERE type = 'code-reviewer'
ASSERT VALUE count = 200000 WHERE type = 'city-social'
ASSERT VALUE count = 200000 WHERE type = 'lunch-buddy'
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
RETURN r.relationship_type AS type, count(r) AS count,
       avg(r.weight) AS avg_strength
ORDER BY count DESC;


-- ============================================================================
-- 6. ENGINEERING VETERANS — Senior engineers over 50
-- ============================================================================
-- Age formula: 22 + CAST(((id × φ) mod 1) × 38.0 AS INT). The INT cast
-- truncates toward zero, so the INT-cast portion is in [0, 37] — the true
-- age range is 22..59. The top rows returned by ORDER BY age DESC all have
-- age = 59 (the max).

ASSERT ROW_COUNT = 25
ASSERT VALUE age = 59
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (n)
WHERE n.department = 'Engineering' AND n.age > 50
RETURN n.name AS name, n.age AS age, n.city AS city
ORDER BY n.age DESC
LIMIT 25;


-- ============================================================================
-- 7. STRONGEST MENTORSHIPS — High-impact mentor bonds
-- ============================================================================
-- Mentor weights use: 0.6 + 0.4 × ((src×3 + dst×7) × φ mod 1).
-- Range is [0.6, 1.0). Pairs with weight > 0.8 are in the top 50%
-- of the mentor weight distribution.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor' AND r.weight > 0.8
RETURN mentor.name AS mentor, mentee.name AS mentee, r.weight AS strength
ORDER BY r.weight DESC
LIMIT 25;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Who connects the silos?
-- ============================================================================
-- The top cross-department pair is Engineering → Platform with exactly
-- 91,000 connections: 50K shared-city edges + 41K bridge/hub/weak-tie
-- edges between departments that are 15 IDs apart.

ASSERT ROW_COUNT = 30
ASSERT VALUE connections = 91000 WHERE from_dept = 'Engineering' AND to_dept = 'Platform'
USE {{zone_name}}.stress_test_network.stress_test_network
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
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
RETURN a.city AS from_city, b.city AS to_city,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC
LIMIT 25;


-- ============================================================================
-- 10. IDENTITY CHECK — Verify specific employees by ID
-- ============================================================================
-- The generation is deterministic: employee #1 is Marcus_1 in Marketing/SF,
-- employee #1000 is Priya_1000, a VP in Engineering/Seattle,
-- employee #500000 is Priya_500000, a VP in Engineering/Tokyo.
-- This proves the Cypher engine correctly resolves vertex properties.

ASSERT ROW_COUNT = 3
ASSERT VALUE name = 'Marcus_1' WHERE id = 1
ASSERT VALUE department = 'Marketing' WHERE id = 1
ASSERT VALUE city = 'SF' WHERE id = 1
ASSERT VALUE name = 'Priya_1000' WHERE id = 1000
ASSERT VALUE title = 'VP' WHERE id = 1000
ASSERT VALUE department = 'Engineering' WHERE id = 1000
ASSERT VALUE city = 'Seattle' WHERE id = 1000
ASSERT VALUE name = 'Priya_500000' WHERE id = 500000
ASSERT VALUE title = 'VP' WHERE id = 500000
ASSERT VALUE city = 'Tokyo' WHERE id = 500000
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (n)
WHERE n.id IN [1, 1000, 500000]
RETURN n.id AS id, n.name AS name, n.department AS department,
       n.city AS city, n.title AS title, n.level AS level
ORDER BY n.id;


-- ============================================================================
-- 11. KNOWLEDGE PATHS — 2-hop information flow from employee #1
-- ============================================================================
-- Employee #1 has 236 deterministic 2-hop (r1,r2) edge combinations.
-- LIMIT 50 truncates to exactly 50 rows.

ASSERT ROW_COUNT = 50
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1
RETURN a.name AS source, b.name AS relay, c.name AS reached
LIMIT 50;


-- ============================================================================
-- 12. REACHABILITY — Who can employee #1 reach within 2 hops?
-- ============================================================================
-- Employee #1 reaches 125 distinct employees within 2 hops; LIMIT 50
-- truncates to exactly 50 rows.

ASSERT ROW_COUNT = 50
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[*1..2]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
LIMIT 50;


-- ============================================================================
-- 13. RECIPROCAL RELATIONSHIPS — Mutual bonds at scale
-- ============================================================================
-- Most edges flow forward (dst > src) from the deterministic stride-based
-- generation. Only 3 bidirectional (a,b) pairs exist at a.id < b.id, all
-- created by Batch 7's random long-range connections.

ASSERT ROW_COUNT = 3
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN a.name AS person_a, b.name AS person_b,
       r1.relationship_type AS a_to_b, r2.relationship_type AS b_to_a
LIMIT 25;


-- ============================================================================
-- 14. MENTORSHIP LEVEL FLOW — Are seniors mentoring juniors?
-- ============================================================================
-- Batch 4 generates mentor edges from L5+ to same-department subordinates.
-- L6 (8,000 Sr Managers × 30 mentees × dept overlap) → exact level-pair
-- counts. The engine must correctly join edge + vertex tables at scale.

ASSERT ROW_COUNT = 14
ASSERT VALUE mentorship_count = 192000 WHERE mentor_level = 'L6'
ASSERT VALUE mentorship_count = 120000 WHERE mentor_level = 'L5'
ASSERT VALUE mentorship_count = 80000 WHERE mentor_level = 'L8'
ASSERT VALUE mentorship_count = 48000 WHERE mentor_level = 'L7'
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.level AS mentor_level, mentee.level AS mentee_level,
       count(r) AS mentorship_count, avg(r.weight) AS avg_strength
ORDER BY mentorship_count DESC;


-- ============================================================================
-- 15. SENIORITY FLOW — How does information flow across levels?
-- ============================================================================
-- Do senior people mostly connect to other seniors (echo chamber), or
-- do connections span levels?

ASSERT ROW_COUNT = 20
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
RETURN a.level AS from_level, b.level AS to_level,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC
LIMIT 20;


-- ============================================================================
-- 16. TEAM vs CROSS-TEAM — Is the org collaborating or siloed?
-- ============================================================================
-- Batches 1, 2, and 4 preserve department membership (stride 20, 200, 50).
-- Cross-department edges come from Batches 3, 5, 6, 7.
-- Exact split: 3,125,998 within-department + 1,934,000 cross-department
-- = 5,059,998 total. These two queries must sum to the total edge count.

ASSERT ROW_COUNT = 1
ASSERT VALUE connections = 3125998
-- Non-deterministic: float average may vary slightly across engines
ASSERT WARNING VALUE avg_strength BETWEEN 0.7 AND 0.8
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.department = b.department
RETURN 'within_department' AS scope, count(r) AS connections,
       avg(r.weight) AS avg_strength;


-- ============================================================================
-- 17. CROSS-DEPARTMENT VOLUME — Complement to within-department
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE connections = 1934000
-- Non-deterministic: float average may vary slightly across engines
ASSERT WARNING VALUE avg_strength BETWEEN 0.35 AND 0.45
USE {{zone_name}}.stress_test_network.stress_test_network
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
-- CSR topology is pre-built in setup.sql (CREATE GRAPHCSR), so the first
-- algorithm below loads the graph in ~200 ms from the .dcsr sidecar.


-- ============================================================================
-- 18. DEGREE CENTRALITY — Most connected people at 1M scale
-- ============================================================================
-- The most connected nodes are L4+ hub nodes (id%20=0) who also happen
-- to be VPs (id%1000=0). They get edges from Batches 1,2,4,6 plus
-- inbound from all batches. Top total_degree = 194 (out=178, in=16).

ASSERT ROW_COUNT = 25
ASSERT VALUE total_degree = 194
ASSERT VALUE out_degree = 178
ASSERT VALUE in_degree = 16
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 25;


-- ============================================================================
-- 19. PAGERANK — True organizational influence at enterprise scale
-- ============================================================================
-- PageRank at 1M nodes and 5M edges: finds the people who are
-- connected to by other well-connected people. The real power structure.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 5})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 25;


-- ============================================================================
-- 20. NATURAL COMMUNITIES — Connected components at scale
-- ============================================================================
-- The graph is fully connected: weak ties (Batch 7) and bridge nodes
-- (Batch 5) ensure a single giant component containing all 1,000,000
-- employees. Zero isolated nodes.

ASSERT ROW_COUNT = 1
ASSERT VALUE community_size = 1000000
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS community_size
ORDER BY community_size DESC
LIMIT 25;


-- ============================================================================
-- 21. LOUVAIN COMMUNITIES — Real organizational clusters
-- ============================================================================
-- Finds dense subgroups regardless of the formal org chart. Do the
-- detected communities align with the 20 departments?

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS size
ORDER BY size DESC
LIMIT 25;


-- ============================================================================
-- 22. GATEKEEPERS — Who controls information flow at scale?
-- ============================================================================
-- Betweenness centrality at 1M nodes: finds people on many shortest paths.
-- If they leave, communication between groups breaks down. Critical
-- for succession planning and retention strategy.
-- Uses samplingSize for approximate mode (exact is O(n·m), infeasible at 1M).

ASSERT ROW_COUNT = 25
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.betweenness({samplingSize: 1000})
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 25;


-- ============================================================================
-- 23. TIGHT-KNIT GROUPS — Triangle count at scale
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.triangle_count()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC
LIMIT 25;


-- ============================================================================
-- 24. SHORTEST PATH — Route across the enterprise
-- ============================================================================
-- If employee #1 (Marcus_1, Marketing, SF) needs to reach employee #500000
-- (Priya_500000, VP, Engineering, Tokyo), what's the fastest chain?
-- Tests Dijkstra at 1M scale with weighted edges.

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.shortestPath({source: 1, target: 500000})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 25. SIX DEGREES — Small world property at 1M scale
-- ============================================================================
-- Most people should be reachable within 4-6 hops even in an enterprise
-- of 1 million. More suggests organizational fragmentation.

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.bfs({source: 1})
YIELD node_id, depth, parent_id
RETURN depth, count(*) AS people_at_distance
ORDER BY depth
LIMIT 20;


-- ============================================================================
-- 26. DIRECTED REACHABILITY — Strongly connected components
-- ============================================================================
-- A large SCC means good bidirectional communication. Many small SCCs
-- indicate one-way information flow (top-down only).

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.scc()
YIELD node_id, component_id
RETURN component_id, count(*) AS scc_size
ORDER BY scc_size DESC
LIMIT 25;


-- ============================================================================
-- 27. ACCESSIBILITY — Who can reach everyone fastest?
-- ============================================================================
-- High closeness = good candidate for company-wide announcements or
-- change agent roles.
-- Uses samplingSize for approximate mode (exact is O(n·m), infeasible at 1M).

ASSERT ROW_COUNT = 25
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.closeness({samplingSize: 1000})
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 25;


-- ============================================================================
-- 28. BACKBONE NETWORK — Essential connections
-- ============================================================================
-- The minimum spanning tree at 1M scale: exactly 999,999 edges
-- (N-1 for a connected graph of N=1,000,000 nodes).

ASSERT ROW_COUNT = 1
ASSERT VALUE backbone_edges = 999999
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN count(*) AS backbone_edges, sum(weight) AS total_weight;


-- ============================================================================
-- 29. ALL DISTANCES FROM EMPLOYEE #1
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.allShortestPaths({source: 1})
YIELD node_id, distance, path
RETURN node_id, distance
ORDER BY distance
LIMIT 50;


-- ============================================================================
-- 30. DEPTH-FIRST EXPLORATION — Trace influence chains
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.stress_test_network.stress_test_network
CALL algo.dfs({source: 1})
YIELD node_id, discovery_time, finish_time, parent_id
RETURN node_id, discovery_time, finish_time, parent_id
ORDER BY discovery_time
LIMIT 50;


-- ============================================================================
-- 31. NEAREST NEIGHBORS — Structural similarity at scale
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.stress_test_network.stress_test_network
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
-- All edge counts are independently verified via DuckDB against the
-- same deterministic edge-generation formulas.
--
--   32.  Single team (~100 nodes)        — should render instantly
--   33.  Small department slice (500)     — fast render
--   34.  Full department unit (1,000)     — smooth render
--   35.  Multi-team view (5,000)          — may need layout time
--   36.  Division-level (10,000)          — stress test begins
--   37.  Regional network (50,000)        — expect lag
--   38.  Large region (100,000)           — browser stress
--   39.  Full organization (1M + 5M)      — ultimate stress test
--
-- Node-only rendering:
--   40.  100 nodes    — node layout test
--   41.  1,000 nodes  — medium density
--   42.  10,000 nodes — large node cloud
--   43.  All 1M nodes — extreme test
-- ############################################################################


-- ============================================================================
-- 32. VIZ: SINGLE PROJECT TEAM — ~100 person team network
-- ============================================================================
-- Edges among ids 1-100: 389 edges from Batches 1-7 where both
-- endpoints fall within this range.

ASSERT ROW_COUNT = 389
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 100 AND b.id <= 100
RETURN a, r, b;


-- ============================================================================
-- 33. VIZ: DEPARTMENT SLICE — 500 employees
-- ============================================================================

ASSERT ROW_COUNT = 3217
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 500 AND b.id <= 500
RETURN a, r, b;


-- ============================================================================
-- 34. VIZ: FULL DEPARTMENT — 1,000 employees
-- ============================================================================

ASSERT ROW_COUNT = 7377
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 1000 AND b.id <= 1000
RETURN a, r, b;


-- ============================================================================
-- 35. VIZ: MULTI-TEAM — 5,000 employees
-- ============================================================================

ASSERT ROW_COUNT = 41095
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 5000 AND b.id <= 5000
RETURN a, r, b;


-- ============================================================================
-- 36. VIZ: DIVISION — 10,000 employees, stress test begins
-- ============================================================================

ASSERT ROW_COUNT = 83323
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 10000 AND b.id <= 10000
RETURN a, r, b;


-- ============================================================================
-- 37. VIZ: REGIONAL — 50,000 employees, extreme rendering
-- ============================================================================
-- WARNING: Large result set. The visualizer may become sluggish.

ASSERT ROW_COUNT = 421693
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 50000 AND b.id <= 50000
RETURN a, r, b;


-- ============================================================================
-- 38. VIZ: LARGE REGION — 100,000 employees, browser stress
-- ============================================================================
-- WARNING: Expect significant lag or memory pressure.

ASSERT ROW_COUNT = 846096
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
WHERE a.id <= 100000 AND b.id <= 100000
RETURN a, r, b;


-- ============================================================================
-- 39. VIZ: FULL ORGANIZATION — All 1M employees + 5M connections
-- ============================================================================
-- WARNING: Ultimate stress test. The visualizer will likely freeze.

ASSERT ROW_COUNT = 5059998
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 40. VIZ NODES: SINGLE TEAM — 100 employee nodes
-- ============================================================================

ASSERT ROW_COUNT = 100
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (n)
WHERE n.id <= 100
RETURN n;


-- ============================================================================
-- 41. VIZ NODES: DEPARTMENT — 1,000 employee nodes
-- ============================================================================

ASSERT ROW_COUNT = 1000
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (n)
WHERE n.id <= 1000
RETURN n;


-- ============================================================================
-- 42. VIZ NODES: DIVISION — 10,000 employee nodes
-- ============================================================================

ASSERT ROW_COUNT = 10000
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (n)
WHERE n.id <= 10000
RETURN n;


-- ============================================================================
-- 43. VIZ NODES: FULL ORGANIZATION — All 1M employee nodes
-- ============================================================================
-- WARNING: Returns 1,000,000 node objects. Ultimate node rendering test.

ASSERT ROW_COUNT = 1000000
USE {{zone_name}}.stress_test_network.stress_test_network
MATCH (n)
RETURN n;


-- ============================================================================
-- VERIFY 1: Node count, department count, city count
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_people = 1000000
ASSERT VALUE dept_count = 20
ASSERT VALUE city_count = 15
SELECT
    COUNT(*)                   AS total_people,
    COUNT(DISTINCT department) AS dept_count,
    COUNT(DISTINCT city)       AS city_count
FROM {{zone_name}}.stress_test_network.st_people;


-- ============================================================================
-- VERIFY 2: Total edge count across all 7 batches
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_edges = 5059998
SELECT COUNT(*) AS total_edges
FROM {{zone_name}}.stress_test_network.st_edges;


-- ============================================================================
-- VERIFY 3: Uniform department headcount (1M / 20 = 50000 per dept)
-- ============================================================================

ASSERT ROW_COUNT = 20
ASSERT VALUE headcount = 50000 WHERE department = 'Engineering'
ASSERT VALUE headcount = 50000 WHERE department = 'AI/ML'
SELECT department, COUNT(*) AS headcount
FROM {{zone_name}}.stress_test_network.st_people
GROUP BY department
ORDER BY department;


-- ============================================================================
-- VERIFY 4: Title hierarchy (7 levels)
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE cnt = 800000 WHERE title = 'Associate'
ASSERT VALUE cnt = 140000 WHERE title = 'Engineer'
ASSERT VALUE cnt = 40000 WHERE title = 'Senior Engineer'
ASSERT VALUE cnt = 10000 WHERE title = 'Manager'
ASSERT VALUE cnt = 8000 WHERE title = 'Senior Manager'
ASSERT VALUE cnt = 1000 WHERE title = 'Director'
ASSERT VALUE cnt = 1000 WHERE title = 'VP'
SELECT title, COUNT(*) AS cnt
FROM {{zone_name}}.stress_test_network.st_people
GROUP BY title
ORDER BY cnt DESC;


-- ============================================================================
-- VERIFY 5: Level distribution (8 levels)
-- ============================================================================

ASSERT ROW_COUNT = 8
ASSERT VALUE cnt = 533333 WHERE level = 'L1'
ASSERT VALUE cnt = 266667 WHERE level = 'L2'
ASSERT VALUE cnt = 140000 WHERE level = 'L3'
ASSERT VALUE cnt = 40000 WHERE level = 'L4'
ASSERT VALUE cnt = 10000 WHERE level = 'L5'
ASSERT VALUE cnt = 8000 WHERE level = 'L6'
ASSERT VALUE cnt = 1000 WHERE level = 'L7'
ASSERT VALUE cnt = 1000 WHERE level = 'L8'
SELECT level, COUNT(*) AS cnt
FROM {{zone_name}}.stress_test_network.st_people
GROUP BY level
ORDER BY level;


-- ============================================================================
-- VERIFY 6: Active/Inactive split (active = id % 21 != 0)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE active_count = 952381
ASSERT VALUE inactive_count = 47619
SELECT
    SUM(CASE WHEN active = true THEN 1 ELSE 0 END)  AS active_count,
    SUM(CASE WHEN active = false THEN 1 ELSE 0 END) AS inactive_count
FROM {{zone_name}}.stress_test_network.st_people;


-- ============================================================================
-- VERIFY 7: Relationship type breakdown (18 types)
-- ============================================================================

ASSERT ROW_COUNT = 18
ASSERT VALUE cnt = 750000 WHERE relationship_type = 'colleague'
ASSERT VALUE cnt = 750000 WHERE relationship_type = 'teammate'
ASSERT VALUE cnt = 550000 WHERE relationship_type = 'mentor'
ASSERT VALUE cnt = 333334 WHERE relationship_type = 'project-mate'
ASSERT VALUE cnt = 200000 WHERE relationship_type = 'city-social'
ASSERT VALUE cnt = 163335 WHERE relationship_type = 'strategic-partner'
ASSERT VALUE cnt = 163334 WHERE relationship_type = 'leadership-network'
ASSERT VALUE cnt = 163331 WHERE relationship_type = 'executive-link'
ASSERT VALUE cnt = 160000 WHERE relationship_type = 'alumni-connection'
ASSERT VALUE cnt = 159998 WHERE relationship_type = 'acquaintance'
ASSERT VALUE cnt = 133334 WHERE relationship_type = 'inter-team-link'
ASSERT VALUE cnt = 133333 WHERE relationship_type = 'cross-dept-bridge'
ASSERT VALUE cnt = 133333 WHERE relationship_type = 'liaison'
SELECT relationship_type, COUNT(*) AS cnt
FROM {{zone_name}}.stress_test_network.st_edges
GROUP BY relationship_type
ORDER BY cnt DESC;


-- ============================================================================
-- VERIFY 8: Within vs cross department edge split
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE within_dept = 3125998
ASSERT VALUE cross_dept = 1934000
ASSERT VALUE total_check = 5059998
SELECT
    SUM(CASE WHEN s.department = d.department THEN 1 ELSE 0 END) AS within_dept,
    SUM(CASE WHEN s.department != d.department THEN 1 ELSE 0 END) AS cross_dept,
    COUNT(*) AS total_check
FROM {{zone_name}}.stress_test_network.st_edges e
JOIN {{zone_name}}.stress_test_network.st_people s ON e.src = s.id
JOIN {{zone_name}}.stress_test_network.st_people d ON e.dst = d.id;
