-- ============================================================================
-- Graph JSON Mode — Cypher Queries
-- ============================================================================
-- Demonstrates graph analytics using Cypher on JSON property tables.
-- All vertex/edge properties are stored in a single JSON string column.
-- The Cypher engine extracts properties transparently — the query syntax
-- is identical to flattened or hybrid mode. JSON mode provides maximum
-- schema flexibility at the cost of JSON parsing on every access.
-- ============================================================================


-- ============================================================================
-- PART 1: EXPLORE THE ORGANIZATION
-- ============================================================================


-- ============================================================================
-- 1. MEET THE TEAM — Browse all 50 employees
-- ============================================================================
-- Properties are stored as JSON blobs, but Cypher accesses them the same
-- way as flat columns: n.name, n.department, etc. The engine handles
-- JSON extraction automatically.

ASSERT ROW_COUNT = 50
ASSERT VALUE city = 'SF' WHERE name = 'Priya_1'
ASSERT VALUE dept = 'Marketing' WHERE name = 'Priya_1'
ASSERT VALUE age = 43 WHERE name = 'Priya_1'
USE {{zone_name}}.graph.json_demo
MATCH (n)
RETURN n.name AS name, n.age AS age, n.department AS dept,
       n.city AS city, n.title AS title, n.level AS level
ORDER BY n.department, n.name;


-- ============================================================================
-- 2. HEADCOUNT BY DEPARTMENT — Workforce distribution
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 10 WHERE department = 'Engineering'
ASSERT VALUE headcount = 10 WHERE department = 'Marketing'
ASSERT VALUE headcount = 10 WHERE department = 'Sales'
USE {{zone_name}}.graph.json_demo
MATCH (n)
RETURN n.department AS department, count(n) AS headcount,
       avg(n.age) AS avg_age
ORDER BY headcount DESC;


-- ============================================================================
-- 3. FIND ENGINEERING — Department filter
-- ============================================================================
-- In JSON mode, this filter is applied after JSON extraction rather than
-- pushed down to storage. Flattened mode would be faster here, but the
-- query syntax is identical.

ASSERT ROW_COUNT = 10
ASSERT VALUE title = 'Director' WHERE name = 'Luca_10'
ASSERT VALUE city = 'Chicago' WHERE name = 'Luca_10'
USE {{zone_name}}.graph.json_demo
MATCH (n)
WHERE n.department = 'Engineering'
RETURN n.name AS name, n.age AS age, n.city AS city, n.title AS title
ORDER BY n.age DESC;


-- ============================================================================
-- 4. COMPANY NETWORK — Visualize all connections
-- ============================================================================

ASSERT ROW_COUNT = 189
USE {{zone_name}}.graph.json_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- PART 2: RELATIONSHIP ANALYSIS
-- ============================================================================


-- ============================================================================
-- 5. MENTORSHIP MAP — Who is coaching whom?
-- ============================================================================

ASSERT ROW_COUNT = 25
ASSERT VALUE bond_strength = 0.99 WHERE mentor = 'Wei_15'
USE {{zone_name}}.graph.json_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.name AS mentor, mentor.title AS mentor_title,
       mentor.department AS dept, mentee.name AS mentee,
       mentee.title AS mentee_title, r.weight AS bond_strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 6. VISUALIZE MENTORSHIPS — Coaching hierarchy
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.json_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor, r, mentee;


-- ============================================================================
-- 7. STRONGEST BONDS — High-impact relationships
-- ============================================================================

ASSERT ROW_COUNT = 53
ASSERT VALUE strength = 1.0 WHERE person_a = 'Luca_30'
USE {{zone_name}}.graph.json_demo
MATCH (a)-[r]->(b)
WHERE r.weight > 0.8
RETURN a.name AS person_a, b.name AS person_b,
       r.relationship_type AS type, r.weight AS strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Who connects the silos?
-- ============================================================================

ASSERT ROW_COUNT = 80
USE {{zone_name}}.graph.json_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- 9. DEPARTMENT CONNECTIVITY — Which teams collaborate?
-- ============================================================================

ASSERT ROW_COUNT = 14
ASSERT VALUE connections = 10 WHERE from_dept = 'HR'
USE {{zone_name}}.graph.json_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC;


-- ============================================================================
-- 10. RECIPROCAL BONDS — Genuine two-way relationships
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE person_a = 'Wei_15' WHERE a_to_b = 'mentor'
ASSERT VALUE a_to_b_weight = 0.99 WHERE a_to_b = 'mentor'
USE {{zone_name}}.graph.json_demo
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN a.name AS person_a, b.name AS person_b,
       r1.relationship_type AS a_to_b, r2.relationship_type AS b_to_a,
       r1.weight AS a_to_b_weight, r2.weight AS b_to_a_weight
ORDER BY r1.weight + r2.weight DESC;


-- ============================================================================
-- PART 3: NETWORK TRAVERSAL
-- ============================================================================


-- ============================================================================
-- 11. FRIENDS OF FRIENDS — 2-hop information flow
-- ============================================================================

ASSERT ROW_COUNT = 21
ASSERT VALUE relay_dept = 'Engineering' WHERE relay = 'Wei_5'
USE {{zone_name}}.graph.json_demo
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1 AND a <> c
RETURN a.name AS source, b.name AS relay, c.name AS reached,
       b.department AS relay_dept, c.department AS reached_dept;


-- ============================================================================
-- 12. REACHABILITY — Who can person #1 reach within 3 hops?
-- ============================================================================

ASSERT ROW_COUNT = 27
USE {{zone_name}}.graph.json_demo
MATCH (a)-[*1..3]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
ORDER BY b.name;


-- ============================================================================
-- PART 4: GRAPH ALGORITHMS
-- ============================================================================


-- ============================================================================
-- 13. PAGERANK — Informal influencers
-- ============================================================================

ASSERT ROW_COUNT = 50
-- Non-deterministic: PageRank scores are floating-point and implementation-dependent; only structural count is asserted
USE {{zone_name}}.graph.json_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 14. DEGREE CENTRALITY — Connection counts
-- ============================================================================

ASSERT ROW_COUNT = 50
ASSERT VALUE total_degree = 6 WHERE node_id = 1
USE {{zone_name}}.graph.json_demo
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC;


-- ============================================================================
-- 15. GATEKEEPERS — Betweenness centrality
-- ============================================================================

ASSERT ROW_COUNT = 50
-- Non-deterministic: betweenness centrality scores are floating-point; only structural count is asserted
USE {{zone_name}}.graph.json_demo
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 16. NATURAL TEAMS — Louvain community detection
-- ============================================================================

-- Non-deterministic: Louvain community count varies by random initialization; range assertion used
ASSERT WARNING ROW_COUNT >= 2
USE {{zone_name}}.graph.json_demo
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, collect(node_id) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 17. TIGHT-KNIT GROUPS — Triangle count
-- ============================================================================

ASSERT ROW_COUNT = 50
USE {{zone_name}}.graph.json_demo
CALL algo.triangle_count()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC;


-- ============================================================================
-- 18. SHORTEST PATH — Route a message across the company
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE node_id = 1 WHERE step = 0
ASSERT VALUE node_id = 42 WHERE step = 4
USE {{zone_name}}.graph.json_demo
CALL algo.shortestPath({source: 1, target: 42})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 19. SIX DEGREES — How far apart is everyone?
-- ============================================================================

ASSERT ROW_COUNT = 8
ASSERT VALUE people_at_distance = 1 WHERE depth = 0
ASSERT VALUE people_at_distance = 13 WHERE depth = 3
USE {{zone_name}}.graph.json_demo
CALL algo.bfs({source: 1})
YIELD node_id, depth, parent_id
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ============================================================================
-- PART 5: VISUALIZATION
-- ============================================================================


-- ============================================================================
-- 20. FULL COMPANY GRAPH
-- ============================================================================

USE {{zone_name}}.graph.json_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 21. MENTORSHIP HIERARCHY
-- ============================================================================

USE {{zone_name}}.graph.json_demo
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor'
RETURN a, r, b;


-- ============================================================================
-- 22. CROSS-DEPARTMENT BRIDGES
-- ============================================================================

USE {{zone_name}}.graph.json_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: all 50 nodes loaded, bridge node 13 has expected
-- total degree of 19 (out=14, in=5), and node 1 has total degree of 6.
-- Confirms full graph topology and edge counts are correct.

ASSERT ROW_COUNT = 50
ASSERT VALUE total_degree = 19 WHERE node_id = 13
ASSERT VALUE total_degree = 6 WHERE node_id = 1
USE {{zone_name}}.graph.json_demo
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC;
