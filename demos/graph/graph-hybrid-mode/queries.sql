-- ============================================================================
-- Graph Hybrid Mode — Cypher Queries
-- ============================================================================
-- Demonstrates graph analytics using Cypher on HYBRID property tables.
-- Core properties (name, age, weight, relationship_type) are direct columns
-- while extended properties live in JSON. The Cypher engine accesses all
-- properties transparently — the same query syntax works regardless of
-- the underlying storage strategy.
-- ============================================================================


-- ============================================================================
-- PART 1: EXPLORE THE ORGANIZATION
-- ============================================================================


-- ============================================================================
-- 1. MEET THE TEAM — Browse all 50 employees
-- ============================================================================
-- The Cypher engine reads core columns (name, age) at full speed and
-- transparently extracts extended properties (department, city, title)
-- from the JSON extras column.

ASSERT ROW_COUNT = 50
ASSERT VALUE city = 'SF' WHERE name = 'Alice_1'
ASSERT VALUE dept = 'Marketing' WHERE name = 'Alice_1'
ASSERT VALUE age = 43 WHERE name = 'Alice_1'
USE {{zone_name}}.graph.hybrid_demo
MATCH (n)
RETURN n.name AS name, n.age AS age, n.department AS dept,
       n.city AS city, n.title AS title, n.level AS level
ORDER BY n.department, n.name;


-- ============================================================================
-- 2. EXPERIENCED EMPLOYEES — Filter on core column (age)
-- ============================================================================
-- Age is a core column — this predicate pushes down directly to storage
-- for maximum performance, even though the query also reads JSON properties.

ASSERT ROW_COUNT = 31
ASSERT VALUE dept = 'Marketing' WHERE name = 'Alice_1'
USE {{zone_name}}.graph.hybrid_demo
MATCH (n)
WHERE n.age > 35
RETURN n.name AS name, n.age AS age, n.department AS dept,
       n.title AS title
ORDER BY n.age DESC;


-- ============================================================================
-- 3. HEADCOUNT BY DEPARTMENT — Workforce distribution
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 10 WHERE department = 'Engineering'
ASSERT VALUE headcount = 10 WHERE department = 'Marketing'
ASSERT VALUE headcount = 10 WHERE department = 'Sales'
USE {{zone_name}}.graph.hybrid_demo
MATCH (n)
RETURN n.department AS department, count(n) AS headcount,
       avg(n.age) AS avg_age
ORDER BY headcount DESC;


-- ============================================================================
-- 4. COMPANY NETWORK — Visualize all connections
-- ============================================================================

ASSERT ROW_COUNT = 189
USE {{zone_name}}.graph.hybrid_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- PART 2: RELATIONSHIP ANALYSIS
-- ============================================================================


-- ============================================================================
-- 5. MENTORSHIP MAP — Who is coaching whom?
-- ============================================================================
-- relationship_type is a core edge column — filtering is fast. The
-- Cypher engine enriches results with JSON-stored properties (title,
-- department) transparently.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.hybrid_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.name AS mentor, mentor.title AS mentor_title,
       mentor.department AS dept, mentee.name AS mentee,
       mentee.title AS mentee_title, r.weight AS bond_strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 6. VISUALIZE MENTORSHIPS — Coaching hierarchy
-- ============================================================================

USE {{zone_name}}.graph.hybrid_demo
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor, r, mentee;


-- ============================================================================
-- 7. STRONGEST BONDS — High-impact relationships
-- ============================================================================
-- Weight is a core column, so this filter pushes down to storage.

ASSERT ROW_COUNT = 53
USE {{zone_name}}.graph.hybrid_demo
MATCH (a)-[r]->(b)
WHERE r.weight > 0.8
RETURN a.name AS person_a, b.name AS person_b,
       r.relationship_type AS type, r.weight AS strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Who connects the silos?
-- ============================================================================

ASSERT ROW_COUNT = 80
USE {{zone_name}}.graph.hybrid_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- 9. DEPARTMENT CONNECTIVITY — Which teams collaborate?
-- ============================================================================

ASSERT ROW_COUNT = 14
ASSERT VALUE connections >= 3 WHERE from_dept = 'Engineering'
USE {{zone_name}}.graph.hybrid_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC;


-- ============================================================================
-- 10. RECIPROCAL BONDS — Genuine two-way relationships
-- ============================================================================

ASSERT ROW_COUNT = 2
USE {{zone_name}}.graph.hybrid_demo
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
USE {{zone_name}}.graph.hybrid_demo
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1 AND a <> c
RETURN a.name AS source, b.name AS relay, c.name AS reached,
       b.department AS relay_dept, c.department AS reached_dept;


-- ============================================================================
-- 12. REACHABILITY — Who can person #1 reach within 3 hops?
-- ============================================================================

ASSERT ROW_COUNT = 27
USE {{zone_name}}.graph.hybrid_demo
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

-- Non-deterministic: PageRank scores vary by algorithm implementation; only row count is asserted
ASSERT ROW_COUNT = 50
USE {{zone_name}}.graph.hybrid_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 14. DEGREE CENTRALITY — Connection counts
-- ============================================================================

ASSERT ROW_COUNT = 50
ASSERT VALUE total_degree = 6 WHERE node_id = 1
USE {{zone_name}}.graph.hybrid_demo
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC;


-- ============================================================================
-- 15. GATEKEEPERS — Betweenness centrality
-- ============================================================================

-- Non-deterministic: Betweenness centrality scores vary by algorithm implementation; only row count is asserted
ASSERT ROW_COUNT = 50
USE {{zone_name}}.graph.hybrid_demo
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 16. NATURAL TEAMS — Louvain community detection
-- ============================================================================

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.graph.hybrid_demo
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, collect(node_id) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 17. TIGHT-KNIT GROUPS — Triangle count
-- ============================================================================

ASSERT ROW_COUNT = 50
USE {{zone_name}}.graph.hybrid_demo
CALL algo.triangle_count()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC;


-- ============================================================================
-- 18. SHORTEST PATH — Route a message across the company
-- ============================================================================

ASSERT ROW_COUNT >= 2
ASSERT VALUE node_id = 1 WHERE step = 0
ASSERT VALUE distance = 0 WHERE step = 0
USE {{zone_name}}.graph.hybrid_demo
CALL algo.shortestPath({source: 1, target: 42})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 19. SIX DEGREES — How far apart is everyone?
-- ============================================================================

ASSERT ROW_COUNT >= 1
ASSERT VALUE people_at_distance = 1 WHERE depth = 0
USE {{zone_name}}.graph.hybrid_demo
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

USE {{zone_name}}.graph.hybrid_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 21. MENTORSHIP HIERARCHY
-- ============================================================================

USE {{zone_name}}.graph.hybrid_demo
MATCH (a)-[r]->(b)
WHERE r.relationship_type = 'mentor'
RETURN a, r, b;


-- ============================================================================
-- 22. CROSS-DEPARTMENT BRIDGES
-- ============================================================================

USE {{zone_name}}.graph.hybrid_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: 50 employees accessible via hybrid mode, core
-- column (age) and JSON-extracted properties (department, city) both readable,
-- proving the hybrid storage strategy exposes all properties transparently.

ASSERT ROW_COUNT = 50
ASSERT VALUE age = 43 WHERE name = 'Alice_1'
ASSERT VALUE dept = 'Marketing' WHERE name = 'Alice_1'
ASSERT VALUE city = 'SF' WHERE name = 'Alice_1'
USE {{zone_name}}.graph.hybrid_demo
MATCH (n)
RETURN n.name AS name, n.age AS age, n.department AS dept, n.city AS city
ORDER BY n.name;

ASSERT ROW_COUNT = 189
USE {{zone_name}}.graph.hybrid_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;
