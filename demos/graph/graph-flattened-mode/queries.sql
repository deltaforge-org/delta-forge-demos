-- ============================================================================
-- Graph Flattened Mode — Cypher Queries
-- ============================================================================
-- All queries use Cypher to demonstrate graph analytics on FLATTENED
-- property tables. The Cypher engine accesses vertex/edge properties
-- transparently — whether stored as flat columns or JSON, the query
-- syntax is the same. Flattened mode provides the fastest property
-- access with full predicate pushdown.
-- ============================================================================


-- ============================================================================
-- PART 1: EXPLORE THE ORGANIZATION
-- ============================================================================


-- ============================================================================
-- 1. MEET THE TEAM — Browse all 50 employees
-- ============================================================================
-- Returns every employee with their key properties. In flattened mode,
-- each property is a direct column — the graph engine reads them without
-- any JSON extraction overhead.

ASSERT ROW_COUNT = 50
ASSERT VALUE city = 'SF' WHERE name = 'Priya_1'
ASSERT VALUE dept = 'Marketing' WHERE name = 'Priya_1'
ASSERT VALUE age = 43 WHERE name = 'Priya_1'
USE {{zone_name}}.graph.flattened_demo
MATCH (n)
RETURN n.name AS name, n.department AS dept, n.title AS title,
       n.city AS city, n.level AS level, n.age AS age
ORDER BY n.department, n.name;


-- ============================================================================
-- 2. HEADCOUNT BY DEPARTMENT — Workforce distribution
-- ============================================================================
-- Aggregations on node properties. How many people are in each department
-- and what's the average age? Identifies teams that may need hiring.

ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 10 WHERE department = 'Engineering'
ASSERT VALUE headcount = 10 WHERE department = 'Marketing'
ASSERT VALUE headcount = 10 WHERE department = 'HR'
ASSERT VALUE headcount = 10 WHERE department = 'Finance'
ASSERT VALUE headcount = 10 WHERE department = 'Sales'
USE {{zone_name}}.graph.flattened_demo
MATCH (n)
RETURN n.department AS department, count(n) AS headcount,
       avg(n.age) AS avg_age
ORDER BY headcount DESC;


-- ============================================================================
-- 3. SENIOR STAFF — Find experienced employees ready to mentor
-- ============================================================================
-- Active senior staff (L3+) could lead mentorship programs. Property
-- predicates push down to storage for fast filtering in flattened mode.

ASSERT ROW_COUNT = 20
USE {{zone_name}}.graph.flattened_demo
MATCH (n)
WHERE n.active = true AND n.level IN ['L3', 'L4', 'L5']
RETURN n.name AS name, n.department AS dept, n.city AS city,
       n.title AS title, n.level AS level
ORDER BY n.level DESC, n.name;


-- ============================================================================
-- 4. COMPANY NETWORK — Visualize all connections
-- ============================================================================
-- Full graph visualization: 50 people and ~150 connections. Department
-- clusters should appear as dense groups with bridge edges between them.

ASSERT ROW_COUNT = 189
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- PART 2: RELATIONSHIP ANALYSIS
-- ============================================================================


-- ============================================================================
-- 5. MENTORSHIP MAP — Who is coaching whom?
-- ============================================================================
-- The formal mentorship structure. Strong bonds (high weight) indicate
-- effective mentoring. Cross-level mentorship (L5→L2) is more valuable
-- than peer mentoring (L4→L4).

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.flattened_demo
MATCH (mentor)-[r:mentor]->(mentee)
RETURN mentor.name AS mentor, mentor.title AS mentor_title,
       mentor.department AS dept, mentee.name AS mentee,
       mentee.title AS mentee_title, r.weight AS bond_strength,
       r.since_year AS since, r.frequency AS freq, r.rating AS rating
ORDER BY r.weight DESC;


-- ============================================================================
-- 6. VISUALIZE MENTORSHIPS — See the coaching hierarchy
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.flattened_demo
MATCH (mentor)-[r:mentor]->(mentee)
RETURN mentor, r, mentee;


-- ============================================================================
-- 7. STRONGEST CONNECTIONS — The backbone relationships
-- ============================================================================
-- High-weight edges (> 0.8) are the relationships that hold the org
-- together. Losing one of these is like cutting a load-bearing beam.
-- Edge properties from flattened columns are accessed directly via r.property.

ASSERT ROW_COUNT = 53
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
WHERE r.weight > 0.8
RETURN a.name AS person_a, b.name AS person_b,
       r.relationship_type AS type, r.weight AS strength,
       r.since_year AS since, r.frequency AS freq, r.context AS context
ORDER BY r.weight DESC;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Preventing organizational silos
-- ============================================================================
-- Show only edges between different departments. These bridge employees
-- are critical for cross-team collaboration and knowledge sharing.

ASSERT ROW_COUNT = 80
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- 9. DEPARTMENT CONNECTIVITY — Which teams talk to each other?
-- ============================================================================
-- Before a reorg, leadership needs to know which departments are already
-- collaborating. High connection counts suggest natural alignment.

ASSERT ROW_COUNT = 14
ASSERT VALUE connections = 10 WHERE from_dept = 'HR'
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC;


-- ============================================================================
-- 10. RELATIONSHIP TYPES — What kinds of bonds exist?
-- ============================================================================
-- Understanding the relationship type mix reveals organizational health.
-- Both type(r) and r.relationship_type access the edge type column.
-- r.context accesses a flattened edge property column directly.

ASSERT ROW_COUNT = 11
ASSERT VALUE count = 25 WHERE type = 'mentor'
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
RETURN r.relationship_type AS type, r.context AS context,
       count(r) AS count, avg(r.weight) AS avg_weight
ORDER BY count DESC;


-- ============================================================================
-- 11. RECIPROCAL BONDS — Where are mutual relationships?
-- ============================================================================
-- When A connects to B AND B connects back to A, the relationship is
-- genuinely collaborative. High mutual count = healthy team culture.

ASSERT ROW_COUNT = 2
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN a.name AS person_a, b.name AS person_b,
       r1.relationship_type AS a_to_b, r2.relationship_type AS b_to_a,
       r1.weight AS a_to_b_weight, r2.weight AS b_to_a_weight
ORDER BY r1.weight + r2.weight DESC;


-- ============================================================================
-- 12. CITY-BASED COLLABORATION — Cross-department bonds within offices
-- ============================================================================
-- People in the same city but different departments form social bridges
-- that prevent the org from becoming siloed by department alone.

ASSERT ROW_COUNT = 39
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
WHERE a.city = b.city AND a.department <> b.department
RETURN a.city AS city, a.department AS from_dept,
       b.department AS to_dept, count(r) AS connections,
       avg(r.weight) AS avg_weight
ORDER BY connections DESC;


-- ============================================================================
-- PART 3: NETWORK TRAVERSAL
-- ============================================================================


-- ============================================================================
-- 13. FRIENDS OF FRIENDS — 2-hop information flow
-- ============================================================================
-- If person #1 shares important news, who hears it directly and who
-- hears it through the grapevine? Shows the relay chain.

ASSERT ROW_COUNT = 21
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1 AND a <> c
RETURN a.name AS source, b.name AS relay, c.name AS reached,
       b.department AS relay_dept, c.department AS reached_dept;


-- ============================================================================
-- 14. REACHABILITY — Who can person #1 reach within 3 hops?
-- ============================================================================

ASSERT ROW_COUNT = 27
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[*1..3]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
ORDER BY b.name;


-- ============================================================================
-- 15. ENGINEERING SUBGRAPH — Internal team collaboration
-- ============================================================================

ASSERT ROW_COUNT = 45
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department = 'Engineering' AND b.department = 'Engineering'
RETURN a, r, b;


-- ============================================================================
-- PART 4: GRAPH ALGORITHMS
-- ============================================================================


-- ============================================================================
-- 16. PAGERANK — Who are the informal influencers?
-- ============================================================================
-- PageRank finds nodes referenced by other well-connected nodes.
-- Directors and bridge nodes should rank highest.

ASSERT ROW_COUNT = 50
USE {{zone_name}}.graph.flattened_demo
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 17. DEGREE CENTRALITY — Connection counts from the graph engine
-- ============================================================================

ASSERT ROW_COUNT = 50
ASSERT VALUE total_degree = 6 WHERE node_id = 1
USE {{zone_name}}.graph.flattened_demo
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC;


-- ============================================================================
-- 18. GATEKEEPERS — Who controls information flow?
-- ============================================================================

ASSERT ROW_COUNT = 50
USE {{zone_name}}.graph.flattened_demo
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 19. NATURAL TEAMS — Louvain community detection
-- ============================================================================
-- Should find ~5 communities matching the department structure, plus
-- possible sub-clusters from city-based bonds.

-- Non-deterministic: Louvain community count varies with resolution parameter and graph structure
ASSERT WARNING ROW_COUNT >= 2
USE {{zone_name}}.graph.flattened_demo
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, collect(node_id) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 20. IS EVERYONE CONNECTED? — Connected components
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE size = 50 WHERE size = 50
USE {{zone_name}}.graph.flattened_demo
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 21. TIGHT-KNIT GROUPS — Triangle count
-- ============================================================================

ASSERT ROW_COUNT = 50
USE {{zone_name}}.graph.flattened_demo
CALL algo.triangle_count()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC;


-- ============================================================================
-- 22. SHORTEST PATH — Fastest route between two employees
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE node_id = 1 WHERE step = 0
ASSERT VALUE distance = 0 WHERE step = 0
USE {{zone_name}}.graph.flattened_demo
CALL algo.shortestPath({source: 1, target: 42})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 23. SIX DEGREES — How many hops apart are people?
-- ============================================================================

ASSERT ROW_COUNT = 8
ASSERT VALUE people_at_distance = 1 WHERE depth = 0
USE {{zone_name}}.graph.flattened_demo
CALL algo.bfs({source: 1})
YIELD node_id, depth, parent_id
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ============================================================================
-- PART 5: VISUALIZATION
-- ============================================================================


-- ============================================================================
-- 24. FULL COMPANY GRAPH — All 50 people and ~150 edges
-- ============================================================================

ASSERT ROW_COUNT = 189
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 25. MENTORSHIP HIERARCHY
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r:mentor]->(b)
RETURN a, r, b;


-- ============================================================================
-- 26. ENGINEERING DEPARTMENT SUBGRAPH
-- ============================================================================

ASSERT ROW_COUNT = 45
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department = 'Engineering' AND b.department = 'Engineering'
RETURN a, r, b;


-- ============================================================================
-- 27. CROSS-DEPARTMENT BRIDGES ONLY
-- ============================================================================
-- Strips away intra-department edges to reveal the bridges that prevent silos.

ASSERT ROW_COUNT = 80
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total nodes, headcount per department,
-- active senior staff, total edges, mentor edges, and cross-dept bridge count.

ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 10 WHERE department = 'Marketing'
USE {{zone_name}}.graph.flattened_demo
MATCH (n)
RETURN n.department AS department, count(n) AS headcount
ORDER BY department;

ASSERT ROW_COUNT = 20
USE {{zone_name}}.graph.flattened_demo
MATCH (n)
WHERE n.active = true AND n.level IN ['L3', 'L4', 'L5']
RETURN n.name AS name, n.level AS level, n.department AS dept
ORDER BY n.level DESC, n.name;

ASSERT ROW_COUNT = 189
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
RETURN a, r, b;

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r:mentor]->(b)
RETURN a, r, b;

ASSERT ROW_COUNT = 80
USE {{zone_name}}.graph.flattened_demo
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;
