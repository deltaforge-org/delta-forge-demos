-- ############################################################################
-- ############################################################################
--
--   STARTUP SOCIAL NETWORK — 100 EMPLOYEES / ~300 CONNECTIONS
--   Organizational Network Analytics via Cypher
--
-- ############################################################################
-- ############################################################################
--
-- This demo models a 100-person startup with 8 departments across 5 cities.
-- The graph has realistic community structure: departments form tight
-- clusters, cities create cross-department bonds, and a few bridge
-- employees connect the organizational silos.
--
-- PART 1: EXPLORE & ANALYZE (queries 1–14)
--   Pattern matching, property filtering, relationship analysis.
--
-- PART 2: GRAPH ALGORITHMS (queries 15–24)
--   Influence ranking, community detection, path analysis.
--
-- PART 3: GRAPH VISUALIZATION (queries 25–27)
--   Visual exploration of the company network.
--
-- ############################################################################


-- ############################################################################
-- PART 1: EXPLORE & ANALYZE
-- ############################################################################


-- ============================================================================
-- 1. MEET THE TEAM — All 100 employees with their roles
-- ============================================================================
-- The CEO wants a quick overview of who works here. Department, title,
-- city, and seniority level give a complete picture.

ASSERT ROW_COUNT = 100
ASSERT VALUE dept = 'Marketing' WHERE name = 'Marcus_1'
ASSERT VALUE city = 'SF' WHERE name = 'Marcus_1'
USE {{zone_name}}.graph.social_network
MATCH (n)
RETURN n.name AS name, n.department AS dept, n.title AS title,
       n.city AS city, n.level AS level, n.age AS age
ORDER BY n.department, n.name;


-- ============================================================================
-- 2. HEADCOUNT BY DEPARTMENT — Workforce distribution
-- ============================================================================
-- How large is each team? Which departments may need hiring?

ASSERT ROW_COUNT = 8
ASSERT VALUE headcount = 12 WHERE department = 'Engineering'
ASSERT VALUE headcount >= 12 WHERE department = 'Marketing'
USE {{zone_name}}.graph.social_network
MATCH (n)
RETURN n.department AS department, count(n) AS headcount,
       avg(n.age) AS avg_age
ORDER BY headcount DESC;


-- ============================================================================
-- 3. NETWORK SIZE — How connected is this startup?
-- ============================================================================
-- A healthy 100-person company should have 200+ connections (2+ per
-- person on average). Fewer suggests siloed teams.

ASSERT VALUE total_connections = 314
USE {{zone_name}}.graph.social_network
MATCH (a)-[r]->(b)
RETURN count(r) AS total_connections;


-- ============================================================================
-- 4. RELATIONSHIP MIX — What types of bonds hold the company together?
-- ============================================================================
-- Understanding the mix reveals organizational health. All "colleagues"
-- with no "mentors" means weak knowledge transfer.

ASSERT ROW_COUNT = 11
ASSERT VALUE count = 60 WHERE type = 'mentor'
USE {{zone_name}}.graph.social_network
MATCH (a)-[r]->(b)
RETURN r.relationship_type AS type, count(r) AS count,
       avg(r.weight) AS avg_strength
ORDER BY count DESC;


-- ============================================================================
-- 5. ENGINEERING ROSTER — VP of Engineering's team review
-- ============================================================================

ASSERT ROW_COUNT = 12
ASSERT VALUE title = 'Director' WHERE name = 'Priya_40'
USE {{zone_name}}.graph.social_network
MATCH (n)
WHERE n.department = 'Engineering'
RETURN n.name AS name, n.title AS title, n.city AS city, n.level AS level
ORDER BY n.level DESC;


-- ============================================================================
-- 6. MENTORSHIP NETWORK — Who is coaching whom?
-- ============================================================================
-- Which mentor-mentee pairs have the strongest bonds? These are the
-- mentorships worth studying and replicating across teams.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.graph.social_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor' AND r.weight > 0.8
RETURN mentor.name AS mentor, mentee.name AS mentee,
       mentor.level AS mentor_level, r.weight AS strength
ORDER BY r.weight DESC
LIMIT 10;


-- ============================================================================
-- 7. CROSS-DEPARTMENT BRIDGES — Who prevents organizational silos?
-- ============================================================================
-- Show connections between different departments. These bridge employees
-- are critical for cross-team collaboration.

ASSERT ROW_COUNT = 20
USE {{zone_name}}.graph.social_network
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.name AS from_person, a.department AS from_dept,
       b.name AS to_person, b.department AS to_dept,
       r.relationship_type AS type, r.weight AS strength
ORDER BY r.weight DESC
LIMIT 20;


-- ============================================================================
-- 8. DEPARTMENT CONNECTIVITY — Which teams talk to each other?
-- ============================================================================

ASSERT ROW_COUNT = 44
ASSERT VALUE connections >= 3 WHERE from_dept = 'Engineering'
USE {{zone_name}}.graph.social_network
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC;


-- ============================================================================
-- 9. OFFICE COLLABORATION — How well do our 5 offices work together?
-- ============================================================================
-- With employees in NYC, SF, Chicago, London, and Berlin, the company
-- needs to ensure remote offices aren't isolated.

ASSERT ROW_COUNT = 15
ASSERT VALUE connections >= 18 WHERE from_city = 'NYC'
USE {{zone_name}}.graph.social_network
MATCH (a)-[r]->(b)
RETURN a.city AS from_city, b.city AS to_city,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC;


-- ============================================================================
-- 10. KNOWLEDGE PATHS — 2-hop information flow from employee #1
-- ============================================================================
-- If employee #1 knows something important, this traces how it spreads:
-- who they tell directly, and who those people then tell.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.graph.social_network
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1
RETURN a.name AS source, b.name AS relay, c.name AS reached,
       b.department AS relay_dept, c.department AS reached_dept
LIMIT 25;


-- ============================================================================
-- 11. REACHABILITY — Who can employee #1 reach within 3 hops?
-- ============================================================================

ASSERT ROW_COUNT = 18
USE {{zone_name}}.graph.social_network
MATCH (a)-[*1..3]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
ORDER BY b.name;


-- ============================================================================
-- 12. RECIPROCAL RELATIONSHIPS — Where are mutual bonds?
-- ============================================================================
-- Reciprocal connections (A→B and B→A) are the strongest relationships.
-- High mutual count = healthy collaborative culture.

ASSERT ROW_COUNT = 2
USE {{zone_name}}.graph.social_network
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN a.name AS person_a, a.department AS dept_a,
       b.name AS person_b, b.department AS dept_b,
       r1.relationship_type AS a_to_b, r2.relationship_type AS b_to_a
ORDER BY a.name
LIMIT 15;


-- ============================================================================
-- 13. DISENGAGED EMPLOYEES — Who has zero outgoing connections?
-- ============================================================================
-- Employees with no outgoing connections may be disengaged, brand new,
-- or remote without onboarding. HR should check on these people.

ASSERT ROW_COUNT = 0
USE {{zone_name}}.graph.social_network
MATCH (n)
WHERE NOT (n)-->()
RETURN n.name AS name, n.department AS dept, n.city AS city,
       n.level AS level
ORDER BY n.name;


-- ============================================================================
-- 14. STRONGEST MENTORSHIPS — High-impact coaching bonds
-- ============================================================================

ASSERT ROW_COUNT = 15
USE {{zone_name}}.graph.social_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor.name AS mentor, mentor.level AS mentor_level,
       mentee.name AS mentee, mentee.level AS mentee_level,
       r.weight AS bond_strength
ORDER BY r.weight DESC
LIMIT 15;


-- ############################################################################
-- ############################################################################
--
-- PART 2: GRAPH ALGORITHMS — Influence, Communities & Paths
--
-- ############################################################################
-- ############################################################################


-- ============================================================================
-- 15. PAGERANK — Who has the most organizational influence?
-- ============================================================================
-- PageRank reveals the truly influential people — not just those with
-- the most connections, but those connected to by other well-connected
-- people. These are the informal leaders whose opinions shape company
-- culture and technical direction.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.graph.social_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 16. DEGREE CENTRALITY — Raw connection counts
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.graph.social_network
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 17. NATURAL COMMUNITIES — Do teams match the org chart?
-- ============================================================================
-- Louvain finds groups based on actual connections, not the org chart.
-- If communities match departments, the structure reflects reality.

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.graph.social_network
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 18. GATEKEEPERS — Who controls information flow?
-- ============================================================================
-- If these people leave, communication between groups breaks down.
-- In a 100-person startup, even one key gatekeeper leaving is a crisis.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.graph.social_network
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 10;


-- ============================================================================
-- 19. IS THE ORG FULLY CONNECTED? — Connected components
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.graph.social_network
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 20. TIGHT-KNIT GROUPS — Triangle count
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.graph.social_network
CALL algo.triangle_count()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC
LIMIT 10;


-- ============================================================================
-- 21. SIX DEGREES — How many hops apart are people?
-- ============================================================================
-- In a well-connected startup, everyone should be reachable within 3-4
-- hops. More than that suggests organizational fragmentation.

ASSERT ROW_COUNT >= 1
ASSERT VALUE people_at_distance = 1 WHERE depth = 0
USE {{zone_name}}.graph.social_network
CALL algo.bfs({source: 1})
YIELD node_id, depth, parent_id
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ============================================================================
-- 22. SHORTEST PATH — How does a message travel across the company?
-- ============================================================================
-- If employee #1 needs to reach employee #50 (likely in a different
-- department and city), what's the fastest path through the network?

ASSERT ROW_COUNT >= 2
ASSERT VALUE node_id = 1 WHERE step = 0
ASSERT VALUE distance = 0 WHERE step = 0
USE {{zone_name}}.graph.social_network
CALL algo.shortestPath({source: 1, target: 50})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 23. ACCESSIBILITY — Who can reach everyone fastest?
-- ============================================================================

ASSERT ROW_COUNT = 10
USE {{zone_name}}.graph.social_network
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 10;


-- ============================================================================
-- 24. BACKBONE NETWORK — Minimum connections to stay linked
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.graph.social_network
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN sourceId, targetId, weight
ORDER BY weight
LIMIT 25;


-- ############################################################################
-- ############################################################################
--
-- PART 3: GRAPH VISUALIZATION — See the Company Network
--
-- ############################################################################
-- ############################################################################


-- ============================================================================
-- 25. FULL COMPANY GRAPH — All 100 employees with all connections
-- ============================================================================
-- Department clusters should be visible as dense groups, with bridge
-- employees spanning between them.

ASSERT ROW_COUNT = 314
USE {{zone_name}}.graph.social_network
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 26. MENTORSHIP NETWORK — Only mentor relationships
-- ============================================================================
-- Reveals the hierarchical skeleton: who mentors whom. Directors should
-- appear as high-degree hub nodes connecting to multiple mentees.

ASSERT ROW_COUNT = 60
USE {{zone_name}}.graph.social_network
MATCH (mentor)-[r]->(mentee)
WHERE r.relationship_type = 'mentor'
RETURN mentor, r, mentee;


-- ============================================================================
-- 27. CROSS-DEPARTMENT BRIDGES — Only inter-department connections
-- ============================================================================
-- Strips away intra-department edges to see only the connections that
-- cross departmental boundaries. Isolated departments with no outgoing
-- edges are organizational blind spots.

ASSERT ROW_COUNT = 173
USE {{zone_name}}.graph.social_network
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total vertices, total edges, relationship type
-- distribution, and headcount — the core structural invariants of this demo.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_employees = 100
ASSERT VALUE total_connections = 314
ASSERT VALUE mentor_connections = 60
ASSERT VALUE engineering_headcount = 12
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.graph.employees)                                           AS total_employees,
    (SELECT COUNT(*) FROM {{zone_name}}.graph.connections)                                         AS total_connections,
    (SELECT COUNT(*) FROM {{zone_name}}.graph.connections WHERE relationship_type = 'mentor')      AS mentor_connections,
    (SELECT COUNT(*) FROM {{zone_name}}.graph.employees WHERE department = 'Engineering')          AS engineering_headcount;
