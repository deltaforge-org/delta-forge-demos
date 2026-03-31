-- ============================================================================
-- Graph Advanced Cypher — Demonstration Queries
-- ============================================================================
-- University research collaboration network: 40 researchers, 5 departments,
-- 170 directed edges (co-author, advisor, committee, reviewer).
--
-- This demo focuses on ADVANCED CYPHER PATTERNS:
--   - Negative patterns (WHERE NOT)
--   - Aggregation functions (collect, count, avg, min, max)
--   - Multi-hop traversals with edge type filtering
--   - Mixed relationship type queries
--   - Graph algorithms (PageRank, betweenness, components, Louvain)
-- ============================================================================


-- ============================================================================
-- 1. ALL RESEARCHERS — Browse the full faculty directory
-- ============================================================================
-- The provost wants a roster of all 40 researchers with their department,
-- rank, h-index, and active status.

ASSERT ROW_COUNT = 40
ASSERT VALUE department = 'CompSci' WHERE name = 'Prof. Chen_10'
ASSERT VALUE rank = 'Professor' WHERE name = 'Prof. Chen_10'
ASSERT VALUE h_index = 30 WHERE name = 'Prof. Chen_10'
USE {{zone_name}}.research_network.research_network
MATCH (n)
RETURN n.name AS name, n.department AS department, n.rank AS rank,
       n.h_index AS h_index, n.active AS active
ORDER BY n.department, n.name;


-- ============================================================================
-- 2. DEPARTMENT DISTRIBUTION — Faculty balance across departments
-- ============================================================================
-- Each of the 5 departments should have exactly 8 researchers (40 / 5).

ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 8 WHERE department = 'CompSci'
ASSERT VALUE headcount = 8 WHERE department = 'Physics'
USE {{zone_name}}.research_network.research_network
MATCH (n)
RETURN n.department AS department, count(n) AS headcount
ORDER BY department;


-- ============================================================================
-- 3. COLLABORATION OVERVIEW — Edge type distribution
-- ============================================================================
-- Understand the mix of collaboration types. A healthy network has diverse
-- connection types — not just co-authorship.

ASSERT ROW_COUNT = 4
ASSERT VALUE count = 70 WHERE type = 'co-author'
ASSERT VALUE count = 35 WHERE type = 'advisor'
ASSERT VALUE count = 35 WHERE type = 'committee'
ASSERT VALUE count = 30 WHERE type = 'reviewer'
USE {{zone_name}}.research_network.research_network
MATCH (a)-[r]->(b)
RETURN r.collab_type AS type, count(r) AS count
ORDER BY count DESC;


-- ============================================================================
-- 4. NEGATIVE PATTERN: ISOLATED RESEARCHERS — No outgoing collaborations
-- ============================================================================
-- Researchers with no outgoing edges are not actively reaching out.
-- These are the most junior assistants (ids 36-40) who receive mentorship
-- and committee invitations but have not initiated collaborations.
-- Cypher negative pattern: WHERE NOT (n)-->()

ASSERT ROW_COUNT = 5
ASSERT VALUE name = 'Prof. Chen_40' WHERE name = 'Prof. Chen_40'
ASSERT VALUE name = 'Prof. Larsson_36' WHERE name = 'Prof. Larsson_36'
USE {{zone_name}}.research_network.research_network
MATCH (n)
WHERE NOT (n)-->()
RETURN n.name AS name, n.department AS department, n.rank AS rank
ORDER BY n.name;


-- ============================================================================
-- 5. NEGATIVE PATTERN: NO INCOMING — Nobody collaborates with them
-- ============================================================================
-- All 40 researchers receive at least one incoming edge (as co-author
-- destinations, advisor targets, committee invitees, or review subjects).
-- An empty result confirms the network has no completely disconnected nodes.

ASSERT ROW_COUNT = 0
USE {{zone_name}}.research_network.research_network
MATCH (n)
WHERE NOT (n)<--()
RETURN n.name AS name, n.department AS department;


-- ============================================================================
-- 6. CYPHER AGGREGATION: DEPARTMENT H-INDEX STATS
-- ============================================================================
-- Uses Cypher aggregation functions: count, avg, min, max, collect.
-- Compare research output across departments using h-index statistics.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_h = 27.5 WHERE dept = 'CompSci'
ASSERT VALUE min_h = 10 WHERE dept = 'CompSci'
ASSERT VALUE max_h = 45 WHERE dept = 'CompSci'
ASSERT VALUE min_h = 6 WHERE dept = 'Math'
ASSERT VALUE max_h = 49 WHERE dept = 'Biology'
USE {{zone_name}}.research_network.research_network
MATCH (n)
RETURN n.department AS dept,
       count(n) AS num_researchers,
       avg(n.h_index) AS avg_h,
       min(n.h_index) AS min_h,
       max(n.h_index) AS max_h
ORDER BY avg_h DESC;


-- ============================================================================
-- 7. MULTI-MATCH: CO-AUTHOR TRIANGLES
-- ============================================================================
-- Closed co-authorship triangles: A co-authors with B, B with C, C with A.
-- These tight-knit triads are the backbone of productive research groups.
-- The ring+backskip co-author pattern creates directed 3-cycles within depts.

ASSERT ROW_COUNT = 75
USE {{zone_name}}.research_network.research_network
MATCH (a)-[:co-author]->(b)-[:co-author]->(c)-[:co-author]->(a)
RETURN a.name AS researcher_a, b.name AS researcher_b, c.name AS researcher_c
ORDER BY a.name, b.name;


-- ============================================================================
-- 8. EDGE TYPE FILTERING: ADVISOR CHAINS
-- ============================================================================
-- 2-hop advisor chains: Dean advises Professor, who advises Associate/Assistant.
-- These knowledge transfer pipelines are critical for junior researcher growth.
-- Each department has one Dean→Professor→(Associate or Assistant) chain set.

ASSERT ROW_COUNT = 20
USE {{zone_name}}.research_network.research_network
MATCH (a)-[:advisor]->(b)-[:advisor]->(c)
WHERE a <> c
RETURN a.name AS senior, a.rank AS senior_rank,
       b.name AS middle, b.rank AS middle_rank,
       c.name AS junior, c.rank AS junior_rank
ORDER BY a.name, c.name;


-- ============================================================================
-- 9. MIXED TYPES: ADVISORS WHO ALSO CO-AUTHOR
-- ============================================================================
-- When an advisor also co-authors papers with their advisee, it signals
-- a deep mentoring relationship beyond just guidance. These dual-edge
-- pairs appear where Deans/Professors connect to the next department
-- member (who is also their co-author via the ring pattern).

ASSERT ROW_COUNT = 10
USE {{zone_name}}.research_network.research_network
MATCH (a)-[:advisor]->(b), (a)-[:co-author]->(b)
RETURN a.name AS advisor, a.rank AS advisor_rank,
       b.name AS advisee, b.rank AS advisee_rank
ORDER BY a.name;


-- ============================================================================
-- 10. PROPERTY AGGREGATION: PROLIFIC COLLABORATORS
-- ============================================================================
-- Researchers with the most unique outgoing collaboration targets.
-- High out-degree researchers are the connectors who bridge the network.

ASSERT ROW_COUNT = 35
ASSERT VALUE collaborator_count = 7 WHERE name = 'Prof. Okafor_5'
USE {{zone_name}}.research_network.research_network
MATCH (a)-[]->(b)
RETURN a.name AS name, a.department AS dept, count(DISTINCT b) AS collaborator_count
ORDER BY collaborator_count DESC, name;


-- ============================================================================
-- 11. PAGERANK — Who are the most influential researchers?
-- ============================================================================
-- PageRank measures recursive influence: being connected to well-connected
-- people matters more than raw connection count. Deans and Professors
-- should rank high due to their central advisory positions.

ASSERT ROW_COUNT = 40
USE {{zone_name}}.research_network.research_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 12. BETWEENNESS CENTRALITY — Cross-department connectors
-- ============================================================================
-- Researchers who sit on many shortest paths between others control
-- information flow. Committee members and reviewers who span departments
-- should have high betweenness centrality.

ASSERT ROW_COUNT = 40
USE {{zone_name}}.research_network.research_network
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 13. CONNECTED COMPONENTS — Is the research network fully connected?
-- ============================================================================
-- In a healthy university, all researchers should be reachable from any
-- other. Multiple components would indicate isolated research silos.

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.research_network.research_network
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 14. LOUVAIN COMMUNITIES — Natural research clusters
-- ============================================================================
-- Louvain community detection finds groups based on actual collaboration
-- density. With 5 departments and cross-department edges, the algorithm
-- should detect meaningful community structure.

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.research_network.research_network
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 15. VERIFY: ALL CHECKS — Cross-cutting sanity checks
-- ============================================================================
-- Confirms the graph was loaded correctly: 40 researchers, 170 edges,
-- balanced departments, and exactly 5 isolated (no outgoing) researchers.

ASSERT ROW_COUNT = 40
USE {{zone_name}}.research_network.research_network
MATCH (n)
RETURN n.id AS id;

ASSERT VALUE total_collaborations = 170
USE {{zone_name}}.research_network.research_network
MATCH (a)-[r]->(b)
RETURN count(r) AS total_collaborations;

ASSERT VALUE dept_count = 5
USE {{zone_name}}.research_network.research_network
MATCH (n)
RETURN count(DISTINCT n.department) AS dept_count;

ASSERT VALUE isolated_count = 5
USE {{zone_name}}.research_network.research_network
MATCH (n)
WHERE NOT (n)-->()
RETURN count(n) AS isolated_count;
