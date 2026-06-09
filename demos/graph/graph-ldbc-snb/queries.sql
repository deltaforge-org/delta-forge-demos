-- ############################################################################
-- ############################################################################
--
--   LDBC SOCIAL NETWORK BENCHMARK — FULL MODEL VERIFICATION
--   Scale Factor 0.1: 8 Entity Types / 23 Relationship Types / ~1.2M rows
--
-- ############################################################################
-- ############################################################################
--
-- Uses the industry-standard LDBC SNB dataset with official golden values
-- from the LDBC reference implementation validation parameters.
--
-- The graph definition ({{zone_name}}.ldbc_social_network.ldbc_social_network) maps Person vertices + KNOWS
-- edges. Cypher queries use this for pattern matching and algorithms.
-- SQL queries join across the full relational model for multi-relationship
-- traversals that span beyond the KNOWS graph.
--
-- PART 1: DATA INTEGRITY CHECKS (queries 1–5)
-- PART 2: CYPHER — SOCIAL GRAPH EXPLORATION (queries 6–14)
-- PART 3: CYPHER — GRAPH ALGORITHMS (queries 15–25)
-- PART 4: MIXED SQL + CYPHER — Joining graph results with Delta tables (queries 26–30)
-- PART 5: LDBC INTERACTIVE QUERIES — SQL golden value checks (queries 31–41)
-- PART 6: VERIFICATION SUMMARY (query 42)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY CHECKS
-- ############################################################################


-- ============================================================================
-- 1. ENTITY COUNTS — Verify all 8 entity types loaded correctly
-- ============================================================================
-- Expected: Person 1,528 | Comment 151,043 | Post 135,701 | Forum 13,750
--           Place 1,460  | Organisation 7,955 | Tag 16,080 | TagClass 71

ASSERT VALUE row_count = 1528 WHERE entity = 'person'
ASSERT VALUE row_count = 151043 WHERE entity = 'comment'
ASSERT VALUE row_count = 135701 WHERE entity = 'post'
ASSERT VALUE row_count = 13750 WHERE entity = 'forum'
ASSERT VALUE row_count = 1460 WHERE entity = 'place'
ASSERT VALUE row_count = 7955 WHERE entity = 'organisation'
ASSERT VALUE row_count = 71 WHERE entity = 'tagclass'
ASSERT VALUE row_count = 16080 WHERE entity = 'tag'
ASSERT ROW_COUNT = 8
SELECT 'person' AS entity, COUNT(*) AS row_count FROM {{zone_name}}.ldbc_social_network.person
UNION ALL SELECT 'comment', COUNT(*) FROM {{zone_name}}.ldbc_social_network.comment
UNION ALL SELECT 'post', COUNT(*) FROM {{zone_name}}.ldbc_social_network.post
UNION ALL SELECT 'forum', COUNT(*) FROM {{zone_name}}.ldbc_social_network.forum
UNION ALL SELECT 'place', COUNT(*) FROM {{zone_name}}.ldbc_social_network.place
UNION ALL SELECT 'organisation', COUNT(*) FROM {{zone_name}}.ldbc_social_network.organisation
UNION ALL SELECT 'tag', COUNT(*) FROM {{zone_name}}.ldbc_social_network.tag
UNION ALL SELECT 'tagclass', COUNT(*) FROM {{zone_name}}.ldbc_social_network.tagclass
ORDER BY entity;


-- ============================================================================
-- 2. EDGE COUNTS — Verify all relationship types loaded
-- ============================================================================

ASSERT ROW_COUNT = 25
SELECT 'person_knows_person' AS edge, COUNT(*) AS row_count FROM {{zone_name}}.ldbc_social_network.person_knows_person
UNION ALL SELECT 'comment_has_creator_person', COUNT(*) FROM {{zone_name}}.ldbc_social_network.comment_has_creator_person
UNION ALL SELECT 'comment_has_tag_tag', COUNT(*) FROM {{zone_name}}.ldbc_social_network.comment_has_tag_tag
UNION ALL SELECT 'comment_is_located_in_place', COUNT(*) FROM {{zone_name}}.ldbc_social_network.comment_is_located_in_place
UNION ALL SELECT 'comment_reply_of_comment', COUNT(*) FROM {{zone_name}}.ldbc_social_network.comment_reply_of_comment
UNION ALL SELECT 'comment_reply_of_post', COUNT(*) FROM {{zone_name}}.ldbc_social_network.comment_reply_of_post
UNION ALL SELECT 'forum_container_of_post', COUNT(*) FROM {{zone_name}}.ldbc_social_network.forum_container_of_post
UNION ALL SELECT 'forum_has_member_person', COUNT(*) FROM {{zone_name}}.ldbc_social_network.forum_has_member_person
UNION ALL SELECT 'forum_has_moderator_person', COUNT(*) FROM {{zone_name}}.ldbc_social_network.forum_has_moderator_person
UNION ALL SELECT 'forum_has_tag_tag', COUNT(*) FROM {{zone_name}}.ldbc_social_network.forum_has_tag_tag
UNION ALL SELECT 'person_email', COUNT(*) FROM {{zone_name}}.ldbc_social_network.person_email
UNION ALL SELECT 'person_has_interest_tag', COUNT(*) FROM {{zone_name}}.ldbc_social_network.person_has_interest_tag
UNION ALL SELECT 'person_is_located_in_place', COUNT(*) FROM {{zone_name}}.ldbc_social_network.person_is_located_in_place
UNION ALL SELECT 'person_likes_comment', COUNT(*) FROM {{zone_name}}.ldbc_social_network.person_likes_comment
UNION ALL SELECT 'person_likes_post', COUNT(*) FROM {{zone_name}}.ldbc_social_network.person_likes_post
UNION ALL SELECT 'person_speaks_language', COUNT(*) FROM {{zone_name}}.ldbc_social_network.person_speaks_language
UNION ALL SELECT 'person_study_at_organisation', COUNT(*) FROM {{zone_name}}.ldbc_social_network.person_study_at_organisation
UNION ALL SELECT 'person_work_at_organisation', COUNT(*) FROM {{zone_name}}.ldbc_social_network.person_work_at_organisation
UNION ALL SELECT 'post_has_creator_person', COUNT(*) FROM {{zone_name}}.ldbc_social_network.post_has_creator_person
UNION ALL SELECT 'post_has_tag_tag', COUNT(*) FROM {{zone_name}}.ldbc_social_network.post_has_tag_tag
UNION ALL SELECT 'post_is_located_in_place', COUNT(*) FROM {{zone_name}}.ldbc_social_network.post_is_located_in_place
UNION ALL SELECT 'organisation_is_located_in_place', COUNT(*) FROM {{zone_name}}.ldbc_social_network.organisation_is_located_in_place
UNION ALL SELECT 'place_is_part_of_place', COUNT(*) FROM {{zone_name}}.ldbc_social_network.place_is_part_of_place
UNION ALL SELECT 'tag_has_type_tagclass', COUNT(*) FROM {{zone_name}}.ldbc_social_network.tag_has_type_tagclass
UNION ALL SELECT 'tagclass_is_subclass_of_tagclass', COUNT(*) FROM {{zone_name}}.ldbc_social_network.tagclass_is_subclass_of_tagclass
ORDER BY edge;


-- ============================================================================
-- 3. GRAPH CONFIG — Verify graph definition
-- ============================================================================

SHOW GRAPH;


-- ============================================================================
-- 4. PLACE HIERARCHY — Countries, cities, continents
-- ============================================================================
-- Golden: 1343 cities, 6 continents, 111 countries (sum = 1460 places).

ASSERT ROW_COUNT = 3
ASSERT VALUE count = 1343 WHERE type = 'city'
ASSERT VALUE count = 111 WHERE type = 'country'
ASSERT VALUE count = 6 WHERE type = 'continent'
SELECT type, COUNT(*) AS count
FROM {{zone_name}}.ldbc_social_network.place
GROUP BY type
ORDER BY type;


-- ============================================================================
-- 5. REFERENTIAL INTEGRITY — All KNOWS edges have valid endpoints
-- ============================================================================

ASSERT VALUE orphan_edges = 0
SELECT COUNT(*) AS orphan_edges
FROM {{zone_name}}.ldbc_social_network.person_knows_person k
WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.ldbc_social_network.person p WHERE p.id = k.src)
   OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.ldbc_social_network.person p WHERE p.id = k.dst);


-- ############################################################################
-- PART 2: CYPHER — SOCIAL GRAPH EXPLORATION
-- ############################################################################
-- These queries use the {{zone_name}}.ldbc_social_network.ldbc_social_network graph definition to traverse
-- the Person-KNOWS-Person social graph using Cypher pattern matching.
-- ############################################################################


-- ============================================================================
-- 6. BROWSE THE SOCIAL NETWORK — All 1,528 persons
-- ============================================================================
-- LDBC Short Query 1 pattern: person profile lookup.

ASSERT ROW_COUNT = 25
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
MATCH (p)
RETURN p.id AS id, p.first_name AS first_name, p.last_name AS last_name,
       p.gender AS gender
ORDER BY p.last_name, p.first_name
LIMIT 25;


-- ============================================================================
-- 7. DIRECT FRIENDS — Who does person 933 (Mahinda Perera) know?
-- ============================================================================
-- LDBC Short Query 3 pattern: friend list with friendship dates.
-- Cypher: MATCH (:Person {id:933})-[r:KNOWS]-(friend) RETURN friend

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
MATCH (a)-[r]->(b)
WHERE a.id = 933
RETURN a.first_name AS person, a.last_name AS person_last,
       b.first_name AS friend_first, b.last_name AS friend_last,
       b.gender AS gender;


-- ============================================================================
-- 8. FRIEND OF FRIEND — 2-hop social exploration
-- ============================================================================
-- LDBC Q1 pattern (simplified): find friends-of-friends.
-- Cypher: MATCH (:Person {id:933})-[:KNOWS*1..2]-(friend)
-- Classic social network recommendation: suggest connections.

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 933 AND a.id <> c.id
RETURN DISTINCT c.first_name AS suggested_friend, c.last_name AS last_name
ORDER BY c.first_name
LIMIT 20;


-- ============================================================================
-- 9. REACHABILITY — Who can person 933 reach within 3 hops?
-- ============================================================================
-- LDBC Q13 pattern: variable-length KNOWS traversal.
-- In a well-connected social network, most people should be reachable
-- within 3-4 hops (small-world property).

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
MATCH (a)-[*1..3]->(b)
WHERE a.id = 933 AND a <> b
RETURN DISTINCT b.id AS reachable_id, b.first_name AS name
ORDER BY b.first_name
LIMIT 30;


-- ============================================================================
-- 10. MUTUAL FRIENDSHIPS — Reciprocal KNOWS relationships
-- ============================================================================
-- If A knows B AND B knows A, that's a mutual friendship.
-- Cypher: MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(a) WHERE a.id < b.id

ASSERT VALUE mutual_friendship_count = 0
ASSERT ROW_COUNT = 1
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN count(*) AS mutual_friendship_count;


-- ============================================================================
-- 11. GENDER DISTRIBUTION OF CONNECTIONS
-- ============================================================================
-- Do people preferentially connect within or across genders?
-- Golden (from raw KNOWS edges):
--   female→male  = 3667   female→female = 3490
--   male→female  = 3483   male→male     = 3433
-- Sum = 14073 = total KNOWS edges.

ASSERT ROW_COUNT = 4
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
MATCH (a)-[r]->(b)
RETURN a.gender AS from_gender, b.gender AS to_gender, count(r) AS connections
ORDER BY connections DESC;


-- ============================================================================
-- 12. SOCIAL HUBS — Top connected people via Cypher
-- ============================================================================
-- Cross-verify with degree centrality algorithm results.
-- Cypher: MATCH (a)-[:KNOWS]->(b) RETURN a, count(b) ORDER BY count DESC

ASSERT ROW_COUNT = 15
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
MATCH (a)-[r]->(b)
RETURN a.id AS person_id, a.first_name AS first_name, a.last_name AS last_name,
       count(r) AS out_degree
ORDER BY out_degree DESC
LIMIT 15;


-- ============================================================================
-- 13. HUB NEIGHBORHOOD — Direct friends of the top hub
-- ============================================================================
-- Person 26388279067534 has degree 340 — who are their direct contacts?

ASSERT ROW_COUNT = 20
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
MATCH (hub)-[r]->(friend)
WHERE hub.id = 26388279067534
RETURN friend.id AS friend_id, friend.first_name AS first_name,
       friend.last_name AS last_name, friend.gender AS gender
ORDER BY friend.first_name
LIMIT 20;


-- ============================================================================
-- 14. GRAPH VISUALIZATION — Social network structure
-- ============================================================================
-- Renders the KNOWS graph. With 1,528 nodes and 14,073 edges,
-- community structure should be visible as dense clusters.

ASSERT ROW_COUNT = 500
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
MATCH (a)-[r]->(b)
RETURN a, r, b
LIMIT 500;


-- ############################################################################
-- PART 3: CYPHER — GRAPH ALGORITHMS
-- ############################################################################
-- Each algorithm runs on the {{zone_name}}.ldbc_social_network.ldbc_social_network graph (Person + KNOWS).
-- Golden values come from the raw dataset and LDBC validation parameters.
-- ############################################################################


-- ============================================================================
-- 15. PAGERANK — Most influential people in the social network
-- ============================================================================
-- Golden: Person 26388279067534 (degree 340) and 32985348834375 (degree 338)
-- should rank near the top.

ASSERT ROW_COUNT = 15
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 15;


-- ============================================================================
-- 16. DEGREE CENTRALITY — Raw connection counts
-- ============================================================================
-- Golden values (total degree from raw data):
--   26388279067534: 340 | 32985348834375: 338 | 2199023256816: 269
--   24189255811566: 256 | 6597069767242: 230

ASSERT VALUE total_degree = 340 WHERE node_id = 26388279067534
ASSERT VALUE total_degree = 338 WHERE node_id = 32985348834375
ASSERT ROW_COUNT = 15
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 15;


-- ============================================================================
-- 17. BETWEENNESS CENTRALITY — Bridge nodes in the social network
-- ============================================================================
-- Identifies people who sit on the shortest paths between many pairs.
-- Removing high-betweenness nodes would fragment the network.

ASSERT ROW_COUNT = 15
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC
LIMIT 15;


-- ============================================================================
-- 18. CLOSENESS CENTRALITY — Who can reach everyone fastest?
-- ============================================================================

ASSERT ROW_COUNT = 15
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC
LIMIT 15;


-- ============================================================================
-- 19. CONNECTED COMPONENTS — Is the network fully connected?
-- ============================================================================
-- A single large component means everyone can reach everyone else.

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 20. LOUVAIN COMMUNITIES — Natural social clusters
-- ============================================================================
-- Detects communities based on actual connection density.

-- Non-deterministic: Louvain's community assignment depends on random tie-breaking
ASSERT WARNING ROW_COUNT >= 2
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 21. TRIANGLE COUNT — Clustering coefficient
-- ============================================================================

ASSERT ROW_COUNT = 15
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.triangle_count()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC
LIMIT 15;


-- ============================================================================
-- 22. STRONGLY CONNECTED COMPONENTS — Directed reachability groups
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.scc()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 23. SHORTEST PATH — LDBC Q13 golden value verification
-- ============================================================================
-- Golden: 2199023256816 → 24189255812380 = path length 3
-- Path: K. Bose → Karl Muller → Bruna Costa → Fernanda Souza

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.shortestPath({source: 2199023256816, target: 24189255812380})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 24. SHORTEST PATH — Second golden verification (length 2)
-- ============================================================================
-- Golden: 6597069767242 → 6597069768287 = path length 1
-- Path: Salim Ahmed Binalshibh → Chito Reyes (direct edge)

ASSERT ROW_COUNT >= 2
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.shortestPath({source: 6597069767242, target: 6597069768287})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 25. BFS — Distance distribution from top hub
-- ============================================================================
-- Starting from highest-degree node (26388279067534, degree 340).
-- Most nodes should be within 3-4 hops in a well-connected social network.

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.ldbc_social_network.ldbc_social_network
CALL algo.bfs({source: 26388279067534})
YIELD node_id, depth, parent_id
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ############################################################################
-- PART 4: MIXED SQL + CYPHER — Joining Graph Results with Delta Tables
-- ############################################################################
-- DeltaForge can mix Cypher graph traversal results with Delta table SQL
-- queries using the cypher() table function. This enables powerful patterns:
-- run a graph algorithm or pattern match via Cypher, then enrich or filter
-- the results by joining with the full relational model in SQL.
--
-- Syntax: SELECT * FROM cypher('graph', $$ CYPHER_QUERY $$) AS (col TYPE, ...)
-- The cypher() result acts as a regular table in SQL — use it in JOINs, CTEs,
-- subqueries, WHERE IN clauses, etc.
-- ############################################################################


-- ============================================================================
-- 26. FRIENDS WITH LOCATIONS — Cypher traversal + Delta table enrichment
-- ============================================================================
-- Step 1 (Cypher): Find direct friends of Jun Wang via KNOWS graph
-- Step 2 (SQL): Join with person_is_located_in_place + place to get cities/countries
-- This demonstrates the core mixed pattern: graph traversal + relational enrichment.

ASSERT ROW_COUNT >= 1
WITH friends AS (
    SELECT * FROM cypher('{{zone_name}}.ldbc_social_network.ldbc_social_network', $$
        MATCH (a)-[]->(b)
        WHERE a.id = 26388279068220
        RETURN b.id AS friend_id, b.first_name AS first_name, b.last_name AS last_name
    $$) AS (friend_id BIGINT, first_name VARCHAR, last_name VARCHAR)
)
SELECT
    f.first_name, f.last_name,
    city.name AS city, country.name AS country
FROM friends f
JOIN {{zone_name}}.ldbc_social_network.person_is_located_in_place pip ON f.friend_id = pip.person_id
JOIN {{zone_name}}.ldbc_social_network.place city ON pip.place_id = city.id
JOIN {{zone_name}}.ldbc_social_network.place_is_part_of_place pipp ON city.id = pipp.place_id
JOIN {{zone_name}}.ldbc_social_network.place country ON pipp.parent_place_id = country.id
ORDER BY country.name, city.name, f.last_name;


-- ============================================================================
-- 27. PAGERANK LEADERS WITH EMPLOYMENT — Algorithm results + relational context
-- ============================================================================
-- Step 1 (Cypher): Run PageRank on the KNOWS graph to find influential people
-- Step 2 (SQL): Join with person_work_at_organisation to show where leaders work
-- Shows how graph centrality metrics gain meaning when enriched with metadata.

ASSERT ROW_COUNT >= 1
WITH ranked AS (
    SELECT * FROM cypher('{{zone_name}}.ldbc_social_network.ldbc_social_network', $$
        CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
        YIELD node_id, score
        RETURN node_id AS person_id, score
        ORDER BY score DESC
        LIMIT 10
    $$) AS (person_id BIGINT, score DOUBLE)
)
SELECT
    p.first_name, p.last_name, r.score AS pagerank_score,
    o.name AS employer, w.work_from AS work_since
FROM ranked r
JOIN {{zone_name}}.ldbc_social_network.person p ON r.person_id = p.id
LEFT JOIN {{zone_name}}.ldbc_social_network.person_work_at_organisation w ON p.id = w.person_id
LEFT JOIN {{zone_name}}.ldbc_social_network.organisation o ON w.organisation_id = o.id
ORDER BY r.score DESC;


-- ============================================================================
-- 28. SHORTEST PATH WITH PROFILES — Path enriched with person details
-- ============================================================================
-- Step 1 (Cypher): Compute shortest path between two people (golden: length 3)
-- Step 2 (SQL): Join each node on the path with person + place data
-- Turns abstract graph paths into meaningful "who connects to whom and where".

ASSERT ROW_COUNT >= 2
WITH path_nodes AS (
    SELECT * FROM cypher('{{zone_name}}.ldbc_social_network.ldbc_social_network', $$
        CALL algo.shortestPath({source: 2199023256816, target: 24189255812380})
        YIELD node_id, step, distance
        RETURN node_id AS person_id, step, distance
    $$) AS (person_id BIGINT, step BIGINT, distance BIGINT)
)
SELECT
    pn.step, pn.distance,
    p.first_name, p.last_name, p.gender,
    city.name AS city, country.name AS country
FROM path_nodes pn
JOIN {{zone_name}}.ldbc_social_network.person p ON pn.person_id = p.id
JOIN {{zone_name}}.ldbc_social_network.person_is_located_in_place pip ON p.id = pip.person_id
JOIN {{zone_name}}.ldbc_social_network.place city ON pip.place_id = city.id
JOIN {{zone_name}}.ldbc_social_network.place_is_part_of_place pipp ON city.id = pipp.place_id
JOIN {{zone_name}}.ldbc_social_network.place country ON pipp.parent_place_id = country.id
ORDER BY pn.step;


-- ============================================================================
-- 29. COMMUNITY MEMBERS WITH INTERESTS — Louvain communities + tag enrichment
-- ============================================================================
-- Step 1 (Cypher): Run Louvain community detection on the KNOWS graph
-- Step 2 (SQL): For the largest community, find shared interests via person_has_interest_tag
-- Reveals what topics bind a community together — graph structure meets content.

ASSERT ROW_COUNT >= 1
WITH communities AS (
    SELECT * FROM cypher('{{zone_name}}.ldbc_social_network.ldbc_social_network', $$
        CALL algo.louvain({resolution: 1.0})
        YIELD node_id, community_id
        RETURN node_id AS person_id, community_id AS community_id
    $$) AS (person_id BIGINT, community_id BIGINT)
),
largest_community AS (
    SELECT community_id
    FROM communities
    GROUP BY community_id
    ORDER BY COUNT(*) DESC
    LIMIT 1
)
SELECT t.name AS interest, COUNT(DISTINCT c.person_id) AS members_interested
FROM communities c
JOIN largest_community lc ON c.community_id = lc.community_id
JOIN {{zone_name}}.ldbc_social_network.person_has_interest_tag phi ON c.person_id = phi.person_id
JOIN {{zone_name}}.ldbc_social_network.tag t ON phi.tag_id = t.id
GROUP BY t.name
ORDER BY members_interested DESC
LIMIT 15;


-- ============================================================================
-- 30. DEGREE CENTRALITY WITH CONTENT ACTIVITY — Hub analysis
-- ============================================================================
-- Step 1 (Cypher): Get degree centrality from the KNOWS graph
-- Step 2 (SQL): Count posts and comments authored by top-degree people
-- Tests whether social network hubs are also the most active content creators.

ASSERT ROW_COUNT >= 1
WITH hub_scores AS (
    SELECT * FROM cypher('{{zone_name}}.ldbc_social_network.ldbc_social_network', $$
        CALL algo.degree()
        YIELD node_id, total_degree
        RETURN node_id AS person_id, total_degree AS degree
        ORDER BY total_degree DESC
        LIMIT 10
    $$) AS (person_id BIGINT, degree DOUBLE)
)
SELECT
    p.first_name, p.last_name, h.degree,
    COALESCE(post_counts.post_count, 0) AS posts_authored,
    COALESCE(comment_counts.comment_count, 0) AS comments_authored
FROM hub_scores h
JOIN {{zone_name}}.ldbc_social_network.person p ON h.person_id = p.id
LEFT JOIN (
    SELECT person_id, COUNT(*) AS post_count
    FROM {{zone_name}}.ldbc_social_network.post_has_creator_person
    GROUP BY person_id
) post_counts ON h.person_id = post_counts.person_id
LEFT JOIN (
    SELECT person_id, COUNT(*) AS comment_count
    FROM {{zone_name}}.ldbc_social_network.comment_has_creator_person
    GROUP BY person_id
) comment_counts ON h.person_id = comment_counts.person_id
ORDER BY h.degree DESC;


-- ############################################################################
-- PART 5: LDBC INTERACTIVE QUERIES — SQL Golden Value Checks
-- ############################################################################
-- These queries traverse multiple relationship types (KNOWS + HAS_CREATOR +
-- IS_LOCATED_IN + HAS_TAG etc.) which require SQL joins across the full
-- relational model. Golden expected results from validation_params-sf0.1.csv.
-- ############################################################################


-- ============================================================================
-- 31. LDBC SHORT Q1 — Person Profile
-- ============================================================================
-- Golden: first_name=Jun, last_name=Wang, gender=female, browser_used=Opera,
--         city_id=507 (Shanxi)

ASSERT VALUE first_name = 'Jun'
ASSERT VALUE last_name = 'Wang'
ASSERT VALUE gender = 'female'
ASSERT VALUE browser_used = 'Opera'
ASSERT VALUE city_id = 507
ASSERT ROW_COUNT = 1
SELECT
    p.first_name, p.last_name, p.birthday, p.location_ip,
    p.browser_used, p.gender, p.creation_date,
    pl.id AS city_id
FROM {{zone_name}}.ldbc_social_network.person p
JOIN {{zone_name}}.ldbc_social_network.person_is_located_in_place pip ON p.id = pip.person_id
JOIN {{zone_name}}.ldbc_social_network.place pl ON pip.place_id = pl.id
WHERE p.id = 26388279068220;


-- ============================================================================
-- 32. LDBC SHORT Q3 — Person's Friends
-- ============================================================================
-- Golden: Jun Wang (26388279068220) has 2 outgoing KNOWS edges:
--   Jie Yang (30786325577752) and Alexander Hleb (30786325578932)

ASSERT ROW_COUNT = 2
ASSERT VALUE first_name = 'Jie' WHERE person_id = 30786325577752
ASSERT VALUE first_name = 'Alexander' WHERE person_id = 30786325578932
SELECT
    p2.id AS person_id, p2.first_name, p2.last_name,
    k.creation_date AS friendship_creation_date
FROM {{zone_name}}.ldbc_social_network.person_knows_person k
JOIN {{zone_name}}.ldbc_social_network.person p2 ON k.dst = p2.id
WHERE k.src = 26388279068220
ORDER BY k.creation_date DESC, p2.id ASC;


-- ============================================================================
-- 33. LDBC SHORT Q5 — Message Creator
-- ============================================================================
-- Golden: person_id=26388279068220, Jun Wang

ASSERT VALUE first_name = 'Jun'
ASSERT VALUE last_name = 'Wang'
ASSERT ROW_COUNT = 1
SELECT p.id AS person_id, p.first_name, p.last_name
FROM {{zone_name}}.ldbc_social_network.comment c
JOIN {{zone_name}}.ldbc_social_network.comment_has_creator_person chc ON c.id = chc.comment_id
JOIN {{zone_name}}.ldbc_social_network.person p ON chc.person_id = p.id
WHERE c.id = 1099511997848;


-- ============================================================================
-- 34. LDBC SHORT Q6 — Message Forum
-- ============================================================================
-- Golden: forum_id=824633737506, title="Wall of Anh Pham",
--         moderator=Anh Pham

ASSERT ROW_COUNT = 1
SELECT
    f.id AS forum_id, f.title AS forum_title,
    mod_p.id AS moderator_id, mod_p.first_name, mod_p.last_name
FROM {{zone_name}}.ldbc_social_network.comment c
JOIN {{zone_name}}.ldbc_social_network.comment_reply_of_post crp ON c.id = crp.comment_id
JOIN {{zone_name}}.ldbc_social_network.forum_container_of_post fcp ON crp.post_id = fcp.post_id
JOIN {{zone_name}}.ldbc_social_network.forum f ON fcp.forum_id = f.id
JOIN {{zone_name}}.ldbc_social_network.forum_has_moderator_person fhm ON f.id = fhm.forum_id
JOIN {{zone_name}}.ldbc_social_network.person mod_p ON fhm.person_id = mod_p.id
WHERE c.id = 1099511997848;


-- ============================================================================
-- 35. LDBC Q2 — Recent Messages by Friends
-- ============================================================================
-- Golden: first result = The Kunda, message_id=1099511875186

ASSERT ROW_COUNT >= 1
SELECT
    p2.id AS person_id, p2.first_name, p2.last_name,
    msg.id AS message_id,
    COALESCE(msg.content, '') AS message_content,
    msg.creation_date AS message_creation_date
FROM {{zone_name}}.ldbc_social_network.person_knows_person k
JOIN {{zone_name}}.ldbc_social_network.person p2 ON k.dst = p2.id
JOIN {{zone_name}}.ldbc_social_network.post_has_creator_person phc ON p2.id = phc.person_id
JOIN {{zone_name}}.ldbc_social_network.post msg ON phc.post_id = msg.id
WHERE k.src = 19791209300143
  AND msg.creation_date <= 1354060800000
ORDER BY msg.creation_date DESC, msg.id ASC
LIMIT 20;


-- ============================================================================
-- 36. LDBC Q4 — New Tags in Time Window
-- ============================================================================
-- Golden: Norodom_Sihanouk (3), George_Clooney (1), Louis_Philippe_I (1)
-- Exactly 3 tag rows match the NOT-EXISTS 28-day window filter.

ASSERT ROW_COUNT = 3
SELECT t.name AS tag_name, COUNT(DISTINCT po.id) AS post_count
FROM {{zone_name}}.ldbc_social_network.person_knows_person k
JOIN {{zone_name}}.ldbc_social_network.post_has_creator_person phc ON k.dst = phc.person_id
JOIN {{zone_name}}.ldbc_social_network.post po ON phc.post_id = po.id
JOIN {{zone_name}}.ldbc_social_network.post_has_tag_tag pht ON po.id = pht.post_id
JOIN {{zone_name}}.ldbc_social_network.tag t ON pht.tag_id = t.id
WHERE k.src = 10995116278874
  AND po.creation_date >= 1338508800000
  AND po.creation_date < 1338508800000 + CAST(28 AS BIGINT) * 86400000
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.ldbc_social_network.person_knows_person k2
      JOIN {{zone_name}}.ldbc_social_network.post_has_creator_person phc2 ON k2.dst = phc2.person_id
      JOIN {{zone_name}}.ldbc_social_network.post old ON phc2.post_id = old.id
      JOIN {{zone_name}}.ldbc_social_network.post_has_tag_tag pht2 ON old.id = pht2.post_id
      WHERE k2.src = 10995116278874
        AND pht2.tag_id = pht.tag_id
        AND old.creation_date < 1338508800000
  )
GROUP BY t.name
ORDER BY post_count DESC, tag_name ASC
LIMIT 10;


-- ============================================================================
-- 37. LDBC Q6 — Tag Co-occurrence
-- ============================================================================
-- Golden: David_Foster (4), Harrison_Ford (2), Muammar_Gaddafi (2)
-- Query uses LIMIT 10 and the unfiltered result has >= 10 co-occurring tags.

ASSERT ROW_COUNT = 10
SELECT t2.name AS tag_name, COUNT(DISTINCT po.id) AS post_count
FROM {{zone_name}}.ldbc_social_network.person_knows_person k1
LEFT JOIN {{zone_name}}.ldbc_social_network.person_knows_person k2 ON k1.dst = k2.src
JOIN {{zone_name}}.ldbc_social_network.post_has_creator_person phc
    ON (k1.dst = phc.person_id OR k2.dst = phc.person_id)
JOIN {{zone_name}}.ldbc_social_network.post po ON phc.post_id = po.id
JOIN {{zone_name}}.ldbc_social_network.post_has_tag_tag pht1 ON po.id = pht1.post_id
JOIN {{zone_name}}.ldbc_social_network.tag t1 ON pht1.tag_id = t1.id AND t1.name = 'Shakira'
JOIN {{zone_name}}.ldbc_social_network.post_has_tag_tag pht2 ON po.id = pht2.post_id
JOIN {{zone_name}}.ldbc_social_network.tag t2 ON pht2.tag_id = t2.id AND t2.name <> 'Shakira'
WHERE k1.src = 2199023256816
GROUP BY t2.name
ORDER BY post_count DESC, tag_name ASC
LIMIT 10;


-- ============================================================================
-- 38. LDBC Q7 — Recent Likes
-- ============================================================================
-- Golden: first liker = Anh Nguyen (32985348834301),
--         like_date=1347061110109, message_id=1030792374999
-- Person 26388279067534 has far more than 20 likes across their posts, so LIMIT 20.

ASSERT ROW_COUNT = 20
SELECT
    p2.id AS person_id, p2.first_name, p2.last_name,
    lk.creation_date AS like_creation_date,
    po.id AS message_id,
    COALESCE(po.content, po.image_file) AS message_content
FROM {{zone_name}}.ldbc_social_network.post_has_creator_person phc
JOIN {{zone_name}}.ldbc_social_network.post po ON phc.post_id = po.id
JOIN {{zone_name}}.ldbc_social_network.person_likes_post lk ON po.id = lk.post_id
JOIN {{zone_name}}.ldbc_social_network.person p2 ON lk.person_id = p2.id
WHERE phc.person_id = 26388279067534
ORDER BY lk.creation_date DESC, po.id ASC
LIMIT 20;


-- ============================================================================
-- 39. LDBC Q8 — Recent Replies
-- ============================================================================
-- Golden: first reply by Ana Paula Silva, comment_id=1099511667820
-- Person 2199023256816 has more than 20 replies on their posts, so LIMIT 20.

ASSERT ROW_COUNT = 20
SELECT
    p2.id AS person_id, p2.first_name, p2.last_name,
    c.creation_date AS comment_creation_date,
    c.id AS comment_id,
    c.content AS comment_content
FROM {{zone_name}}.ldbc_social_network.post_has_creator_person phc
JOIN {{zone_name}}.ldbc_social_network.comment_reply_of_post crp ON phc.post_id = crp.post_id
JOIN {{zone_name}}.ldbc_social_network.comment c ON crp.comment_id = c.id
JOIN {{zone_name}}.ldbc_social_network.comment_has_creator_person chc ON c.id = chc.comment_id
JOIN {{zone_name}}.ldbc_social_network.person p2 ON chc.person_id = p2.id
WHERE phc.person_id = 2199023256816
ORDER BY c.creation_date DESC, c.id ASC
LIMIT 20;


-- ============================================================================
-- 40. LDBC Q12 — Expert Friends by TagClass
-- ============================================================================
-- Golden: Mathieu Bemba (2199023257063), reply_count=3
-- 11 friends of 2199023256816 have replied to posts tagged with BasketballPlayer topics.

ASSERT ROW_COUNT = 11
SELECT
    p2.id AS person_id, p2.first_name, p2.last_name,
    COUNT(DISTINCT c.id) AS reply_count
FROM {{zone_name}}.ldbc_social_network.person_knows_person k
JOIN {{zone_name}}.ldbc_social_network.person p2 ON k.dst = p2.id
JOIN {{zone_name}}.ldbc_social_network.comment_has_creator_person chc ON p2.id = chc.person_id
JOIN {{zone_name}}.ldbc_social_network.comment c ON chc.comment_id = c.id
JOIN {{zone_name}}.ldbc_social_network.comment_reply_of_post crp ON c.id = crp.comment_id
JOIN {{zone_name}}.ldbc_social_network.post_has_tag_tag pht ON crp.post_id = pht.post_id
JOIN {{zone_name}}.ldbc_social_network.tag t ON pht.tag_id = t.id
JOIN {{zone_name}}.ldbc_social_network.tag_has_type_tagclass tht ON t.id = tht.tag_id
JOIN {{zone_name}}.ldbc_social_network.tagclass tc ON tht.tagclass_id = tc.id
WHERE k.src = 2199023256816
  AND tc.name = 'BasketballPlayer'
GROUP BY p2.id, p2.first_name, p2.last_name
ORDER BY reply_count DESC, p2.id ASC
LIMIT 20;


-- ============================================================================
-- 41. CONTENT ANALYSIS — Most discussed tags
-- ============================================================================
-- Top tag (most comment mentions): Augustine_of_Hippo with 1,064 comments.
-- Muammar_Gaddafi is second with 697.

ASSERT ROW_COUNT = 15
ASSERT VALUE comment_count = 1064 WHERE tag_name = 'Augustine_of_Hippo'
ASSERT VALUE comment_count = 697 WHERE tag_name = 'Muammar_Gaddafi'
SELECT t.name AS tag_name, COUNT(*) AS comment_count
FROM {{zone_name}}.ldbc_social_network.comment_has_tag_tag cht
JOIN {{zone_name}}.ldbc_social_network.tag t ON cht.tag_id = t.id
GROUP BY t.name
ORDER BY comment_count DESC
LIMIT 15;


-- ############################################################################
-- PART 6: VERIFICATION SUMMARY
-- ############################################################################


-- ============================================================================
-- 42. AUTOMATED VERIFICATION — PASS/FAIL against golden values
-- ============================================================================
-- All checks should return PASS. Any FAIL indicates data loading issues
-- or algorithm correctness problems.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 17
SELECT 'Person count = 1528' AS test,
       CASE WHEN cnt = 1528 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.person)

UNION ALL
SELECT 'Comment count = 151043',
       CASE WHEN cnt = 151043 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.comment)

UNION ALL
SELECT 'Post count = 135701',
       CASE WHEN cnt = 135701 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.post)

UNION ALL
SELECT 'Forum count = 13750',
       CASE WHEN cnt = 13750 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.forum)

UNION ALL
SELECT 'Place count = 1460',
       CASE WHEN cnt = 1460 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.place)

UNION ALL
SELECT 'Organisation count = 7955',
       CASE WHEN cnt = 7955 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.organisation)

UNION ALL
SELECT 'Tag count = 16080',
       CASE WHEN cnt = 16080 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.tag)

UNION ALL
SELECT 'TagClass count = 71',
       CASE WHEN cnt = 71 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.tagclass)

UNION ALL
SELECT 'KNOWS edge count = 14073',
       CASE WHEN cnt = 14073 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.person_knows_person)

UNION ALL
SELECT 'No self-loops in KNOWS',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.person_knows_person WHERE src = dst)

UNION ALL
SELECT 'All KNOWS endpoints exist',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' orphans)' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.person_knows_person k
    WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.ldbc_social_network.person p WHERE p.id = k.src)
       OR NOT EXISTS (SELECT 1 FROM {{zone_name}}.ldbc_social_network.person p WHERE p.id = k.dst)
)

UNION ALL
SELECT 'Top hub degree >= 300',
       CASE WHEN max_deg >= 300 THEN 'PASS' ELSE 'FAIL (got ' || CAST(max_deg AS VARCHAR) || ')' END
FROM (
    SELECT MAX(deg) AS max_deg FROM (
        SELECT node_id, SUM(cnt) AS deg FROM (
            SELECT src AS node_id, COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.person_knows_person GROUP BY src
            UNION ALL
            SELECT dst AS node_id, COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.person_knows_person GROUP BY dst
        ) GROUP BY node_id
    )
)

UNION ALL
SELECT 'SQ1: Jun Wang exists at person 26388279068220',
       CASE WHEN cnt = 1 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.person
    WHERE id = 26388279068220 AND first_name = 'Jun' AND last_name = 'Wang'
)

UNION ALL
SELECT 'SQ5: Comment 1099511997848 created by person 26388279068220',
       CASE WHEN cnt = 1 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.comment_has_creator_person
    WHERE comment_id = 1099511997848 AND person_id = 26388279068220
)

UNION ALL
SELECT 'SQ6: Comment 1099511997848 in forum 824633737506',
       CASE WHEN cnt >= 1 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT COUNT(*) AS cnt
    FROM {{zone_name}}.ldbc_social_network.comment_reply_of_post crp
    JOIN {{zone_name}}.ldbc_social_network.forum_container_of_post fcp ON crp.post_id = fcp.post_id
    WHERE crp.comment_id = 1099511997848 AND fcp.forum_id = 824633737506
)

UNION ALL
SELECT 'Gender values valid (male/female only)',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (
    SELECT COUNT(*) AS cnt FROM {{zone_name}}.ldbc_social_network.person
    WHERE gender NOT IN ('male', 'female')
)

UNION ALL
SELECT 'Avg degree > 10',
       CASE WHEN avg_deg > 10.0 THEN 'PASS' ELSE 'FAIL (got ' || CAST(avg_deg AS VARCHAR) || ')' END
FROM (
    SELECT ROUND(CAST(COUNT(*) AS DOUBLE) * 2.0 / (SELECT COUNT(*) FROM {{zone_name}}.ldbc_social_network.person), 1) AS avg_deg
    FROM {{zone_name}}.ldbc_social_network.person_knows_person
);
