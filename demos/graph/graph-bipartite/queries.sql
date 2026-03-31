-- ############################################################################
-- ############################################################################
--
--   MOVIE RECOMMENDATION ENGINE — BIPARTITE GRAPH
--   25 Subscribers / 20 Movies / 84 Rating Edges
--
-- ############################################################################
-- ############################################################################
--
-- A streaming platform's bipartite graph: subscribers on one side, movies
-- on the other, connected by rating edges (1-5 stars). The two vertex
-- partitions enable collaborative filtering, genre preference analysis,
-- and taste similarity scoring via cross-type traversal patterns.
--
-- PART 1: DATA EXPLORATION (queries 1-5)
--   Vertex and edge inventory, popularity rankings.
--
-- PART 2: RECOMMENDATION PATTERNS (queries 6-8)
--   Collaborative filtering, similarity, genre preferences.
--
-- PART 3: GRAPH ALGORITHMS (queries 9-12)
--   PageRank, degree centrality, connected components, verification.
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA EXPLORATION
-- ############################################################################


-- ============================================================================
-- 1. ALL SUBSCRIBERS — Platform user base with join dates
-- ============================================================================
-- The product team wants a roster of all 25 subscribers and when they
-- joined the platform.

ASSERT ROW_COUNT = 25
ASSERT VALUE join_year = 2020 WHERE name = 'User_1'
ASSERT VALUE entity_type = 'subscriber' WHERE name = 'User_1'
USE {{zone_name}}.movie_recs.movie_recs
MATCH (n)
WHERE n.entity_type = 'subscriber'
RETURN n.id AS id, n.name AS name, n.join_year AS join_year, n.entity_type AS entity_type
ORDER BY id;


-- ============================================================================
-- 2. ALL MOVIES — Complete catalog with genres and release years
-- ============================================================================
-- The content team needs the full movie catalog: genre classification
-- and release year for licensing decisions.

ASSERT ROW_COUNT = 20
ASSERT VALUE genre = 'action' WHERE name = 'The_Matrix'
ASSERT VALUE release_year = 1999 WHERE name = 'The_Matrix'
ASSERT VALUE genre = 'sci-fi' WHERE name = 'Inception'
USE {{zone_name}}.movie_recs.movie_recs
MATCH (n)
WHERE n.entity_type = 'movie'
RETURN n.id AS id, n.name AS name, n.genre AS genre, n.release_year AS release_year
ORDER BY id;


-- ============================================================================
-- 3. RATING OVERVIEW — All 84 subscriber-to-movie ratings
-- ============================================================================
-- Complete edge inventory: who rated what, how highly, and when.

ASSERT ROW_COUNT = 84
ASSERT VALUE rating = 2.7 WHERE subscriber = 'User_1' AND movie = 'The_Matrix'
ASSERT VALUE rating = 4.0 WHERE subscriber = 'User_1' AND movie = 'Forrest_Gump'
USE {{zone_name}}.movie_recs.movie_recs
MATCH (u)-[r]->(m)
WHERE u.entity_type = 'subscriber'
RETURN u.id AS uid, m.id AS mid, u.name AS subscriber, m.name AS movie,
       r.weight AS rating, r.rating_type AS type, r.watch_date AS watched
ORDER BY uid, mid;


-- ============================================================================
-- 4. MOST POPULAR MOVIES — Ranked by number of ratings received
-- ============================================================================
-- Which movies attract the most engagement? The_Matrix leads with 12
-- ratings, followed by Inception with 9. Long-tail movies like Parasite
-- have only 1 rating — cold-start candidates for promotion.

ASSERT ROW_COUNT = 20
ASSERT VALUE rating_count = 12 WHERE movie = 'The_Matrix'
ASSERT VALUE rating_count = 9 WHERE movie = 'Inception'
ASSERT VALUE rating_count = 1 WHERE movie = 'Parasite'
USE {{zone_name}}.movie_recs.movie_recs
MATCH (u)-[r]->(m)
WHERE m.entity_type = 'movie'
RETURN m.name AS movie, count(r) AS rating_count,
       round(avg(r.weight), 2) AS avg_rating
ORDER BY rating_count DESC, avg_rating DESC;


-- ============================================================================
-- 5. HIGHEST RATED MOVIES — By average star rating
-- ============================================================================
-- Quality vs. popularity: Goodfellas tops with 4.17 avg from 3 ratings,
-- while The_Matrix's 12 ratings average only 2.68. Small sample sizes
-- inflate averages — combine with rating count for true quality signals.

ASSERT ROW_COUNT = 20
ASSERT VALUE avg_rating = 4.17 WHERE movie = 'Goodfellas'
ASSERT VALUE avg_rating = 4.03 WHERE movie = 'Shawshank'
ASSERT VALUE avg_rating = 2.68 WHERE movie = 'The_Matrix'
USE {{zone_name}}.movie_recs.movie_recs
MATCH (u)-[r]->(m)
WHERE m.entity_type = 'movie'
RETURN m.name AS movie, round(avg(r.weight), 2) AS avg_rating,
       count(r) AS num_ratings, m.genre AS genre
ORDER BY avg_rating DESC;


-- ############################################################################
-- PART 2: RECOMMENDATION PATTERNS
-- ############################################################################


-- ============================================================================
-- 6. COLLABORATIVE FILTERING — "Users who liked The_Matrix also liked..."
-- ============================================================================
-- The core recommendation pattern: 2-hop traversal through the bipartite
-- graph. Start at The_Matrix, walk back to users who rated it, then
-- forward to other movies those users rated. Inception appears 3 times
-- (3 Matrix fans also rated it), making it the top recommendation.

ASSERT ROW_COUNT >= 5
ASSERT VALUE co_ratings = 3 WHERE recommended_movie = 'Inception'
ASSERT VALUE co_ratings = 3 WHERE recommended_movie = 'Blade_Runner'
USE {{zone_name}}.movie_recs.movie_recs
MATCH (m1)<-[r1]-(u)-[r2]->(m2)
WHERE m1.name = 'The_Matrix' AND m2.name <> 'The_Matrix'
RETURN m2.name AS recommended_movie, count(DISTINCT u) AS co_ratings,
       round(avg(r2.weight), 2) AS avg_co_rating
ORDER BY co_ratings DESC, avg_co_rating DESC;


-- ============================================================================
-- 7. USER TASTE SIMILARITY — Subscribers who share the most movies
-- ============================================================================
-- Pairs of users with overlapping taste: User_1 and User_21 share 4
-- movies (The_Matrix, Forrest_Gump, Whiplash, Blade_Runner) — the most
-- similar pair. These high-overlap pairs are the basis for user-user
-- collaborative filtering.

ASSERT ROW_COUNT = 20
ASSERT VALUE shared_count = 4 WHERE user_a = 'User_1' AND user_b = 'User_21'
USE {{zone_name}}.movie_recs.movie_recs
MATCH (u1)-[]->(m)<-[]-(u2)
WHERE u1.entity_type = 'subscriber' AND u2.entity_type = 'subscriber'
  AND u1.id < u2.id
RETURN u1.name AS user_a, u2.name AS user_b,
       count(DISTINCT m) AS shared_count
ORDER BY shared_count DESC, user_a
LIMIT 20;


-- ============================================================================
-- 8. GENRE PREFERENCES — What genres does each subscriber prefer?
-- ============================================================================
-- Join ratings with movie genre metadata to build per-user taste profiles.
-- User_1 prefers drama (2 movies: Forrest_Gump, Whiplash) over sci-fi (1)
-- and action (1). These profiles drive content recommendations.

ASSERT ROW_COUNT >= 20
ASSERT VALUE movies_rated = 2 WHERE subscriber = 'User_1' AND genre = 'drama'
USE {{zone_name}}.movie_recs.movie_recs
MATCH (u)-[r]->(m)
WHERE u.entity_type = 'subscriber'
RETURN u.name AS subscriber, m.genre AS genre,
       count(m) AS movies_rated, round(avg(r.weight), 2) AS avg_rating
ORDER BY subscriber, movies_rated DESC;


-- ############################################################################
-- PART 3: GRAPH ALGORITHMS
-- ############################################################################


-- ============================================================================
-- 9. PAGERANK — Most influential nodes in the bipartite graph
-- ============================================================================
-- In a bipartite rating graph, PageRank reveals which movies accumulate
-- the most "recommendation weight" from subscribers. The_Matrix should
-- rank highest due to its 12 incoming rating edges.

ASSERT ROW_COUNT = 10
USE {{zone_name}}.movie_recs.movie_recs
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 10. DEGREE CENTRALITY — Most-connected nodes
-- ============================================================================
-- In a bipartite graph, movie nodes have only in-degree (ratings received)
-- and subscriber nodes have only out-degree (ratings given). The_Matrix
-- leads with in-degree 12, total degree 12.

ASSERT ROW_COUNT = 10
ASSERT VALUE total_degree = 12 WHERE node_id = 101
USE {{zone_name}}.movie_recs.movie_recs
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 11. CONNECTED COMPONENTS — Verify the graph is fully connected
-- ============================================================================
-- A healthy recommendation graph has 1 connected component — every
-- subscriber can reach every movie through some path. Disconnected
-- components indicate cold-start users or orphaned movies.

ASSERT ROW_COUNT = 1
ASSERT VALUE size = 45 WHERE component_id = 0
USE {{zone_name}}.movie_recs.movie_recs
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 12. VERIFY — Structural integrity check
-- ============================================================================
-- Cross-cutting sanity check: total vertices, total edges, The_Matrix
-- as most popular movie, and correct entity type distribution.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_entities = 45
ASSERT VALUE total_ratings = 84
ASSERT VALUE subscribers = 25
ASSERT VALUE total_movies = 20
ASSERT VALUE matrix_ratings = 12
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.movie_recs.entities)                                              AS total_entities,
    (SELECT COUNT(*) FROM {{zone_name}}.movie_recs.ratings)                                               AS total_ratings,
    (SELECT COUNT(*) FROM {{zone_name}}.movie_recs.entities WHERE entity_type = 'subscriber')             AS subscribers,
    (SELECT COUNT(*) FROM {{zone_name}}.movie_recs.entities WHERE entity_type = 'movie')                  AS total_movies,
    (SELECT COUNT(*) FROM {{zone_name}}.movie_recs.ratings WHERE dst = 101)                               AS matrix_ratings;
