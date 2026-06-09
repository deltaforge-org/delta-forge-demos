-- ============================================================================
-- Graph Bipartite — Movie Recommendation Engine — Setup Script
-- ============================================================================
-- Creates a bipartite graph with 25 subscribers and 20 movies connected by
-- 84 rating edges. The two vertex types (subscriber, movie) form distinct
-- partitions with edges crossing between them. This structure enables
-- collaborative filtering, genre analysis, and recommendation queries.
--
-- Tables:
--   1. entities  — 45 vertices (25 subscribers + 20 movies)
--   2. ratings   — 84 directed edges (subscriber → movie)
--
-- Graph:
--   movie_recs   — bipartite graph with entity_type as node type
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.movie_recs
    COMMENT 'Bipartite movie recommendation graph — subscribers, movies, and rating edges';

-- ============================================================================
-- TABLE 1: entities — 45 vertices (25 subscribers + 20 movies)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.movie_recs.entities (
    id              BIGINT,
    name            STRING,
    entity_type     STRING,
    genre           STRING,
    join_year       INT,
    release_year    INT
) LOCATION 'graph-bipartite/entities';


-- 25 subscribers (id 1-25)
INSERT INTO {{zone_name}}.movie_recs.entities VALUES
    (1,  'User_1',  'subscriber', NULL, 2020, NULL),
    (2,  'User_2',  'subscriber', NULL, 2021, NULL),
    (3,  'User_3',  'subscriber', NULL, 2022, NULL),
    (4,  'User_4',  'subscriber', NULL, 2023, NULL),
    (5,  'User_5',  'subscriber', NULL, 2024, NULL),
    (6,  'User_6',  'subscriber', NULL, 2019, NULL),
    (7,  'User_7',  'subscriber', NULL, 2020, NULL),
    (8,  'User_8',  'subscriber', NULL, 2021, NULL),
    (9,  'User_9',  'subscriber', NULL, 2022, NULL),
    (10, 'User_10', 'subscriber', NULL, 2023, NULL),
    (11, 'User_11', 'subscriber', NULL, 2024, NULL),
    (12, 'User_12', 'subscriber', NULL, 2019, NULL),
    (13, 'User_13', 'subscriber', NULL, 2020, NULL),
    (14, 'User_14', 'subscriber', NULL, 2021, NULL),
    (15, 'User_15', 'subscriber', NULL, 2022, NULL),
    (16, 'User_16', 'subscriber', NULL, 2023, NULL),
    (17, 'User_17', 'subscriber', NULL, 2024, NULL),
    (18, 'User_18', 'subscriber', NULL, 2019, NULL),
    (19, 'User_19', 'subscriber', NULL, 2020, NULL),
    (20, 'User_20', 'subscriber', NULL, 2021, NULL),
    (21, 'User_21', 'subscriber', NULL, 2022, NULL),
    (22, 'User_22', 'subscriber', NULL, 2023, NULL),
    (23, 'User_23', 'subscriber', NULL, 2024, NULL),
    (24, 'User_24', 'subscriber', NULL, 2019, NULL),
    (25, 'User_25', 'subscriber', NULL, 2020, NULL);

-- 20 movies (id 101-120)
INSERT INTO {{zone_name}}.movie_recs.entities VALUES
    (101, 'The_Matrix',    'movie', 'action',   NULL, 1999),
    (102, 'Inception',     'movie', 'sci-fi',   NULL, 2010),
    (103, 'Interstellar',  'movie', 'sci-fi',   NULL, 2014),
    (104, 'Pulp_Fiction',  'movie', 'thriller', NULL, 1994),
    (105, 'Fight_Club',    'movie', 'thriller', NULL, 1999),
    (106, 'The_Godfather', 'movie', 'drama',    NULL, 1972),
    (107, 'Dark_Knight',   'movie', 'action',   NULL, 2008),
    (108, 'Forrest_Gump',  'movie', 'drama',    NULL, 1994),
    (109, 'Shawshank',     'movie', 'drama',    NULL, 1994),
    (110, 'Goodfellas',    'movie', 'drama',    NULL, 1990),
    (111, 'Parasite',      'movie', 'thriller', NULL, 2019),
    (112, 'Whiplash',      'movie', 'drama',    NULL, 2014),
    (113, 'Arrival',       'movie', 'sci-fi',   NULL, 2016),
    (114, 'Blade_Runner',  'movie', 'sci-fi',   NULL, 1982),
    (115, 'Mad_Max',       'movie', 'action',   NULL, 2015),
    (116, 'Alien',         'movie', 'horror',   NULL, 1979),
    (117, 'Jaws',          'movie', 'horror',   NULL, 1975),
    (118, 'Psycho',        'movie', 'horror',   NULL, 1960),
    (119, 'Vertigo',       'movie', 'thriller', NULL, 1958),
    (120, 'Rear_Window',   'movie', 'thriller', NULL, 1954);

-- ============================================================================
-- TABLE 2: ratings — 84 directed edges (subscriber → movie)
-- ============================================================================
-- Deterministic generation: each user rates 3-4 movies based on prime
-- multipliers (uid * prime) % 20 + 101. The_Matrix (101) is boosted as
-- the most popular movie (12 ratings). Inception (102) is second (9 ratings).
-- All 45 nodes form a single connected component.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.movie_recs.ratings (
    id              BIGINT,
    src             BIGINT,
    dst             BIGINT,
    weight          DOUBLE,
    rating_type     STRING,
    watch_date      STRING
) LOCATION 'graph-bipartite/ratings';


INSERT INTO {{zone_name}}.movie_recs.ratings VALUES
    (1, 1, 101, 2.7, 'watched', '2024-08-19'),
    (2, 1, 108, 4.0, 'favorited', '2025-03-21'),
    (3, 1, 112, 1.9, 'recommended', '2025-07-25'),
    (4, 1, 114, 2.8, 'favorited', '2025-09-27'),
    (5, 2, 103, 2.3, 'watched', '2024-11-08'),
    (6, 2, 107, 2.0, 'favorited', '2025-03-07'),
    (7, 2, 115, 1.6, 'watched', '2025-11-15'),
    (8, 3, 101, 2.1, 'recommended', '2024-09-23'),
    (9, 3, 102, 1.5, 'watched', '2024-10-24'),
    (10, 3, 114, 2.5, 'watched', '2025-11-01'),
    (11, 3, 120, 3.0, 'watched', '2024-05-02'),
    (12, 4, 105, 3.3, 'favorited', '2025-02-09'),
    (13, 4, 109, 2.9, 'recommended', '2025-06-13'),
    (14, 4, 113, 2.4, 'watched', '2025-10-17'),
    (15, 5, 102, 1.8, 'recommended', '2024-11-28'),
    (16, 5, 106, 3.2, 'watched', '2025-03-27'),
    (17, 5, 116, 2.8, 'favorited', '2024-02-02'),
    (18, 6, 101, 3.1, 'recommended', '2024-11-14'),
    (19, 6, 103, 4.8, 'favorited', '2025-01-11'),
    (20, 6, 107, 4.1, 'recommended', '2025-05-15'),
    (21, 6, 119, 2.1, 'recommended', '2024-05-22'),
    (22, 7, 110, 4.5, 'watched', '2025-09-05'),
    (23, 7, 112, 3.2, 'recommended', '2025-11-07'),
    (24, 7, 118, 3.0, 'recommended', '2024-05-08'),
    (25, 8, 101, 2.5, 'favorited', '2024-12-18'),
    (26, 8, 105, 1.6, 'recommended', '2025-04-17'),
    (27, 8, 109, 4.7, 'watched', '2025-08-21'),
    (28, 8, 117, 2.9, 'recommended', '2024-04-24'),
    (29, 9, 101, 4.2, 'recommended', '2024-12-05'),
    (30, 9, 104, 2.9, 'recommended', '2025-04-03'),
    (31, 9, 118, 2.4, 'favorited', '2024-06-12'),
    (32, 9, 120, 2.9, 'watched', '2024-08-14'),
    (33, 10, 102, 2.6, 'favorited', '2025-02-18'),
    (34, 10, 111, 1.1, 'favorited', '2025-11-27'),
    (35, 11, 102, 2.7, 'recommended', '2025-03-05'),
    (36, 11, 104, 1.1, 'favorited', '2025-05-07'),
    (37, 11, 118, 1.8, 'watched', '2024-07-16'),
    (38, 12, 101, 1.2, 'recommended', '2025-02-21'),
    (39, 12, 105, 3.9, 'watched', '2025-06-25'),
    (40, 12, 113, 1.2, 'recommended', '2024-02-28'),
    (41, 12, 117, 3.9, 'watched', '2024-07-02'),
    (42, 13, 104, 3.3, 'watched', '2025-06-11'),
    (43, 13, 110, 4.2, 'watched', '2025-12-17'),
    (44, 13, 112, 4.4, 'recommended', '2024-02-14'),
    (45, 14, 103, 1.8, 'watched', '2025-05-27'),
    (46, 14, 115, 1.1, 'watched', '2024-06-04'),
    (47, 14, 119, 3.6, 'favorited', '2024-10-08'),
    (48, 15, 101, 2.3, 'recommended', '2025-04-12'),
    (49, 15, 102, 3.4, 'watched', '2025-05-13'),
    (50, 15, 106, 3.7, 'favorited', '2025-09-17'),
    (51, 15, 116, 2.5, 'recommended', '2024-07-22'),
    (52, 16, 109, 4.4, 'recommended', '2024-01-02'),
    (53, 16, 113, 2.6, 'watched', '2024-05-06'),
    (54, 16, 117, 4.8, 'favorited', '2024-09-10'),
    (55, 17, 102, 3.7, 'recommended', '2025-06-17'),
    (56, 17, 108, 3.8, 'recommended', '2025-12-23'),
    (57, 17, 120, 4.2, 'recommended', '2024-12-30'),
    (58, 18, 101, 3.3, 'recommended', '2025-06-03'),
    (59, 18, 107, 2.3, 'recommended', '2025-12-09'),
    (60, 18, 115, 2.3, 'favorited', '2024-08-12'),
    (61, 18, 119, 4.3, 'recommended', '2024-12-16'),
    (62, 19, 108, 1.8, 'favorited', '2024-01-22'),
    (63, 19, 110, 3.8, 'watched', '2024-03-24'),
    (64, 19, 114, 3.6, 'favorited', '2024-07-28'),
    (65, 20, 101, 2.7, 'favorited', '2025-07-07'),
    (66, 20, 102, 4.2, 'recommended', '2025-08-08'),
    (67, 21, 101, 4.4, 'recommended', '2025-07-24'),
    (68, 21, 108, 3.8, 'watched', '2024-02-26'),
    (69, 21, 112, 3.5, 'favorited', '2024-06-30'),
    (70, 21, 114, 3.3, 'watched', '2024-09-02'),
    (71, 22, 101, 2.1, 'watched', '2025-08-11'),
    (72, 22, 103, 2.9, 'recommended', '2025-10-13'),
    (73, 22, 107, 4.4, 'watched', '2024-02-12'),
    (74, 22, 115, 3.5, 'recommended', '2024-10-20'),
    (75, 23, 102, 4.6, 'recommended', '2025-09-29'),
    (76, 23, 114, 2.9, 'recommended', '2024-10-06'),
    (77, 23, 120, 4.1, 'recommended', '2025-04-07'),
    (78, 24, 101, 1.5, 'recommended', '2025-09-15'),
    (79, 24, 105, 2.8, 'watched', '2024-01-14'),
    (80, 24, 109, 4.1, 'favorited', '2024-05-18'),
    (81, 24, 113, 1.4, 'recommended', '2024-09-22'),
    (82, 25, 102, 4.9, 'favorited', '2025-11-03'),
    (83, 25, 106, 4.2, 'recommended', '2024-03-02'),
    (84, 25, 116, 2.2, 'watched', '2025-01-07');

-- ============================================================================
-- PHYSICAL LAYOUT — Z-ORDER for fast data skipping
-- ============================================================================
-- The data was inserted in id-generation order, which has reasonable locality
-- for `id` but scatters frequent filter columns (entity_type, genre) across
-- files.  Z-ORDER rewrites files so rows with similar values on the ordering
-- keys co-locate, giving Parquet min/max statistics much tighter ranges per
-- file.  This benefits three hot paths:
--
--   1. CSR build from the ratings table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `id` co-location lets the Parquet reader skip
--      almost every row group for targeted subscriber or movie lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE e.entity_type = 'movie' AND e.genre = 'Sci-Fi'` skip entire
--      files instead of reading the whole bipartite vertex table.
--
-- One-time cost at setup; every subsequent query benefits.  These OPTIMIZE
-- statements also compact small files written by the batched rating load.
OPTIMIZE {{zone_name}}.movie_recs.entities
    ZORDER BY (id, entity_type, genre);

OPTIMIZE {{zone_name}}.movie_recs.ratings
    ZORDER BY (src, dst);

-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
-- Bipartite graph: subscribers and movies as two vertex types, ratings as
-- directed edges from subscriber to movie. Weight = star rating (1.0-5.0).
-- ============================================================================
CREATE GRAPH IF NOT EXISTS {{zone_name}}.movie_recs.movie_recs
    VERTEX TABLE {{zone_name}}.movie_recs.entities ID COLUMN id NODE TYPE COLUMN entity_type NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.movie_recs.ratings SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN rating_type
    DIRECTED;

-- ============================================================================
-- WARM CSR CACHE — Pre-build the Compressed Sparse Row topology
-- ============================================================================
-- CREATE GRAPHCSR writes the binary .dcsr file to disk, so the first Cypher
-- query loads in ~200 ms instead of rebuilding from Delta tables (6-14 s for
-- large graphs). Safe to re-run after bulk edge loads to refresh the cache.
CREATE GRAPHCSR {{zone_name}}.movie_recs.movie_recs;
