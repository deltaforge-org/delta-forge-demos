-- ============================================================================
-- LDBC Social Network Benchmark — Full Model Setup Script
-- ============================================================================
-- Loads the complete LDBC SNB Scale Factor 0.1 dataset into Delta tables
-- and creates a named graph for algorithm verification.
--
-- Data source: https://ldbcouncil.org/benchmarks/snb/
-- Format: pipe-delimited CSV with headers
--
-- Entities (8 vertex types):
--   Person (1,528)  Comment (151,043)  Post (135,701)  Forum (13,750)
--   Place (1,460)   Organisation (7,955)  Tag (16,080)  TagClass (71)
--
-- Relationships (23 edge types):
--   person_knows_person (14,073)       comment_has_creator_person (151,043)
--   post_has_creator_person (135,701)  person_is_located_in_place (1,528)
--   comment_is_located_in_place (151,043) post_is_located_in_place (135,701)
--   comment_reply_of_comment (76,787)  comment_reply_of_post (74,256)
--   comment_has_tag_tag (191,303)      post_has_tag_tag (51,118)
--   forum_has_tag_tag (47,697)         forum_container_of_post (135,701)
--   forum_has_member_person (123,268)  forum_has_moderator_person (13,750)
--   person_has_interest_tag (35,475)   person_likes_comment (62,225)
--   person_likes_post (47,215)         person_study_at_organisation (1,209)
--   person_work_at_organisation (3,313) organisation_is_located_in_place (7,955)
--   place_is_part_of_place (1,454)     tag_has_type_tagclass (16,080)
--   tagclass_is_subclass_of_tagclass (70)
--
-- Graph:
--   {{zone_name}}.ldbc_social_network.ldbc_social_network — Person vertices + KNOWS edges (core social graph)
-- ============================================================================
-- ############################################################################
-- STEP 1: Zone & Schemas
-- ############################################################################
-- This demo creates 33 external tables, 33 Delta tables, and 1 graph.
-- Two schemas keep staging separate from the materialized layer:
--   {{zone_name}}.ldbc_snb_raw   — External CSV tables (staging / read-only)
--   {{zone_name}}.ldbc_social_network — Delta tables + graph definition (queryable)
-- The cleanup script drops both schemas and everything in them.
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.ldbc_snb_raw
    COMMENT 'LDBC SNB — external CSV staging tables (pipe-delimited)';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.ldbc_social_network
    COMMENT 'LDBC SNB SF0.1 — Delta tables, Person-KNOWS graph, and Cypher/SQL queries';
-- ############################################################################
-- STEP 2: External Tables — Raw CSV Readers (pipe-delimited)
-- ############################################################################
-- Each external table points to a pipe-delimited CSV file from the LDBC
-- datagen output. Original LDBC headers with dots (e.g. Person.id) and
-- duplicate column names have been renamed for compatibility.
-- Column names are auto-sanitized by the CSV handler (camelCase → snake_case).
-- ############################################################################
-- === Static Entities ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.place
USING CSV LOCATION 'graph-ldbc-snb/place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.organisation
USING CSV LOCATION 'graph-ldbc-snb/organisation.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.tag
USING CSV LOCATION 'graph-ldbc-snb/tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.tagclass
USING CSV LOCATION 'graph-ldbc-snb/tagclass.csv'
OPTIONS (header = 'true', delimiter = '|');
-- === Static Edges ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.organisation_is_located_in_place
USING CSV LOCATION 'graph-ldbc-snb/organisation_is_located_in_place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.place_is_part_of_place
USING CSV LOCATION 'graph-ldbc-snb/place_is_part_of_place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.tag_has_type_tagclass
USING CSV LOCATION 'graph-ldbc-snb/tag_has_type_tagclass.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.tagclass_is_subclass_of_tagclass
USING CSV LOCATION 'graph-ldbc-snb/tagclass_is_subclass_of_tagclass.csv'
OPTIONS (header = 'true', delimiter = '|');
-- === Dynamic Entities ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person
USING CSV LOCATION 'graph-ldbc-snb/person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.comment
USING CSV LOCATION 'graph-ldbc-snb/comment.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.post
USING CSV LOCATION 'graph-ldbc-snb/post.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.forum
USING CSV LOCATION 'graph-ldbc-snb/forum.csv'
OPTIONS (header = 'true', delimiter = '|');
-- === Dynamic Edges ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person_knows_person
USING CSV LOCATION 'graph-ldbc-snb/person_knows_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.comment_has_creator_person
USING CSV LOCATION 'graph-ldbc-snb/comment_has_creator_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.comment_has_tag_tag
USING CSV LOCATION 'graph-ldbc-snb/comment_has_tag_tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.comment_is_located_in_place
USING CSV LOCATION 'graph-ldbc-snb/comment_is_located_in_place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.comment_reply_of_comment
USING CSV LOCATION 'graph-ldbc-snb/comment_reply_of_comment.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.comment_reply_of_post
USING CSV LOCATION 'graph-ldbc-snb/comment_reply_of_post.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.forum_container_of_post
USING CSV LOCATION 'graph-ldbc-snb/forum_container_of_post.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.forum_has_member_person
USING CSV LOCATION 'graph-ldbc-snb/forum_has_member_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.forum_has_moderator_person
USING CSV LOCATION 'graph-ldbc-snb/forum_has_moderator_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.forum_has_tag_tag
USING CSV LOCATION 'graph-ldbc-snb/forum_has_tag_tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person_email
USING CSV LOCATION 'graph-ldbc-snb/person_email_emailaddress.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person_has_interest_tag
USING CSV LOCATION 'graph-ldbc-snb/person_has_interest_tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person_is_located_in_place
USING CSV LOCATION 'graph-ldbc-snb/person_is_located_in_place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person_likes_comment
USING CSV LOCATION 'graph-ldbc-snb/person_likes_comment.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person_likes_post
USING CSV LOCATION 'graph-ldbc-snb/person_likes_post.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person_speaks_language
USING CSV LOCATION 'graph-ldbc-snb/person_speaks_language.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person_study_at_organisation
USING CSV LOCATION 'graph-ldbc-snb/person_study_at_organisation.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.person_work_at_organisation
USING CSV LOCATION 'graph-ldbc-snb/person_work_at_organisation.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.post_has_creator_person
USING CSV LOCATION 'graph-ldbc-snb/post_has_creator_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.post_has_tag_tag
USING CSV LOCATION 'graph-ldbc-snb/post_has_tag_tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ldbc_snb_raw.post_is_located_in_place
USING CSV LOCATION 'graph-ldbc-snb/post_is_located_in_place.csv'
OPTIONS (header = 'true', delimiter = '|');

-- ############################################################################
-- STEP 3: Delta Tables — Materialized with Proper Types
-- ############################################################################
-- CTAS (CREATE TABLE AS SELECT) from external CSV tables into Delta format.
-- All IDs cast to BIGINT, timestamps to BIGINT (epoch millis).
-- Column names from CSV headers are auto-sanitized: camelCase → snake_case.
-- e.g. firstName → first_name, creationDate → creation_date
-- ############################################################################
-- === Static Entity Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.place
LOCATION 'graph-ldbc-snb/delta/place'
AS SELECT
    CAST(id AS BIGINT) AS id,
    name,
    url,
    type
FROM {{zone_name}}.ldbc_snb_raw.place;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.organisation
LOCATION 'graph-ldbc-snb/delta/organisation'
AS SELECT
    CAST(id AS BIGINT) AS id,
    type,
    name,
    url
FROM {{zone_name}}.ldbc_snb_raw.organisation;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.tag
LOCATION 'graph-ldbc-snb/delta/tag'
AS SELECT
    CAST(id AS BIGINT) AS id,
    name,
    url
FROM {{zone_name}}.ldbc_snb_raw.tag;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.tagclass
LOCATION 'graph-ldbc-snb/delta/tagclass'
AS SELECT
    CAST(id AS BIGINT) AS id,
    name,
    url
FROM {{zone_name}}.ldbc_snb_raw.tagclass;

-- === Static Edge Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.organisation_is_located_in_place
LOCATION 'graph-ldbc-snb/delta/organisation_is_located_in_place'
AS SELECT
    CAST(organisation_id AS BIGINT) AS organisation_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.ldbc_snb_raw.organisation_is_located_in_place;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.place_is_part_of_place
LOCATION 'graph-ldbc-snb/delta/place_is_part_of_place'
AS SELECT
    CAST(place_id AS BIGINT) AS place_id,
    CAST(parent_place_id AS BIGINT) AS parent_place_id
FROM {{zone_name}}.ldbc_snb_raw.place_is_part_of_place;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.tag_has_type_tagclass
LOCATION 'graph-ldbc-snb/delta/tag_has_type_tagclass'
AS SELECT
    CAST(tag_id AS BIGINT) AS tag_id,
    CAST(tagclass_id AS BIGINT) AS tagclass_id
FROM {{zone_name}}.ldbc_snb_raw.tag_has_type_tagclass;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.tagclass_is_subclass_of_tagclass
LOCATION 'graph-ldbc-snb/delta/tagclass_is_subclass_of_tagclass'
AS SELECT
    CAST(tagclass_id AS BIGINT) AS tagclass_id,
    CAST(parent_tagclass_id AS BIGINT) AS parent_tagclass_id
FROM {{zone_name}}.ldbc_snb_raw.tagclass_is_subclass_of_tagclass;

-- === Dynamic Entity Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person
LOCATION 'graph-ldbc-snb/delta/person'
AS SELECT
    CAST(id AS BIGINT) AS id,
    first_name,
    last_name,
    gender,
    CAST(birthday AS BIGINT) AS birthday,
    CAST(creation_date AS BIGINT) AS creation_date,
    location_ip,
    browser_used
FROM {{zone_name}}.ldbc_snb_raw.person;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment
LOCATION 'graph-ldbc-snb/delta/comment'
AS SELECT
    CAST(id AS BIGINT) AS id,
    CAST(creation_date AS BIGINT) AS creation_date,
    location_ip,
    browser_used,
    content,
    CAST(length AS INT) AS length
FROM {{zone_name}}.ldbc_snb_raw.comment;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.post
LOCATION 'graph-ldbc-snb/delta/post'
AS SELECT
    CAST(id AS BIGINT) AS id,
    image_file,
    CAST(creation_date AS BIGINT) AS creation_date,
    location_ip,
    browser_used,
    language,
    content,
    CAST(length AS INT) AS length
FROM {{zone_name}}.ldbc_snb_raw.post;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum
LOCATION 'graph-ldbc-snb/delta/forum'
AS SELECT
    CAST(id AS BIGINT) AS id,
    title,
    CAST(creation_date AS BIGINT) AS creation_date
FROM {{zone_name}}.ldbc_snb_raw.forum;

-- === Dynamic Edge Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_knows_person
LOCATION 'graph-ldbc-snb/delta/person_knows_person'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(creation_date AS BIGINT) AS creation_date
FROM {{zone_name}}.ldbc_snb_raw.person_knows_person;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_has_creator_person
LOCATION 'graph-ldbc-snb/delta/comment_has_creator_person'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(person_id AS BIGINT) AS person_id
FROM {{zone_name}}.ldbc_snb_raw.comment_has_creator_person;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_has_tag_tag
LOCATION 'graph-ldbc-snb/delta/comment_has_tag_tag'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.ldbc_snb_raw.comment_has_tag_tag;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_is_located_in_place
LOCATION 'graph-ldbc-snb/delta/comment_is_located_in_place'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.ldbc_snb_raw.comment_is_located_in_place;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_reply_of_comment
LOCATION 'graph-ldbc-snb/delta/comment_reply_of_comment'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(reply_to_comment_id AS BIGINT) AS reply_to_comment_id
FROM {{zone_name}}.ldbc_snb_raw.comment_reply_of_comment;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_reply_of_post
LOCATION 'graph-ldbc-snb/delta/comment_reply_of_post'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(post_id AS BIGINT) AS post_id
FROM {{zone_name}}.ldbc_snb_raw.comment_reply_of_post;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum_container_of_post
LOCATION 'graph-ldbc-snb/delta/forum_container_of_post'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(post_id AS BIGINT) AS post_id
FROM {{zone_name}}.ldbc_snb_raw.forum_container_of_post;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum_has_member_person
LOCATION 'graph-ldbc-snb/delta/forum_has_member_person'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(person_id AS BIGINT) AS person_id,
    CAST(join_date AS BIGINT) AS join_date
FROM {{zone_name}}.ldbc_snb_raw.forum_has_member_person;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum_has_moderator_person
LOCATION 'graph-ldbc-snb/delta/forum_has_moderator_person'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(person_id AS BIGINT) AS person_id
FROM {{zone_name}}.ldbc_snb_raw.forum_has_moderator_person;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum_has_tag_tag
LOCATION 'graph-ldbc-snb/delta/forum_has_tag_tag'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.ldbc_snb_raw.forum_has_tag_tag;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_email
LOCATION 'graph-ldbc-snb/delta/person_email'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    email
FROM {{zone_name}}.ldbc_snb_raw.person_email;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_has_interest_tag
LOCATION 'graph-ldbc-snb/delta/person_has_interest_tag'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.ldbc_snb_raw.person_has_interest_tag;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_is_located_in_place
LOCATION 'graph-ldbc-snb/delta/person_is_located_in_place'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.ldbc_snb_raw.person_is_located_in_place;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_likes_comment
LOCATION 'graph-ldbc-snb/delta/person_likes_comment'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(creation_date AS BIGINT) AS creation_date
FROM {{zone_name}}.ldbc_snb_raw.person_likes_comment;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_likes_post
LOCATION 'graph-ldbc-snb/delta/person_likes_post'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(post_id AS BIGINT) AS post_id,
    CAST(creation_date AS BIGINT) AS creation_date
FROM {{zone_name}}.ldbc_snb_raw.person_likes_post;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_speaks_language
LOCATION 'graph-ldbc-snb/delta/person_speaks_language'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    language
FROM {{zone_name}}.ldbc_snb_raw.person_speaks_language;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_study_at_organisation
LOCATION 'graph-ldbc-snb/delta/person_study_at_organisation'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(organisation_id AS BIGINT) AS organisation_id,
    CAST(class_year AS INT) AS class_year
FROM {{zone_name}}.ldbc_snb_raw.person_study_at_organisation;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_work_at_organisation
LOCATION 'graph-ldbc-snb/delta/person_work_at_organisation'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(organisation_id AS BIGINT) AS organisation_id,
    CAST(work_from AS INT) AS work_from
FROM {{zone_name}}.ldbc_snb_raw.person_work_at_organisation;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.post_has_creator_person
LOCATION 'graph-ldbc-snb/delta/post_has_creator_person'
AS SELECT
    CAST(post_id AS BIGINT) AS post_id,
    CAST(person_id AS BIGINT) AS person_id
FROM {{zone_name}}.ldbc_snb_raw.post_has_creator_person;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.post_has_tag_tag
LOCATION 'graph-ldbc-snb/delta/post_has_tag_tag'
AS SELECT
    CAST(post_id AS BIGINT) AS post_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.ldbc_snb_raw.post_has_tag_tag;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.post_is_located_in_place
LOCATION 'graph-ldbc-snb/delta/post_is_located_in_place'
AS SELECT
    CAST(post_id AS BIGINT) AS post_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.ldbc_snb_raw.post_is_located_in_place;

-- ############################################################################
-- STEP 3b: Physical Layout — Z-ORDER for fast data skipping
-- ############################################################################
-- The graph uses Person vertices + KNOWS edges; only those two tables are
-- ZORDER'd since they drive CSR build and Cypher seed scans.  Data was loaded
-- in id order, which has reasonable locality for `id` but scatters the
-- frequent filter column (gender) across files.  Z-ORDER rewrites files so
-- rows with similar values on the ordering keys co-locate, giving Parquet
-- min/max statistics much tighter ranges per file.  This benefits three
-- hot paths:
--
--   1. CSR build from the KNOWS edge table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `id` co-location lets the Parquet reader skip
--      almost every row group for targeted person scans.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE p.gender = 'female'` skip entire files instead of reading the
--      whole person table.
--
-- One-time cost at setup; every subsequent query benefits.  Other LDBC
-- entity/edge tables are SQL-only and kept as written.
-- ############################################################################

OPTIMIZE {{zone_name}}.ldbc_social_network.person
    ZORDER BY (id, gender);

OPTIMIZE {{zone_name}}.ldbc_social_network.person_knows_person
    ZORDER BY (src, dst);

-- ############################################################################
-- STEP 4: Graph Definition
-- ############################################################################
-- Creates a named graph coupling Person vertices with KNOWS edges.
-- This is the core social graph used for algorithm verification.
-- Cypher queries reference this by name: USE {{zone_name}}.ldbc_social_network.ldbc_social_network MATCH ...
-- ############################################################################

CREATE GRAPH IF NOT EXISTS {{zone_name}}.ldbc_social_network.ldbc_social_network
    VERTEX TABLE {{zone_name}}.ldbc_social_network.person ID COLUMN id NODE TYPE COLUMN gender NODE NAME COLUMN first_name
    EDGE TABLE {{zone_name}}.ldbc_social_network.person_knows_person SOURCE COLUMN src TARGET COLUMN dst
    DIRECTED;

-- ############################################################################
-- STEP N: Warm the CSR cache
-- ############################################################################
-- CREATE GRAPHCSR pre-builds the Compressed Sparse Row topology and writes
-- it to disk as a .dcsr file. The first Cypher query then loads in ~200 ms
-- instead of rebuilding from Delta tables. Safe to re-run after bulk edge
-- loads to refresh the cache.

CREATE GRAPHCSR {{zone_name}}.ldbc_social_network.ldbc_social_network;
