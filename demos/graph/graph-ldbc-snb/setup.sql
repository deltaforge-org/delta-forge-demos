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
-- This demo creates 33 external tables, 31 Delta tables, and 1 graph.
-- Two schemas keep staging separate from the materialized layer:
--   {{zone_name}}.raw   — External CSV tables (staging / read-only)
--   {{zone_name}}.ldbc_social_network — Delta tables + graph definition (queryable)
-- The cleanup script drops both schemas and everything in them.
-- ############################################################################

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.raw
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

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.place
USING CSV LOCATION '{{data_path}}/place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.organisation
USING CSV LOCATION '{{data_path}}/organisation.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.tag
USING CSV LOCATION '{{data_path}}/tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.tagclass
USING CSV LOCATION '{{data_path}}/tagclass.csv'
OPTIONS (header = 'true', delimiter = '|');
-- === Static Edges ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.organisation_is_located_in_place
USING CSV LOCATION '{{data_path}}/organisation_is_located_in_place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.place_is_part_of_place
USING CSV LOCATION '{{data_path}}/place_is_part_of_place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.tag_has_type_tagclass
USING CSV LOCATION '{{data_path}}/tag_has_type_tagclass.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.tagclass_is_subclass_of_tagclass
USING CSV LOCATION '{{data_path}}/tagclass_is_subclass_of_tagclass.csv'
OPTIONS (header = 'true', delimiter = '|');
-- === Dynamic Entities ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person
USING CSV LOCATION '{{data_path}}/person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment
USING CSV LOCATION '{{data_path}}/comment.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.post
USING CSV LOCATION '{{data_path}}/post.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum
USING CSV LOCATION '{{data_path}}/forum.csv'
OPTIONS (header = 'true', delimiter = '|');
-- === Dynamic Edges ===

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_knows_person
USING CSV LOCATION '{{data_path}}/person_knows_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_has_creator_person
USING CSV LOCATION '{{data_path}}/comment_has_creator_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_has_tag_tag
USING CSV LOCATION '{{data_path}}/comment_has_tag_tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_is_located_in_place
USING CSV LOCATION '{{data_path}}/comment_is_located_in_place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_reply_of_comment
USING CSV LOCATION '{{data_path}}/comment_reply_of_comment.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.comment_reply_of_post
USING CSV LOCATION '{{data_path}}/comment_reply_of_post.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum_container_of_post
USING CSV LOCATION '{{data_path}}/forum_container_of_post.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum_has_member_person
USING CSV LOCATION '{{data_path}}/forum_has_member_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum_has_moderator_person
USING CSV LOCATION '{{data_path}}/forum_has_moderator_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.forum_has_tag_tag
USING CSV LOCATION '{{data_path}}/forum_has_tag_tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_email
USING CSV LOCATION '{{data_path}}/person_email_emailaddress.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_has_interest_tag
USING CSV LOCATION '{{data_path}}/person_has_interest_tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_is_located_in_place
USING CSV LOCATION '{{data_path}}/person_is_located_in_place.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_likes_comment
USING CSV LOCATION '{{data_path}}/person_likes_comment.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_likes_post
USING CSV LOCATION '{{data_path}}/person_likes_post.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_speaks_language
USING CSV LOCATION '{{data_path}}/person_speaks_language.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_study_at_organisation
USING CSV LOCATION '{{data_path}}/person_study_at_organisation.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.person_work_at_organisation
USING CSV LOCATION '{{data_path}}/person_work_at_organisation.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.post_has_creator_person
USING CSV LOCATION '{{data_path}}/post_has_creator_person.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.post_has_tag_tag
USING CSV LOCATION '{{data_path}}/post_has_tag_tag.csv'
OPTIONS (header = 'true', delimiter = '|');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.raw.post_is_located_in_place
USING CSV LOCATION '{{data_path}}/post_is_located_in_place.csv'
OPTIONS (header = 'true', delimiter = '|');
-- ############################################################################
-- STEP 2b: Permissions — External Tables
-- ############################################################################
-- Grants the current user admin access on each external table.
-- ############################################################################

-- Static entities
GRANT ADMIN ON TABLE {{zone_name}}.raw.place TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.organisation TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.tag TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.tagclass TO USER {{current_user}};

-- Static edges
GRANT ADMIN ON TABLE {{zone_name}}.raw.organisation_is_located_in_place TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.place_is_part_of_place TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.tag_has_type_tagclass TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.tagclass_is_subclass_of_tagclass TO USER {{current_user}};

-- Dynamic entities
GRANT ADMIN ON TABLE {{zone_name}}.raw.person TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.comment TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.post TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.forum TO USER {{current_user}};

-- Dynamic edges
GRANT ADMIN ON TABLE {{zone_name}}.raw.person_knows_person TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.comment_has_creator_person TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.comment_has_tag_tag TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.comment_is_located_in_place TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.comment_reply_of_comment TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.comment_reply_of_post TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.forum_container_of_post TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.forum_has_member_person TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.forum_has_moderator_person TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.forum_has_tag_tag TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.person_email TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.person_has_interest_tag TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.person_is_located_in_place TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.person_likes_comment TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.person_likes_post TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.person_speaks_language TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.person_study_at_organisation TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.person_work_at_organisation TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.post_has_creator_person TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.post_has_tag_tag TO USER {{current_user}};

GRANT ADMIN ON TABLE {{zone_name}}.raw.post_is_located_in_place TO USER {{current_user}};
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
LOCATION '{{data_path}}/delta/place'
AS SELECT
    CAST(id AS BIGINT) AS id,
    name,
    url,
    type
FROM {{zone_name}}.raw.place;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.place TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.organisation
LOCATION '{{data_path}}/delta/organisation'
AS SELECT
    CAST(id AS BIGINT) AS id,
    type,
    name,
    url
FROM {{zone_name}}.raw.organisation;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.organisation TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.tag
LOCATION '{{data_path}}/delta/tag'
AS SELECT
    CAST(id AS BIGINT) AS id,
    name,
    url
FROM {{zone_name}}.raw.tag;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.tag TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.tagclass
LOCATION '{{data_path}}/delta/tagclass'
AS SELECT
    CAST(id AS BIGINT) AS id,
    name,
    url
FROM {{zone_name}}.raw.tagclass;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.tagclass TO USER {{current_user}};
-- === Static Edge Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.organisation_is_located_in_place
LOCATION '{{data_path}}/delta/organisation_is_located_in_place'
AS SELECT
    CAST(organisation_id AS BIGINT) AS organisation_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.raw.organisation_is_located_in_place;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.organisation_is_located_in_place TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.place_is_part_of_place
LOCATION '{{data_path}}/delta/place_is_part_of_place'
AS SELECT
    CAST(place_id AS BIGINT) AS place_id,
    CAST(parent_place_id AS BIGINT) AS parent_place_id
FROM {{zone_name}}.raw.place_is_part_of_place;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.place_is_part_of_place TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.tag_has_type_tagclass
LOCATION '{{data_path}}/delta/tag_has_type_tagclass'
AS SELECT
    CAST(tag_id AS BIGINT) AS tag_id,
    CAST(tagclass_id AS BIGINT) AS tagclass_id
FROM {{zone_name}}.raw.tag_has_type_tagclass;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.tag_has_type_tagclass TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.tagclass_is_subclass_of_tagclass
LOCATION '{{data_path}}/delta/tagclass_is_subclass_of_tagclass'
AS SELECT
    CAST(tagclass_id AS BIGINT) AS tagclass_id,
    CAST(parent_tagclass_id AS BIGINT) AS parent_tagclass_id
FROM {{zone_name}}.raw.tagclass_is_subclass_of_tagclass;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.tagclass_is_subclass_of_tagclass TO USER {{current_user}};
-- === Dynamic Entity Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person
LOCATION '{{data_path}}/delta/person'
AS SELECT
    CAST(id AS BIGINT) AS id,
    first_name,
    last_name,
    gender,
    CAST(birthday AS BIGINT) AS birthday,
    CAST(creation_date AS BIGINT) AS creation_date,
    location_ip,
    browser_used
FROM {{zone_name}}.raw.person;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment
LOCATION '{{data_path}}/delta/comment'
AS SELECT
    CAST(id AS BIGINT) AS id,
    CAST(creation_date AS BIGINT) AS creation_date,
    location_ip,
    browser_used,
    content,
    CAST(length AS INT) AS length
FROM {{zone_name}}.raw.comment;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.comment TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.post
LOCATION '{{data_path}}/delta/post'
AS SELECT
    CAST(id AS BIGINT) AS id,
    image_file,
    CAST(creation_date AS BIGINT) AS creation_date,
    location_ip,
    browser_used,
    language,
    content,
    CAST(length AS INT) AS length
FROM {{zone_name}}.raw.post;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.post TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum
LOCATION '{{data_path}}/delta/forum'
AS SELECT
    CAST(id AS BIGINT) AS id,
    title,
    CAST(creation_date AS BIGINT) AS creation_date
FROM {{zone_name}}.raw.forum;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.forum TO USER {{current_user}};
-- === Dynamic Edge Tables ===

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_knows_person
LOCATION '{{data_path}}/delta/person_knows_person'
AS SELECT
    CAST(src AS BIGINT) AS src,
    CAST(dst AS BIGINT) AS dst,
    CAST(creation_date AS BIGINT) AS creation_date
FROM {{zone_name}}.raw.person_knows_person;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person_knows_person TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_has_creator_person
LOCATION '{{data_path}}/delta/comment_has_creator_person'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(person_id AS BIGINT) AS person_id
FROM {{zone_name}}.raw.comment_has_creator_person;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.comment_has_creator_person TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_has_tag_tag
LOCATION '{{data_path}}/delta/comment_has_tag_tag'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.raw.comment_has_tag_tag;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.comment_has_tag_tag TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_is_located_in_place
LOCATION '{{data_path}}/delta/comment_is_located_in_place'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.raw.comment_is_located_in_place;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.comment_is_located_in_place TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_reply_of_comment
LOCATION '{{data_path}}/delta/comment_reply_of_comment'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(reply_to_comment_id AS BIGINT) AS reply_to_comment_id
FROM {{zone_name}}.raw.comment_reply_of_comment;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.comment_reply_of_comment TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.comment_reply_of_post
LOCATION '{{data_path}}/delta/comment_reply_of_post'
AS SELECT
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(post_id AS BIGINT) AS post_id
FROM {{zone_name}}.raw.comment_reply_of_post;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.comment_reply_of_post TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum_container_of_post
LOCATION '{{data_path}}/delta/forum_container_of_post'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(post_id AS BIGINT) AS post_id
FROM {{zone_name}}.raw.forum_container_of_post;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.forum_container_of_post TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum_has_member_person
LOCATION '{{data_path}}/delta/forum_has_member_person'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(person_id AS BIGINT) AS person_id,
    CAST(join_date AS BIGINT) AS join_date
FROM {{zone_name}}.raw.forum_has_member_person;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.forum_has_member_person TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum_has_moderator_person
LOCATION '{{data_path}}/delta/forum_has_moderator_person'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(person_id AS BIGINT) AS person_id
FROM {{zone_name}}.raw.forum_has_moderator_person;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.forum_has_moderator_person TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.forum_has_tag_tag
LOCATION '{{data_path}}/delta/forum_has_tag_tag'
AS SELECT
    CAST(forum_id AS BIGINT) AS forum_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.raw.forum_has_tag_tag;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.forum_has_tag_tag TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_email
LOCATION '{{data_path}}/delta/person_email'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    email
FROM {{zone_name}}.raw.person_email;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person_email TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_has_interest_tag
LOCATION '{{data_path}}/delta/person_has_interest_tag'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.raw.person_has_interest_tag;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person_has_interest_tag TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_is_located_in_place
LOCATION '{{data_path}}/delta/person_is_located_in_place'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.raw.person_is_located_in_place;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person_is_located_in_place TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_likes_comment
LOCATION '{{data_path}}/delta/person_likes_comment'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(comment_id AS BIGINT) AS comment_id,
    CAST(creation_date AS BIGINT) AS creation_date
FROM {{zone_name}}.raw.person_likes_comment;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person_likes_comment TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_likes_post
LOCATION '{{data_path}}/delta/person_likes_post'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(post_id AS BIGINT) AS post_id,
    CAST(creation_date AS BIGINT) AS creation_date
FROM {{zone_name}}.raw.person_likes_post;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person_likes_post TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_speaks_language
LOCATION '{{data_path}}/delta/person_speaks_language'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    language
FROM {{zone_name}}.raw.person_speaks_language;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person_speaks_language TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_study_at_organisation
LOCATION '{{data_path}}/delta/person_study_at_organisation'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(organisation_id AS BIGINT) AS organisation_id,
    CAST(class_year AS INT) AS class_year
FROM {{zone_name}}.raw.person_study_at_organisation;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person_study_at_organisation TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.person_work_at_organisation
LOCATION '{{data_path}}/delta/person_work_at_organisation'
AS SELECT
    CAST(person_id AS BIGINT) AS person_id,
    CAST(organisation_id AS BIGINT) AS organisation_id,
    CAST(work_from AS INT) AS work_from
FROM {{zone_name}}.raw.person_work_at_organisation;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.person_work_at_organisation TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.post_has_creator_person
LOCATION '{{data_path}}/delta/post_has_creator_person'
AS SELECT
    CAST(post_id AS BIGINT) AS post_id,
    CAST(person_id AS BIGINT) AS person_id
FROM {{zone_name}}.raw.post_has_creator_person;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.post_has_creator_person TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.post_has_tag_tag
LOCATION '{{data_path}}/delta/post_has_tag_tag'
AS SELECT
    CAST(post_id AS BIGINT) AS post_id,
    CAST(tag_id AS BIGINT) AS tag_id
FROM {{zone_name}}.raw.post_has_tag_tag;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.post_has_tag_tag TO USER {{current_user}};
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.ldbc_social_network.post_is_located_in_place
LOCATION '{{data_path}}/delta/post_is_located_in_place'
AS SELECT
    CAST(post_id AS BIGINT) AS post_id,
    CAST(place_id AS BIGINT) AS place_id
FROM {{zone_name}}.raw.post_is_located_in_place;

GRANT ADMIN ON TABLE {{zone_name}}.ldbc_social_network.post_is_located_in_place TO USER {{current_user}};
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
