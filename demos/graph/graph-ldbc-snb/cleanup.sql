-- ============================================================================
-- LDBC Social Network Benchmark — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- Two schemas are cleaned up:
--   {{zone_name}}.ldbc_social_network — Delta tables + graph definition
--   {{zone_name}}.raw  — External CSV staging tables
-- ============================================================================

-- STEP 1: Drop graph definition (also cascade-deletes table mappings)
DROP GRAPH IF EXISTS {{zone_name}}.ldbc_social_network.ldbc_social_network;

-- STEP 2: Drop Delta tables — dynamic edges
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.post_is_located_in_place WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.post_has_tag_tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.post_has_creator_person WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person_work_at_organisation WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person_study_at_organisation WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person_speaks_language WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person_likes_post WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person_likes_comment WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person_is_located_in_place WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person_has_interest_tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person_email WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.forum_has_tag_tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.forum_has_moderator_person WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.forum_has_member_person WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.forum_container_of_post WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.comment_reply_of_post WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.comment_reply_of_comment WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.comment_is_located_in_place WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.comment_has_tag_tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.comment_has_creator_person WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person_knows_person WITH FILES;

-- STEP 3: Drop Delta tables — dynamic entities
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.forum WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.post WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.comment WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.person WITH FILES;

-- STEP 4: Drop Delta tables — static edges
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.tagclass_is_subclass_of_tagclass WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.tag_has_type_tagclass WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.place_is_part_of_place WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.organisation_is_located_in_place WITH FILES;

-- STEP 5: Drop Delta tables — static entities
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.tagclass WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.tag WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.organisation WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.ldbc_social_network.place WITH FILES;

-- STEP 6: Drop external tables (staging schema)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.post_is_located_in_place WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.post_has_tag_tag WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.post_has_creator_person WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_work_at_organisation WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_study_at_organisation WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_speaks_language WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_likes_post WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_likes_comment WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_is_located_in_place WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_has_interest_tag WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_email WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum_has_tag_tag WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum_has_moderator_person WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum_has_member_person WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum_container_of_post WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_reply_of_post WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_reply_of_comment WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_is_located_in_place WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_has_tag_tag WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment_has_creator_person WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person_knows_person WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.forum WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.post WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.comment WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.person WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.tagclass_is_subclass_of_tagclass WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.tag_has_type_tagclass WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.place_is_part_of_place WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.organisation_is_located_in_place WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.tagclass WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.tag WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.organisation WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.place WITH FILES;

-- STEP 7: Drop schemas and zone
DROP SCHEMA IF EXISTS {{zone_name}}.ldbc_social_network;
DROP SCHEMA IF EXISTS {{zone_name}}.raw;
DROP ZONE IF EXISTS {{zone_name}};
