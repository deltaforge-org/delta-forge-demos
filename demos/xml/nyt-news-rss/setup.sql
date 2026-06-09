-- ============================================================================
-- XML NYT News RSS Feed Analysis — Setup Script
-- ============================================================================
-- Creates two external tables from 7 NYT RSS feed XML files:
--   1. news_articles  — one row per article, categories joined as comma string
--   2. news_categories — exploded: one row per category per article
--
-- Demonstrates:
--   - 4 XML namespaces (dc:, media:, atom:, nyt:) with strip_namespace_prefixes
--   - Repeating <category> elements with JoinComma and Explode handling
--   - Self-closing elements (<media:content/>) with attribute extraction
--   - column_mappings following delta-forge naming standard (e.g. rss_channel_item_pub_date)
--   - exclude_paths to skip channel-level metadata
--   - Multi-file reading (7 XML files in one directory)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.xml_demos
    COMMENT 'XML-backed external tables';

-- ============================================================================
-- TABLE 1: news_articles — One row per <item>, categories comma-joined
-- ============================================================================
-- Each <item> in the RSS feed becomes one row. Repeating <category> elements
-- are joined into a single comma-separated string. Namespace prefixes (dc:,
-- media:, atom:) are stripped so columns get clean path-based names like
-- "rss_channel_item_creator" instead of "rss_channel_item_dc_creator".
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.xml_demos.news_articles
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    xml_flatten_config = '{
        "row_xpath": "//item",
        "include_paths": [
            "/rss/channel/item/title",
            "/rss/channel/item/link",
            "/rss/channel/item/guid",
            "/rss/channel/item/guid/@isPermaLink",
            "/rss/channel/item/description",
            "/rss/channel/item/creator",
            "/rss/channel/item/pubDate",
            "/rss/channel/item/category",
            "/rss/channel/item/content/@url",
            "/rss/channel/item/content/@height",
            "/rss/channel/item/content/@width",
            "/rss/channel/item/content/@medium",
            "/rss/channel/item/credit"
        ],
        "exclude_paths": ["/rss/channel/image", "/rss/channel/title", "/rss/channel/link", "/rss/channel/description", "/rss/channel/language", "/rss/channel/copyright", "/rss/channel/lastBuildDate", "/rss/channel/pubDate"],
        "default_repeat_handling": "join_comma",
        "column_mappings": {
            "/rss/channel/item/title": "rss_channel_item_title",
            "/rss/channel/item/link": "rss_channel_item_link",
            "/rss/channel/item/guid": "rss_channel_item_guid",
            "/rss/channel/item/guid/@isPermaLink": "rss_channel_item_guid_attr_is_perma_link",
            "/rss/channel/item/description": "rss_channel_item_description",
            "/rss/channel/item/creator": "rss_channel_item_creator",
            "/rss/channel/item/pubDate": "rss_channel_item_pub_date",
            "/rss/channel/item/category": "rss_channel_item_category",
            "/rss/channel/item/credit": "rss_channel_item_credit",
            "/rss/channel/item/content/@url": "rss_channel_item_content_attr_url",
            "/rss/channel/item/content/@height": "rss_channel_item_content_attr_height",
            "/rss/channel/item/content/@width": "rss_channel_item_content_attr_width",
            "/rss/channel/item/content/@medium": "rss_channel_item_content_attr_medium"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "strip_namespace_prefixes": true,
        "namespaces": {
            "dc": "http://purl.org/dc/elements/1.1/",
            "media": "http://search.yahoo.com/mrss/",
            "atom": "http://www.w3.org/2005/Atom",
            "nyt": "http://www.nytimes.com/namespaces/rss/2.0"
        }
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: news_categories — Exploded: one row per <category> per article
-- ============================================================================
-- The same RSS items, but each <category> element is exploded into its own
-- row. The @domain attribute on <category> distinguishes keyword types:
--   - .../keywords/des     → topic descriptors
--   - .../keywords/nyt_per → people mentioned
--   - .../keywords/nyt_geo → geographic locations
--   - .../keywords/nyt_org → organizations
-- This enables keyword frequency analysis across all 231 articles.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.xml_demos.news_categories
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    xml_flatten_config = '{
        "row_xpath": "//item",
        "explode_paths": ["/rss/channel/item/category"],
        "include_paths": [
            "/rss/channel/item/title",
            "/rss/channel/item/link",
            "/rss/channel/item/creator",
            "/rss/channel/item/pubDate",
            "/rss/channel/item/category",
            "/rss/channel/item/category/@domain"
        ],
        "exclude_paths": ["/rss/channel/image", "/rss/channel/title", "/rss/channel/link", "/rss/channel/description", "/rss/channel/language", "/rss/channel/copyright", "/rss/channel/lastBuildDate", "/rss/channel/pubDate"],
        "column_mappings": {
            "/rss/channel/item/title": "rss_channel_item_title",
            "/rss/channel/item/link": "rss_channel_item_link",
            "/rss/channel/item/creator": "rss_channel_item_creator",
            "/rss/channel/item/pubDate": "rss_channel_item_pub_date",
            "/rss/channel/item/category": "rss_channel_item_category",
            "/rss/channel/item/category/@domain": "rss_channel_item_category_attr_domain"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "strip_namespace_prefixes": true,
        "namespaces": {
            "dc": "http://purl.org/dc/elements/1.1/",
            "media": "http://search.yahoo.com/mrss/",
            "atom": "http://www.w3.org/2005/Atom",
            "nyt": "http://www.nytimes.com/namespaces/rss/2.0"
        }
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
