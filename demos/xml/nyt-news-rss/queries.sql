-- ============================================================================
-- XML NYT News RSS Feed Analysis — Verification Queries
-- ============================================================================
-- Each query verifies that namespace handling, repeating element modes,
-- and multi-file reading work correctly.
-- Column names are auto-detected from XPath paths using the naming convention
-- in delta-forge-schema (e.g. /rss/channel/item/pubDate → rss_channel_item_pub_date).
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ARTICLE COUNT — 7 files should produce 231 rows
-- ============================================================================
-- Africa(20) + Americas(31) + AsiaPacific(20) + Europe(20) +
-- MiddleEast(26) + World(57) + news(57) = 231

ASSERT ROW_COUNT = 231
SELECT *
FROM {{zone_name}}.xml_demos.news_articles;


-- ============================================================================
-- 2. BROWSE ARTICLES — See the full schema with auto-detected column names
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE rss_channel_item_title IS NOT NULL
ASSERT VALUE rss_channel_item_creator IS NOT NULL
ASSERT VALUE rss_channel_item_pub_date IS NOT NULL
SELECT rss_channel_item_title,
       rss_channel_item_creator,
       rss_channel_item_pub_date,
       rss_channel_item_category,
       rss_channel_item_content_attr_url,
       rss_channel_item_credit
FROM {{zone_name}}.xml_demos.news_articles
ORDER BY rss_channel_item_pub_date DESC
LIMIT 10;


-- ============================================================================
-- 3. NAMESPACE STRIPPING — dc:creator becomes rss_channel_item_creator
-- ============================================================================
-- If namespaces are NOT stripped, the column would include the dc prefix.
-- With strip_namespace_prefixes=true, dc:creator → creator → rss_channel_item_creator.

ASSERT VALUE author_count = 231
SELECT COUNT(*) FILTER (WHERE rss_channel_item_creator IS NOT NULL) AS author_count
FROM {{zone_name}}.xml_demos.news_articles;


-- ============================================================================
-- 4. REPEATING CATEGORIES (JoinComma) — comma-separated string
-- ============================================================================
-- Most items have multiple <category> elements. With JoinComma they become
-- a single string like "War and Armed Conflicts,Russia,Ukraine".

ASSERT VALUE joined_count = 218
SELECT COUNT(*) FILTER (WHERE rss_channel_item_category LIKE '%,%') AS joined_count
FROM {{zone_name}}.xml_demos.news_articles;


-- ============================================================================
-- 5. MEDIA ATTRIBUTES — thumbnail extracted from self-closing element
-- ============================================================================
-- <media:content height="1800" url="https://..." width="1800"/>
-- The @url attribute is extracted as rss_channel_item_content_attr_url.

ASSERT VALUE thumbnail_count = 215
SELECT COUNT(*) FILTER (WHERE rss_channel_item_content_attr_url LIKE 'https://%') AS thumbnail_count
FROM {{zone_name}}.xml_demos.news_articles;


-- ============================================================================
-- 6. ARTICLES PER REGION — using df_file_name metadata column
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE article_count >= 20
SELECT df_file_name AS region_file,
       COUNT(*) AS article_count
FROM {{zone_name}}.xml_demos.news_articles
GROUP BY df_file_name
ORDER BY article_count DESC;


-- ============================================================================
-- 7. EXPLODED CATEGORY COUNT — should be ~2023 rows (one per category)
-- ============================================================================

ASSERT ROW_COUNT = 2023
SELECT *
FROM {{zone_name}}.xml_demos.news_categories;


-- ============================================================================
-- 8. CATEGORY DOMAIN ATTRIBUTE — distinguishes keyword types
-- ============================================================================
-- The @domain attribute on <category> contains URIs like:
--   .../keywords/des     → topic descriptors
--   .../keywords/nyt_per → people
--   .../keywords/nyt_geo → places
--   .../keywords/nyt_org → organizations

ASSERT ROW_COUNT >= 5
ASSERT VALUE keyword_count >= 5
SELECT rss_channel_item_category_attr_domain,
       COUNT(*) AS keyword_count
FROM {{zone_name}}.xml_demos.news_categories
WHERE rss_channel_item_category_attr_domain IS NOT NULL
GROUP BY rss_channel_item_category_attr_domain
ORDER BY keyword_count DESC;


-- ============================================================================
-- 9. TOP MENTIONED PEOPLE — from nyt_per category domain
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE mention_count >= 6
ASSERT VALUE person IS NOT NULL
SELECT rss_channel_item_category AS person,
       COUNT(*) AS mention_count
FROM {{zone_name}}.xml_demos.news_categories
WHERE rss_channel_item_category_attr_domain LIKE '%nyt_per'
GROUP BY rss_channel_item_category
ORDER BY mention_count DESC
LIMIT 10;


-- ============================================================================
-- 10. TOP GEOGRAPHIC KEYWORDS — from nyt_geo category domain
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE mention_count >= 12
ASSERT VALUE location IS NOT NULL
SELECT rss_channel_item_category AS location,
       COUNT(*) AS mention_count
FROM {{zone_name}}.xml_demos.news_categories
WHERE rss_channel_item_category_attr_domain LIKE '%nyt_geo'
GROUP BY rss_channel_item_category
ORDER BY mention_count DESC
LIMIT 10;


-- ============================================================================
-- 11. ARTICLES WITH MOST CATEGORIES — spot-check the join
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE category_count >= 26
ASSERT VALUE rss_channel_item_title IS NOT NULL
SELECT rss_channel_item_title,
       rss_channel_item_creator,
       LENGTH(rss_channel_item_category) - LENGTH(REPLACE(rss_channel_item_category, ',', '')) + 1 AS category_count
FROM {{zone_name}}.xml_demos.news_articles
WHERE rss_channel_item_category IS NOT NULL
ORDER BY category_count DESC
LIMIT 5;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: all 5 key invariants must return 'PASS'.

ASSERT ROW_COUNT = 5
ASSERT VALUE result IN ('PASS')
SELECT 'total_articles' AS check_name,
       CASE WHEN COUNT(*) = 231 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml_demos.news_articles
UNION ALL
SELECT 'namespace_author',
       CASE WHEN COUNT(*) FILTER (WHERE rss_channel_item_creator IS NOT NULL) = 231
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.news_articles
UNION ALL
SELECT 'categories_joined',
       CASE WHEN COUNT(*) FILTER (WHERE rss_channel_item_category LIKE '%,%') = 218
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.news_articles
UNION ALL
SELECT 'thumbnail_extracted',
       CASE WHEN COUNT(*) FILTER (WHERE rss_channel_item_content_attr_url LIKE 'https://%') = 215
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.news_articles
UNION ALL
SELECT 'exploded_categories',
       CASE WHEN COUNT(*) = 2023 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.news_categories
ORDER BY check_name;
