-- ============================================================================
-- Demo: GitHub Topic Repo Discovery, Link-Header Pagination + FULL REFRESH
-- Feature: pagination = 'link_header', header.* endpoint options,
--          JSON flatten with root_path = "$.items"
-- ============================================================================
--
-- Real-world story: a data-platform DevRel team maintains a quarterly
-- snapshot of the Delta Lake open-source ecosystem, every public repo
-- tagged `topic:delta-lake`, ordered by star count. The snapshot feeds
-- an internal ecosystem report that highlights which projects the
-- community is gravitating toward, which have gone stale, and which new
-- entrants crossed the star threshold that quarter.
--
-- This file declares the catalog objects only. The INVOKE FULL REFRESH
-- call, the per-run audit, the schema detection, and the
-- bronze->silver promotion all live in queries.sql so the user can see
-- in one place how a link-header paginated REST endpoint is driven
-- from SQL.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.github_search_api
    COMMENT 'Open-source ecosystem intelligence, quarterly GitHub topic snapshots';

-- --------------------------------------------------------------------------
-- 2. REST API connection
-- --------------------------------------------------------------------------
-- GitHub's search API is public with an anonymous budget. auth_mode =
-- 'none' matches the rust-release-catalog demo. To escalate to a
-- higher-limit authenticated call, flip auth_mode to 'bearer' and
-- attach a CREDENTIAL pointing at a vault entry holding a classic PAT.

CREATE CONNECTION IF NOT EXISTS github_search_api
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://api.github.com',
        auth_mode    = 'none',
        storage_zone = '{{zone_name}}',
        base_path    = 'github_search',
        timeout_secs = '30'
    );

-- --------------------------------------------------------------------------
-- 3. API endpoint, link-header pagination, GitHub Accept header
-- --------------------------------------------------------------------------
-- Link-header pagination follows GitHub's RFC-5988 `<url>; rel="next"`
-- hints. Unlike page/offset pagination the engine doesn't synthesise
-- URLs, it reads the one GitHub sends on every response, so topic
-- search, cursor-based endpoints, and federated gateways all work
-- without grammar changes. max_pages = 3 bounds the crawl at 90 repos
-- (30 per page x 3 pages). header.Accept carries GitHub's v3 media
-- type so the response shape stays stable across API migrations.

CREATE API ENDPOINT {{zone_name}}.github_search_api.delta_lake_topic
    URL '/search/repositories?q=topic%3Adelta-lake&per_page=30&sort=stars&order=desc'
    RESPONSE FORMAT JSON
    OPTIONS (
        pagination         = 'link_header',
        max_pages          = '3',
        rate_limit_rps     = '1',
        retry_max_attempts = '3',
        header.Accept      = 'application/vnd.github+json'
    );

-- --------------------------------------------------------------------------
-- 4. Bronze external table, root_path = "$.items" + nested $.owner.login
-- --------------------------------------------------------------------------
-- GitHub wraps search responses as {total_count, incomplete_results,
-- items: [...]}. Setting root_path to "$.items" tells the flatten to
-- iterate the items array so each repo becomes one row. The
-- $.owner.login path descends into the nested owner object, same
-- pattern as rust-release-catalog's $.author.login.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.github_search_api.delta_lake_repos_bronze
USING JSON
LOCATION 'github_search/delta_lake_topic'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$.items",
        "include_paths": [
            "$.id",
            "$.full_name",
            "$.owner.login",
            "$.stargazers_count",
            "$.forks_count",
            "$.language",
            "$.archived",
            "$.fork",
            "$.html_url"
        ],
        "column_mappings": {
            "$.id":                "repo_id",
            "$.full_name":         "full_name",
            "$.owner.login":       "owner_login",
            "$.stargazers_count":  "stars",
            "$.forks_count":       "forks",
            "$.language":          "language",
            "$.archived":          "is_archived",
            "$.fork":              "is_fork",
            "$.html_url":          "html_url"
        },
        "max_depth": 3,
        "separator": "_",
        "infer_types": true
    }'
);

-- --------------------------------------------------------------------------
-- 5. Silver Delta table, schema-only declaration
-- --------------------------------------------------------------------------
-- Silver declares explicit BIGINT + BOOLEAN columns so the DevRel
-- report can sort by stars without casting. The quarterly report
-- points at silver; bronze stays available for deeper audits. The
-- bronze->silver INSERT lives in queries.sql.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.github_search_api.delta_lake_repos_silver (
    repo_id      BIGINT,
    full_name    STRING,
    owner_login  STRING,
    stars        BIGINT,
    forks        BIGINT,
    language     STRING,
    is_archived  BOOLEAN,
    is_fork      BOOLEAN,
    html_url     STRING
)
LOCATION 'silver/delta_lake_repos';
