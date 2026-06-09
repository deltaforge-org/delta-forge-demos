-- ============================================================================
-- Cleanup: arXiv AI Research Feed
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.arxiv_api.arxiv_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.arxiv_api.arxiv_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.arxiv_api.cs_ai_latest;

DROP CONNECTION IF EXISTS arxiv_api;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'arxiv-ai-research-feed' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.arxiv_api;
