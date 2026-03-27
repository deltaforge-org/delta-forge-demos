-- ============================================================================
-- Iceberg Native Large Manifests (Web Analytics) — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg V2 table
-- with 10 data files across 10 manifest entries (one per micro-batch append).
--
-- The table contains 600 web analytics session records ingested in 10
-- batches of 60 rows. Each append produced a new snapshot, manifest list,
-- and manifest file. Delta Forge must traverse the full manifest chain
-- to discover and union all 10 Parquet data files.
--
-- Schema: session_id, user_agent, page_url, referrer, time_on_page,
--         is_bounce, event_count, country, device_type
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg V2 table
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- Delta Forge parses the latest metadata.json, resolves the manifest list for the
-- current snapshot, walks all 10 manifests, and discovers all 10 data files.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg.web_analytics
USING ICEBERG
LOCATION '{{data_path}}/web_analytics';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg.web_analytics TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg.web_analytics;
