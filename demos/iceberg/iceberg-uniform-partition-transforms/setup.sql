-- ============================================================================
-- Iceberg UniForm Partition Transforms — Setup
-- ============================================================================
-- Creates a Delta table partitioned by CAST(event_timestamp AS DATE) with
-- Iceberg UniForm enabled. The Iceberg metadata maps this to a day()
-- partition transform in the partition spec.
--
-- Dataset: 36 application events across 6 days (2024-03-01 to 2024-03-06).
-- Event types: click, purchase, login, error, logout, page_view
-- Severities: info, warning, error
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create date-partitioned table with UniForm
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.app_events (
    event_id        INT,
    user_id         VARCHAR,
    event_type      VARCHAR,
    event_timestamp TIMESTAMP,
    payload_size    INT,
    source_app      VARCHAR,
    severity        VARCHAR
) LOCATION '{{data_path}}/app_events'
PARTITIONED BY (CAST(event_timestamp AS DATE))
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.app_events TO USER {{current_user}};

-- STEP 3: Seed 36 events — 6 per day across 6 days (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.app_events VALUES
    -- 2024-03-01: 6 events
    (1,  'usr_101', 'click',     TIMESTAMP '2024-03-01 08:15:00', 256,  'web-app',    'info'),
    (2,  'usr_102', 'login',     TIMESTAMP '2024-03-01 09:00:00', 128,  'mobile-app', 'info'),
    (3,  'usr_103', 'purchase',  TIMESTAMP '2024-03-01 10:30:00', 512,  'web-app',    'info'),
    (4,  'usr_104', 'error',     TIMESTAMP '2024-03-01 11:45:00', 1024, 'api-server', 'error'),
    (5,  'usr_105', 'page_view', TIMESTAMP '2024-03-01 14:20:00', 192,  'web-app',    'info'),
    (6,  'usr_106', 'logout',    TIMESTAMP '2024-03-01 17:00:00', 64,   'mobile-app', 'info'),
    -- 2024-03-02: 6 events
    (7,  'usr_101', 'login',     TIMESTAMP '2024-03-02 07:30:00', 128,  'web-app',    'info'),
    (8,  'usr_107', 'click',     TIMESTAMP '2024-03-02 08:45:00', 256,  'mobile-app', 'info'),
    (9,  'usr_108', 'purchase',  TIMESTAMP '2024-03-02 11:00:00', 768,  'web-app',    'info'),
    (10, 'usr_109', 'error',     TIMESTAMP '2024-03-02 12:15:00', 2048, 'api-server', 'error'),
    (11, 'usr_110', 'page_view', TIMESTAMP '2024-03-02 15:30:00', 192,  'web-app',    'info'),
    (12, 'usr_101', 'logout',    TIMESTAMP '2024-03-02 18:00:00', 64,   'web-app',    'info'),
    -- 2024-03-03: 6 events
    (13, 'usr_111', 'click',     TIMESTAMP '2024-03-03 09:10:00', 320,  'mobile-app', 'info'),
    (14, 'usr_112', 'login',     TIMESTAMP '2024-03-03 09:30:00', 128,  'web-app',    'info'),
    (15, 'usr_113', 'error',     TIMESTAMP '2024-03-03 10:00:00', 1536, 'api-server', 'error'),
    (16, 'usr_114', 'purchase',  TIMESTAMP '2024-03-03 13:45:00', 640,  'mobile-app', 'info'),
    (17, 'usr_115', 'page_view', TIMESTAMP '2024-03-03 16:00:00', 256,  'web-app',    'info'),
    (18, 'usr_111', 'logout',    TIMESTAMP '2024-03-03 19:30:00', 64,   'mobile-app', 'info'),
    -- 2024-03-04: 6 events
    (19, 'usr_116', 'login',     TIMESTAMP '2024-03-04 08:00:00', 128,  'web-app',    'info'),
    (20, 'usr_117', 'click',     TIMESTAMP '2024-03-04 09:20:00', 384,  'web-app',    'info'),
    (21, 'usr_118', 'purchase',  TIMESTAMP '2024-03-04 12:00:00', 896,  'mobile-app', 'info'),
    (22, 'usr_119', 'error',     TIMESTAMP '2024-03-04 13:30:00', 1280, 'api-server', 'warning'),
    (23, 'usr_120', 'page_view', TIMESTAMP '2024-03-04 15:45:00', 192,  'web-app',    'info'),
    (24, 'usr_116', 'logout',    TIMESTAMP '2024-03-04 18:30:00', 64,   'web-app',    'info'),
    -- 2024-03-05: 6 events
    (25, 'usr_121', 'login',     TIMESTAMP '2024-03-05 07:45:00', 128,  'mobile-app', 'info'),
    (26, 'usr_122', 'click',     TIMESTAMP '2024-03-05 10:15:00', 448,  'web-app',    'info'),
    (27, 'usr_123', 'purchase',  TIMESTAMP '2024-03-05 11:30:00', 1024, 'web-app',    'info'),
    (28, 'usr_124', 'error',     TIMESTAMP '2024-03-05 14:00:00', 1792, 'api-server', 'error'),
    (29, 'usr_125', 'page_view', TIMESTAMP '2024-03-05 16:20:00', 320,  'mobile-app', 'info'),
    (30, 'usr_121', 'logout',    TIMESTAMP '2024-03-05 19:00:00', 64,   'mobile-app', 'info'),
    -- 2024-03-06: 6 events
    (31, 'usr_126', 'login',     TIMESTAMP '2024-03-06 08:30:00', 128,  'web-app',    'info'),
    (32, 'usr_127', 'click',     TIMESTAMP '2024-03-06 09:45:00', 512,  'mobile-app', 'info'),
    (33, 'usr_128', 'error',     TIMESTAMP '2024-03-06 11:15:00', 2560, 'api-server', 'error'),
    (34, 'usr_129', 'purchase',  TIMESTAMP '2024-03-06 13:00:00', 704,  'web-app',    'info'),
    (35, 'usr_130', 'page_view', TIMESTAMP '2024-03-06 15:30:00', 256,  'web-app',    'info'),
    (36, 'usr_126', 'logout',    TIMESTAMP '2024-03-06 17:45:00', 64,   'mobile-app', 'info');
