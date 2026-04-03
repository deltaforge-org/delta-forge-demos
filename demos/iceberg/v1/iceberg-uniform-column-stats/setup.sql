-- ============================================================================
-- Iceberg UniForm Column-Level Statistics — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table and seeds it with 30 ad clicks.
-- Dataset: ad-click analytics across 3 campaigns (summer-sale, back-to-school,
-- holiday-promo), 4 device types (mobile, desktop, tablet, smart-tv), with
-- some clicks having NULL conversion_value (non-converted clicks).
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm enabled
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.ad_clicks (
    click_id          INT,
    campaign_id       VARCHAR,
    ad_group          VARCHAR,
    impression_ts     VARCHAR,
    click_ts          VARCHAR,
    cost_per_click    DOUBLE,
    conversion_value  DOUBLE,
    device_type       VARCHAR,
    country           VARCHAR,
    is_converted      BOOLEAN
) LOCATION '{{data_path}}/ad_clicks'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.ad_clicks TO USER {{current_user}};

-- STEP 3: Seed 30 ad clicks (Version 1, Iceberg Snapshot 1)
-- 10 per campaign, ~half with NULL conversion_value (non-converted)
INSERT INTO {{zone_name}}.iceberg_demos.ad_clicks VALUES
    (1,  'summer-sale',    'search-brand',     '2025-06-01 08:12:00', '2025-06-01 08:12:45', 1.25,  12.50,  'mobile',   'US', true),
    (2,  'summer-sale',    'search-brand',     '2025-06-01 09:30:00', '2025-06-01 09:31:10', 0.85,  NULL,   'desktop',  'US', false),
    (3,  'summer-sale',    'display-retarget', '2025-06-02 14:00:00', '2025-06-02 14:01:30', 0.45,  8.75,   'tablet',   'UK', true),
    (4,  'summer-sale',    'display-retarget', '2025-06-02 16:20:00', '2025-06-02 16:21:05', 0.50,  NULL,   'mobile',   'UK', false),
    (5,  'summer-sale',    'search-generic',   '2025-06-03 10:00:00', '2025-06-03 10:00:55', 2.10,  25.00,  'desktop',  'CA', true),
    (6,  'summer-sale',    'search-generic',   '2025-06-03 11:45:00', '2025-06-03 11:46:20', 1.80,  NULL,   'smart-tv', 'CA', false),
    (7,  'summer-sale',    'video-pre-roll',   '2025-06-04 07:00:00', '2025-06-04 07:01:15', 3.50,  45.00,  'mobile',   'US', true),
    (8,  'summer-sale',    'video-pre-roll',   '2025-06-04 19:30:00', '2025-06-04 19:31:00', 3.20,  NULL,   'desktop',  'DE', false),
    (9,  'summer-sale',    'shopping',         '2025-06-05 12:00:00', '2025-06-05 12:01:10', 0.95,  15.00,  'mobile',   'US', true),
    (10, 'summer-sale',    'shopping',         '2025-06-05 15:30:00', '2025-06-05 15:31:45', 1.10,  NULL,   'tablet',   'FR', false),
    (11, 'back-to-school', 'search-brand',     '2025-07-15 08:00:00', '2025-07-15 08:01:00', 1.50,  18.00,  'mobile',   'US', true),
    (12, 'back-to-school', 'search-brand',     '2025-07-15 09:15:00', '2025-07-15 09:16:30', 1.35,  NULL,   'desktop',  'US', false),
    (13, 'back-to-school', 'display-retarget', '2025-07-16 13:00:00', '2025-07-16 13:01:20', 0.60,  10.00,  'tablet',   'UK', true),
    (14, 'back-to-school', 'display-retarget', '2025-07-16 14:45:00', '2025-07-16 14:46:10', 0.55,  NULL,   'smart-tv', 'UK', false),
    (15, 'back-to-school', 'search-generic',   '2025-07-17 10:30:00', '2025-07-17 10:31:15', 2.25,  30.00,  'desktop',  'CA', true),
    (16, 'back-to-school', 'search-generic',   '2025-07-17 11:00:00', '2025-07-17 11:01:30', 1.90,  22.00,  'mobile',   'DE', true),
    (17, 'back-to-school', 'video-pre-roll',   '2025-07-18 06:30:00', '2025-07-18 06:31:00', 3.75,  NULL,   'desktop',  'US', false),
    (18, 'back-to-school', 'video-pre-roll',   '2025-07-18 20:00:00', '2025-07-18 20:01:45', 3.00,  35.00,  'mobile',   'FR', true),
    (19, 'back-to-school', 'shopping',         '2025-07-19 11:00:00', '2025-07-19 11:01:20', 1.05,  NULL,   'tablet',   'US', false),
    (20, 'back-to-school', 'shopping',         '2025-07-19 14:30:00', '2025-07-19 14:31:00', 0.80,  9.50,   'smart-tv', 'CA', true),
    (21, 'holiday-promo',  'search-brand',     '2025-11-20 07:00:00', '2025-11-20 07:01:30', 2.00,  28.00,  'mobile',   'US', true),
    (22, 'holiday-promo',  'search-brand',     '2025-11-20 08:30:00', '2025-11-20 08:31:15', 1.75,  NULL,   'desktop',  'UK', false),
    (23, 'holiday-promo',  'display-retarget', '2025-11-21 12:00:00', '2025-11-21 12:01:00', 0.70,  14.00,  'tablet',   'DE', true),
    (24, 'holiday-promo',  'display-retarget', '2025-11-21 15:00:00', '2025-11-21 15:01:30', 0.65,  NULL,   'mobile',   'FR', false),
    (25, 'holiday-promo',  'search-generic',   '2025-11-22 09:00:00', '2025-11-22 09:01:10', 2.50,  40.00,  'desktop',  'US', true),
    (26, 'holiday-promo',  'search-generic',   '2025-11-22 10:15:00', '2025-11-22 10:16:00', 2.30,  NULL,   'smart-tv', 'CA', false),
    (27, 'holiday-promo',  'video-pre-roll',   '2025-11-23 06:00:00', '2025-11-23 06:01:45', 4.00,  55.00,  'mobile',   'US', true),
    (28, 'holiday-promo',  'video-pre-roll',   '2025-11-23 18:00:00', '2025-11-23 18:01:20', 3.80,  NULL,   'desktop',  'UK', false),
    (29, 'holiday-promo',  'shopping',         '2025-11-24 10:00:00', '2025-11-24 10:01:00', 1.20,  16.50,  'tablet',   'US', true),
    (30, 'holiday-promo',  'shopping',         '2025-11-24 13:00:00', '2025-11-24 13:01:30', 0.90,  NULL,   'mobile',   'DE', false);
