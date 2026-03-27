-- ============================================================================
-- Iceberg UniForm Concurrent Multi-Pipeline Writes — Setup
-- ============================================================================
-- Creates a UniForm-enabled ingestion log and seeds 20 records from the
-- first pipeline (etl-team-alpha, batch-001). Also creates a staging table
-- for MERGE corrections from etl-team-beta.
--
-- Dataset: multi-team data pipeline ingestion log tracking records from
-- crm-primary (alpha), erp-finance (beta), and iot-sensors (gamma).
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create ingestion_log with UniForm enabled
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.ingestion_log (
    record_id       INT,
    pipeline_name   VARCHAR,
    source_system   VARCHAR,
    record_type     VARCHAR,
    payload_hash    VARCHAR,
    ingested_at     VARCHAR,
    batch_id        VARCHAR
) LOCATION '{{data_path}}/ingestion_log'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.ingestion_log TO USER {{current_user}};

-- STEP 3: Seed 20 records from etl-team-alpha (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.ingestion_log VALUES
    (1,  'etl-team-alpha', 'crm-primary', 'order',     '5b2c711c178a34ab', '2025-09-01 10:01:00', 'batch-001'),
    (2,  'etl-team-alpha', 'crm-primary', 'product',   'd1372818d61d6867', '2025-09-01 10:02:00', 'batch-001'),
    (3,  'etl-team-alpha', 'crm-primary', 'inventory', '59f56ccea00fb870', '2025-09-01 10:03:00', 'batch-001'),
    (4,  'etl-team-alpha', 'crm-primary', 'customer',  'ac986a4aee604217', '2025-09-01 10:04:00', 'batch-001'),
    (5,  'etl-team-alpha', 'crm-primary', 'order',     '17a9cdd0477acb9b', '2025-09-01 10:05:00', 'batch-001'),
    (6,  'etl-team-alpha', 'crm-primary', 'product',   '34b71bc8a9f3e473', '2025-09-01 10:06:00', 'batch-001'),
    (7,  'etl-team-alpha', 'crm-primary', 'inventory', '5658db28249ad0c6', '2025-09-01 10:07:00', 'batch-001'),
    (8,  'etl-team-alpha', 'crm-primary', 'customer',  '9067f85eb69ff8bc', '2025-09-01 10:08:00', 'batch-001'),
    (9,  'etl-team-alpha', 'crm-primary', 'order',     'cfc558f7e37ff10e', '2025-09-01 10:09:00', 'batch-001'),
    (10, 'etl-team-alpha', 'crm-primary', 'product',   'b67dd859bc3fa732', '2025-09-01 10:10:00', 'batch-001'),
    (11, 'etl-team-alpha', 'crm-primary', 'inventory', 'a679d182c7bb5f83', '2025-09-01 10:11:00', 'batch-001'),
    (12, 'etl-team-alpha', 'crm-primary', 'customer',  'e06778d90ca5b830', '2025-09-01 10:12:00', 'batch-001'),
    (13, 'etl-team-alpha', 'crm-primary', 'order',     '76a3a74ed3981432', '2025-09-01 10:13:00', 'batch-001'),
    (14, 'etl-team-alpha', 'crm-primary', 'product',   '33bd4ea0a2384339', '2025-09-01 10:14:00', 'batch-001'),
    (15, 'etl-team-alpha', 'crm-primary', 'inventory', 'e709ac7e6bf0cd65', '2025-09-01 10:15:00', 'batch-001'),
    (16, 'etl-team-alpha', 'crm-primary', 'customer',  '6efb1f4661967d79', '2025-09-01 10:16:00', 'batch-001'),
    (17, 'etl-team-alpha', 'crm-primary', 'order',     'b7fa6f84380ef589', '2025-09-01 10:17:00', 'batch-001'),
    (18, 'etl-team-alpha', 'crm-primary', 'product',   '33d1f3a5d8b0cb19', '2025-09-01 10:18:00', 'batch-001'),
    (19, 'etl-team-alpha', 'crm-primary', 'inventory', '4d18aa5de784ca8e', '2025-09-01 10:19:00', 'batch-001'),
    (20, 'etl-team-alpha', 'crm-primary', 'customer',  '4dabf88eaf50b33e', '2025-09-01 10:20:00', 'batch-001');

-- STEP 4: Create staging table for MERGE corrections (used later in queries.sql)
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.beta_corrections (
    record_id       INT,
    pipeline_name   VARCHAR,
    source_system   VARCHAR,
    record_type     VARCHAR,
    payload_hash    VARCHAR,
    ingested_at     VARCHAR,
    batch_id        VARCHAR
) LOCATION '{{data_path}}/beta_corrections'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.beta_corrections TO USER {{current_user}};
