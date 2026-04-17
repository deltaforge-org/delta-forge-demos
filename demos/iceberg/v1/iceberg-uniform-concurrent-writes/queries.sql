-- ============================================================================
-- Iceberg UniForm Concurrent Multi-Pipeline Writes — Queries
-- ============================================================================
-- HOW UNIFORM WORKS
-- -----------------
-- All queries below read through the Delta transaction log — standard
-- Delta Forge behaviour. The Iceberg metadata in metadata/ is generated
-- automatically by the post-commit hook and is never read by these queries.
--
-- Each DML operation (INSERT, UPDATE, DELETE, MERGE) creates:
--   1. A new Delta version in _delta_log/  (what these queries read)
--   2. A new Iceberg snapshot in metadata/ (for external Iceberg engines)
--
-- This demo simulates three ETL pipelines writing to a shared ingestion log,
-- verifying that Iceberg metadata remains consistent through concurrent-style
-- writes, corrections, and deletions.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify each Iceberg snapshot with:
--   python3 verify_iceberg_metadata.py <table_data_path>/ingestion_log -v
-- ============================================================================
-- ============================================================================
-- EXPLORE: Baseline State (Version 1 / Snapshot 1)
-- ============================================================================
-- 20 records from etl-team-alpha, all from batch-001, source crm-primary.

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.ingestion_log ORDER BY record_id;
-- ============================================================================
-- Query 1: Baseline Pipeline Summary
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE record_count = 20
ASSERT VALUE pipeline_name = 'etl-team-alpha'
SELECT
    pipeline_name,
    COUNT(*) AS record_count
FROM {{zone_name}}.iceberg_demos.ingestion_log
GROUP BY pipeline_name
ORDER BY pipeline_name;
-- ============================================================================
-- LEARN: INSERT — Batch From etl-team-beta (Version 2 / Snapshot 2)
-- ============================================================================
-- 15 records from erp-finance, batch-002. Record types cycle through
-- transaction, customer, shipment.

INSERT INTO {{zone_name}}.iceberg_demos.ingestion_log VALUES
    (21, 'etl-team-beta', 'erp-finance', 'transaction', '05f0a17aa87cbad0', '2025-09-01 11:01:00', 'batch-002'),
    (22, 'etl-team-beta', 'erp-finance', 'customer',    'ddac5351ba45d54d', '2025-09-01 11:02:00', 'batch-002'),
    (23, 'etl-team-beta', 'erp-finance', 'shipment',    '4ea0787e42ef1352', '2025-09-01 11:03:00', 'batch-002'),
    (24, 'etl-team-beta', 'erp-finance', 'transaction', '2a5fe6edb4fc7918', '2025-09-01 11:04:00', 'batch-002'),
    (25, 'etl-team-beta', 'erp-finance', 'customer',    '24f602570bd6681c', '2025-09-01 11:05:00', 'batch-002'),
    (26, 'etl-team-beta', 'erp-finance', 'shipment',    '9220b9a5c67f91bc', '2025-09-01 11:06:00', 'batch-002'),
    (27, 'etl-team-beta', 'erp-finance', 'transaction', '380edd8f52f6d301', '2025-09-01 11:07:00', 'batch-002'),
    (28, 'etl-team-beta', 'erp-finance', 'customer',    'b98cff3fdf88a3a3', '2025-09-01 11:08:00', 'batch-002'),
    (29, 'etl-team-beta', 'erp-finance', 'shipment',    'c942e92227448812', '2025-09-01 11:09:00', 'batch-002'),
    (30, 'etl-team-beta', 'erp-finance', 'transaction', '06f029c579e194d4', '2025-09-01 11:10:00', 'batch-002'),
    (31, 'etl-team-beta', 'erp-finance', 'customer',    '1a16d7f591973b92', '2025-09-01 11:11:00', 'batch-002'),
    (32, 'etl-team-beta', 'erp-finance', 'shipment',    '9e5cdfbee806df91', '2025-09-01 11:12:00', 'batch-002'),
    (33, 'etl-team-beta', 'erp-finance', 'transaction', '7aa46c3057458c97', '2025-09-01 11:13:00', 'batch-002'),
    (34, 'etl-team-beta', 'erp-finance', 'customer',    'd2b239f004f1e9c7', '2025-09-01 11:14:00', 'batch-002'),
    (35, 'etl-team-beta', 'erp-finance', 'shipment',    'db1956e30f5af2eb', '2025-09-01 11:15:00', 'batch-002');
-- ============================================================================
-- Query 2: Row Count After Beta INSERT
-- ============================================================================

ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.iceberg_demos.ingestion_log ORDER BY record_id;
-- ============================================================================
-- LEARN: INSERT — Batch From etl-team-gamma (Version 3 / Snapshot 3)
-- ============================================================================
-- 15 records from iot-sensors, batch-003. Record types cycle through
-- event, metric, log.

INSERT INTO {{zone_name}}.iceberg_demos.ingestion_log VALUES
    (36, 'etl-team-gamma', 'iot-sensors', 'event',  'ff79b0c3c1512f81', '2025-09-01 12:01:00', 'batch-003'),
    (37, 'etl-team-gamma', 'iot-sensors', 'metric', '6fe74a5279c97a96', '2025-09-01 12:02:00', 'batch-003'),
    (38, 'etl-team-gamma', 'iot-sensors', 'log',    'a9eabe8a623236db', '2025-09-01 12:03:00', 'batch-003'),
    (39, 'etl-team-gamma', 'iot-sensors', 'event',  'cb32bc49a449a1b0', '2025-09-01 12:04:00', 'batch-003'),
    (40, 'etl-team-gamma', 'iot-sensors', 'metric', '8c290aee130b7923', '2025-09-01 12:05:00', 'batch-003'),
    (41, 'etl-team-gamma', 'iot-sensors', 'log',    'ff1d1e9eca25df4d', '2025-09-01 12:06:00', 'batch-003'),
    (42, 'etl-team-gamma', 'iot-sensors', 'event',  'ae01562144a2db43', '2025-09-01 12:07:00', 'batch-003'),
    (43, 'etl-team-gamma', 'iot-sensors', 'metric', '7f1cd3d3dea50239', '2025-09-01 12:08:00', 'batch-003'),
    (44, 'etl-team-gamma', 'iot-sensors', 'log',    '13741736f66ea69e', '2025-09-01 12:09:00', 'batch-003'),
    (45, 'etl-team-gamma', 'iot-sensors', 'event',  '6e60710ab5746bc0', '2025-09-01 12:10:00', 'batch-003'),
    (46, 'etl-team-gamma', 'iot-sensors', 'metric', 'c2b4ec45e21781d5', '2025-09-01 12:11:00', 'batch-003'),
    (47, 'etl-team-gamma', 'iot-sensors', 'log',    'ffdee59ad88afc65', '2025-09-01 12:12:00', 'batch-003'),
    (48, 'etl-team-gamma', 'iot-sensors', 'event',  '5e59582c52bcc1a0', '2025-09-01 12:13:00', 'batch-003'),
    (49, 'etl-team-gamma', 'iot-sensors', 'metric', '3f0552b96cf3d0cd', '2025-09-01 12:14:00', 'batch-003'),
    (50, 'etl-team-gamma', 'iot-sensors', 'log',    'de1b8d8c103d6930', '2025-09-01 12:15:00', 'batch-003');
-- ============================================================================
-- Query 3: All Three Pipelines Coexist
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.iceberg_demos.ingestion_log ORDER BY record_id;
-- ============================================================================
-- Query 4: Per-Pipeline Counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE record_count = 20 WHERE pipeline_name = 'etl-team-alpha'
ASSERT VALUE record_count = 15 WHERE pipeline_name = 'etl-team-beta'
ASSERT VALUE record_count = 15 WHERE pipeline_name = 'etl-team-gamma'
SELECT
    pipeline_name,
    COUNT(*) AS record_count
FROM {{zone_name}}.iceberg_demos.ingestion_log
GROUP BY pipeline_name
ORDER BY pipeline_name;
-- ============================================================================
-- Query 5: Per-Batch Counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE record_count = 20 WHERE batch_id = 'batch-001'
ASSERT VALUE record_count = 15 WHERE batch_id = 'batch-002'
ASSERT VALUE record_count = 15 WHERE batch_id = 'batch-003'
SELECT
    batch_id,
    COUNT(*) AS record_count
FROM {{zone_name}}.iceberg_demos.ingestion_log
GROUP BY batch_id
ORDER BY batch_id;
-- ============================================================================
-- LEARN: UPDATE — Reprocess Alpha Records (Version 4 / Snapshot 4)
-- ============================================================================
-- Team-alpha discovers that records 3, 7, 11, 15, 19 had stale data
-- and reprocesses them. Mark record_type as 'reprocessed' and update hash.

UPDATE {{zone_name}}.iceberg_demos.ingestion_log
SET record_type = 'reprocessed', payload_hash = '247b1dd9225a4316'
WHERE record_id = 3;

UPDATE {{zone_name}}.iceberg_demos.ingestion_log
SET record_type = 'reprocessed', payload_hash = 'dd35f27f33c28f7b'
WHERE record_id = 7;

UPDATE {{zone_name}}.iceberg_demos.ingestion_log
SET record_type = 'reprocessed', payload_hash = 'e95f3e48ee04b74a'
WHERE record_id = 11;

UPDATE {{zone_name}}.iceberg_demos.ingestion_log
SET record_type = 'reprocessed', payload_hash = 'b2dea09f63912da9'
WHERE record_id = 15;

UPDATE {{zone_name}}.iceberg_demos.ingestion_log
SET record_type = 'reprocessed', payload_hash = '26da0b8419fb678d'
WHERE record_id = 19;
-- ============================================================================
-- Query 6: Verify Reprocessed Records
-- ============================================================================

ASSERT ROW_COUNT = 5
SELECT
    record_id,
    record_type,
    payload_hash
FROM {{zone_name}}.iceberg_demos.ingestion_log
WHERE record_type = 'reprocessed'
ORDER BY record_id;
-- ============================================================================
-- Query 7: Total Count Unchanged After UPDATE
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.iceberg_demos.ingestion_log ORDER BY record_id;
-- ============================================================================
-- LEARN: MERGE — Beta Team Corrections (Version 5 / Snapshot 5)
-- ============================================================================
-- Team-beta sends corrections: update 3 existing records (21, 25, 30) with
-- new hashes and mark as 'corrected', plus insert 5 new records (51-55)
-- in batch-004.

INSERT INTO {{zone_name}}.iceberg_demos.beta_corrections VALUES
    (21, 'etl-team-beta', 'erp-finance', 'corrected',   '7e66326c3dfbffc1', '2025-09-01 14:01:00', 'batch-004'),
    (25, 'etl-team-beta', 'erp-finance', 'corrected',   '16d698342c6c9f34', '2025-09-01 14:02:00', 'batch-004'),
    (30, 'etl-team-beta', 'erp-finance', 'corrected',   '35a9648d3ab57cda', '2025-09-01 14:03:00', 'batch-004'),
    (51, 'etl-team-beta', 'erp-finance', 'transaction', '428ba32d2b04a111', '2025-09-01 14:01:00', 'batch-004'),
    (52, 'etl-team-beta', 'erp-finance', 'customer',    '1321abe0a576812b', '2025-09-01 14:02:00', 'batch-004'),
    (53, 'etl-team-beta', 'erp-finance', 'shipment',    'e8a061ef62bfebc3', '2025-09-01 14:03:00', 'batch-004'),
    (54, 'etl-team-beta', 'erp-finance', 'transaction', 'a87d3107a3f74279', '2025-09-01 14:04:00', 'batch-004'),
    (55, 'etl-team-beta', 'erp-finance', 'customer',    '7e83147bd3a64103', '2025-09-01 14:05:00', 'batch-004');

MERGE INTO {{zone_name}}.iceberg_demos.ingestion_log AS target
USING {{zone_name}}.iceberg_demos.beta_corrections AS source
ON target.record_id = source.record_id
WHEN MATCHED THEN
    UPDATE SET
        record_type  = source.record_type,
        payload_hash = source.payload_hash,
        ingested_at  = source.ingested_at,
        batch_id     = source.batch_id
WHEN NOT MATCHED THEN
    INSERT (record_id, pipeline_name, source_system, record_type, payload_hash, ingested_at, batch_id)
    VALUES (source.record_id, source.pipeline_name, source.source_system,
            source.record_type, source.payload_hash, source.ingested_at, source.batch_id);
-- ============================================================================
-- Query 8: Post-MERGE Row Count — 50 + 5 New = 55
-- ============================================================================

ASSERT ROW_COUNT = 55
SELECT * FROM {{zone_name}}.iceberg_demos.ingestion_log ORDER BY record_id;
-- ============================================================================
-- Query 9: Verify Corrected Records
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE record_type = 'corrected' WHERE record_id = 21
ASSERT VALUE record_type = 'corrected' WHERE record_id = 25
ASSERT VALUE record_type = 'corrected' WHERE record_id = 30
SELECT
    record_id,
    record_type,
    payload_hash,
    batch_id
FROM {{zone_name}}.iceberg_demos.ingestion_log
WHERE record_id IN (21, 25, 30)
ORDER BY record_id;
-- ============================================================================
-- Query 10: Beta Total After MERGE
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE beta_count = 20
SELECT
    COUNT(*) AS beta_count
FROM {{zone_name}}.iceberg_demos.ingestion_log
WHERE pipeline_name = 'etl-team-beta';
-- ============================================================================
-- LEARN: DELETE — Remove Failed Gamma Records (Version 6 / Snapshot 6)
-- ============================================================================
-- Records 36-40 from batch-003 failed validation and must be purged.

DELETE FROM {{zone_name}}.iceberg_demos.ingestion_log
WHERE record_id IN (36, 37, 38, 39, 40);
-- ============================================================================
-- Query 11: Post-DELETE Row Count — 55 - 5 = 50
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.iceberg_demos.ingestion_log ORDER BY record_id;
-- ============================================================================
-- Query 12: Final Per-Pipeline Counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE record_count = 20 WHERE pipeline_name = 'etl-team-alpha'
ASSERT VALUE record_count = 20 WHERE pipeline_name = 'etl-team-beta'
ASSERT VALUE record_count = 10 WHERE pipeline_name = 'etl-team-gamma'
SELECT
    pipeline_name,
    COUNT(*) AS record_count
FROM {{zone_name}}.iceberg_demos.ingestion_log
GROUP BY pipeline_name
ORDER BY pipeline_name;
-- ============================================================================
-- Query 13: Final Per-Batch Counts
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE record_count = 20 WHERE batch_id = 'batch-001'
ASSERT VALUE record_count = 12 WHERE batch_id = 'batch-002'
ASSERT VALUE record_count = 10 WHERE batch_id = 'batch-003'
ASSERT VALUE record_count = 8  WHERE batch_id = 'batch-004'
SELECT
    batch_id,
    COUNT(*) AS record_count
FROM {{zone_name}}.iceberg_demos.ingestion_log
GROUP BY batch_id
ORDER BY batch_id;
-- ============================================================================
-- Query 14: Time Travel — State at Each Version Boundary
-- ============================================================================
-- V1: 20 (seed alpha)
-- V2: 35 (after beta INSERT)
-- V3: 50 (after gamma INSERT)
-- Current: 50 (after UPDATE + MERGE + DELETE)

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_count = 20
ASSERT VALUE v2_count = 35
ASSERT VALUE v3_count = 50
ASSERT VALUE current_count = 50
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.ingestion_log VERSION AS OF 1) AS v1_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.ingestion_log VERSION AS OF 2) AS v2_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.ingestion_log VERSION AS OF 3) AS v3_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.ingestion_log) AS current_count;
-- ============================================================================
-- Query 15: Version History
-- ============================================================================
-- Each version maps to an Iceberg snapshot. The history shows:
-- V1: Initial INSERT (20 alpha records)
-- V2: INSERT beta batch (15 records)
-- V3: INSERT gamma batch (15 records)
-- V4-V8: UPDATE reprocessed (5 individual updates)
-- V9: INSERT beta_corrections staging data
-- V10: MERGE corrections (3 updated + 5 inserted)
-- V11: DELETE failed gamma records

ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.ingestion_log;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check covering the full multi-pipeline lifecycle.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_records = 50
ASSERT VALUE pipeline_count = 3
ASSERT VALUE alpha_count = 20
ASSERT VALUE beta_count = 20
ASSERT VALUE gamma_count = 10
ASSERT VALUE batch_count = 4
ASSERT VALUE source_count = 3
ASSERT VALUE reprocessed_count = 5
ASSERT VALUE corrected_count = 3
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT pipeline_name) AS pipeline_count,
    COUNT(*) FILTER (WHERE pipeline_name = 'etl-team-alpha') AS alpha_count,
    COUNT(*) FILTER (WHERE pipeline_name = 'etl-team-beta') AS beta_count,
    COUNT(*) FILTER (WHERE pipeline_name = 'etl-team-gamma') AS gamma_count,
    COUNT(DISTINCT batch_id) AS batch_count,
    COUNT(DISTINCT source_system) AS source_count,
    COUNT(*) FILTER (WHERE record_type = 'reprocessed') AS reprocessed_count,
    COUNT(*) FILTER (WHERE record_type = 'corrected') AS corrected_count
FROM {{zone_name}}.iceberg_demos.ingestion_log;
-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata is readable by an Iceberg engine after the full
-- multi-pipeline lifecycle (INSERT x3 → UPDATE → MERGE → DELETE).
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.ingestion_log_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.ingestion_log_iceberg
USING ICEBERG
LOCATION '{{data_path}}/ingestion_log';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.ingestion_log_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Verify 1: Row Count — 50 Records After Full Lifecycle
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.iceberg_demos.ingestion_log_iceberg ORDER BY record_id;
-- ============================================================================
-- Iceberg Verify 2: Per-Pipeline Counts — Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE record_count = 20 WHERE pipeline_name = 'etl-team-alpha'
ASSERT VALUE record_count = 20 WHERE pipeline_name = 'etl-team-beta'
ASSERT VALUE record_count = 10 WHERE pipeline_name = 'etl-team-gamma'
SELECT
    pipeline_name,
    COUNT(*) AS record_count
FROM {{zone_name}}.iceberg_demos.ingestion_log_iceberg
GROUP BY pipeline_name
ORDER BY pipeline_name;
-- ============================================================================
-- Iceberg Verify 3: Grand Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_records = 50
ASSERT VALUE pipeline_count = 3
ASSERT VALUE batch_count = 4
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT pipeline_name) AS pipeline_count,
    COUNT(DISTINCT batch_id) AS batch_count
FROM {{zone_name}}.iceberg_demos.ingestion_log_iceberg;
