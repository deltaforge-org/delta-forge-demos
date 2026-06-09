-- ============================================================================
-- Concurrent CDR Ingestion -- Setup Script
-- ============================================================================
-- Scenario: a mobile carrier runs 10 regional Mobile Switching Centers (MSCs).
-- Each MSC exports a daily Call Detail Record (CDR) file with an identical
-- schema (region_01.csv .. region_10.csv, 20,350 rows in total). The nightly
-- mediation job loads all 10 regional feeds CONCURRENTLY into one central,
-- rated-CDR Delta table.
--
-- This demo proves DeltaForge's optimistic concurrency control: ten writers
-- appending to a single Delta table at the same time land every record
-- exactly once, with no lost writes and no duplicates. Two concurrency
-- mechanisms are exercised against two separate target tables:
--
--   1. cdr_consolidated -- filled by CONCURRENT BEGIN...END, ten explicit
--      INSERT statements (one per regional feed) dispatched in parallel.
--   2. cdr_parallel     -- filled by a single PARALLEL INSERT that range-splits
--      the unified feed on region_id and writes the ranges concurrently.
--
-- The load statements live here in setup (not queries.sql) so the verification
-- queries stay strictly read-only and idempotent across re-runs.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Zone & Schema
-- ----------------------------------------------------------------------------
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External CSV feeds plus consolidated Delta tables';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.cdr
    COMMENT 'Telecom call-detail-record ingestion demo';

-- ----------------------------------------------------------------------------
-- Regional feed external tables -- one per MSC export file
-- ----------------------------------------------------------------------------
-- Each external table reads exactly one regional CDR file. The ten tables are
-- the ten independent sources the consolidation job writes concurrently.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_01
USING CSV LOCATION 'concurrent-cdr-ingestion/region_01.csv' OPTIONS (header = 'true');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_02
USING CSV LOCATION 'concurrent-cdr-ingestion/region_02.csv' OPTIONS (header = 'true');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_03
USING CSV LOCATION 'concurrent-cdr-ingestion/region_03.csv' OPTIONS (header = 'true');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_04
USING CSV LOCATION 'concurrent-cdr-ingestion/region_04.csv' OPTIONS (header = 'true');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_05
USING CSV LOCATION 'concurrent-cdr-ingestion/region_05.csv' OPTIONS (header = 'true');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_06
USING CSV LOCATION 'concurrent-cdr-ingestion/region_06.csv' OPTIONS (header = 'true');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_07
USING CSV LOCATION 'concurrent-cdr-ingestion/region_07.csv' OPTIONS (header = 'true');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_08
USING CSV LOCATION 'concurrent-cdr-ingestion/region_08.csv' OPTIONS (header = 'true');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_09
USING CSV LOCATION 'concurrent-cdr-ingestion/region_09.csv' OPTIONS (header = 'true');

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.region_10
USING CSV LOCATION 'concurrent-cdr-ingestion/region_10.csv' OPTIONS (header = 'true');

-- Unified view over all ten files via a glob. Used as the single range-split
-- source for PARALLEL INSERT and as the source-of-truth baseline in queries.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.cdr.all_cdr
USING CSV LOCATION 'concurrent-cdr-ingestion/region_*.csv' OPTIONS (header = 'true');

-- ----------------------------------------------------------------------------
-- Target Delta tables -- created empty, populated by the concurrent loads below
-- ----------------------------------------------------------------------------
-- Column types match the CSV inference: text stays VARCHAR, whole numbers map
-- to BIGINT, money columns to DOUBLE. call_start is kept as a string so no
-- timestamp parsing is required on ingest.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.cdr.cdr_consolidated (
    cdr_id            VARCHAR,
    region_id         BIGINT,
    region_name       VARCHAR,
    caller_number     VARCHAR,
    callee_number     VARCHAR,
    call_start        VARCHAR,
    duration_seconds  BIGINT,
    call_type         VARCHAR,
    bytes_transferred BIGINT,
    network_type      VARCHAR,
    rate_per_minute   DOUBLE,
    charge_amount     DOUBLE
) LOCATION 'concurrent-cdr-ingestion/cdr_consolidated';

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.cdr.cdr_parallel (
    cdr_id            VARCHAR,
    region_id         BIGINT,
    region_name       VARCHAR,
    caller_number     VARCHAR,
    callee_number     VARCHAR,
    call_start        VARCHAR,
    duration_seconds  BIGINT,
    call_type         VARCHAR,
    bytes_transferred BIGINT,
    network_type      VARCHAR,
    rate_per_minute   DOUBLE,
    charge_amount     DOUBLE
) LOCATION 'concurrent-cdr-ingestion/cdr_parallel';

-- ----------------------------------------------------------------------------
-- Concurrent load #1 -- CONCURRENT BEGIN...END
-- ----------------------------------------------------------------------------
-- Ten INSERT statements dispatched in parallel, each appending one regional
-- feed to the SAME Delta table. Every append is a blind append, so optimistic
-- concurrency control resolves the commits without losing or duplicating rows.
-- One successful commit per statement yields ten new versions on top of the
-- create commit (version 0), i.e. eleven versions total.

CONCURRENT BEGIN
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_01;
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_02;
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_03;
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_04;
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_05;
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_06;
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_07;
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_08;
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_09;
    INSERT INTO {{zone_name}}.cdr.cdr_consolidated SELECT * FROM {{zone_name}}.cdr.region_10;
END;

-- ----------------------------------------------------------------------------
-- Concurrent load #2 -- PARALLEL INSERT INTO
-- ----------------------------------------------------------------------------
-- The unified feed is range-partitioned on the numeric region_id and the
-- ranges are written concurrently to a second Delta table. Both methods must
-- produce identical, complete, duplicate-free results.

PARALLEL INSERT INTO {{zone_name}}.cdr.cdr_parallel
    SELECT * FROM {{zone_name}}.cdr.all_cdr
    SPLIT BY region_id
    BATCH SIZE 4000
    THREADS 8;
