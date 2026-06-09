-- ============================================================================
-- Delta Edge Cases — Educational Queries
-- ============================================================================
-- WHAT: Tests Delta table behavior with boundary conditions: single-row
--       tables, wide schemas (30 columns), and empty tables.
-- WHY:  Real-world pipelines encounter these patterns regularly — config
--       singletons, denormalized wide tables, and pre-created staging tables
--       that start empty. Understanding how Delta handles them prevents
--       surprises in production.
-- HOW:  Each edge case creates a separate Delta table with its own
--       transaction log. Even a single-row table gets full ACID semantics,
--       and an empty table still has a valid schema in its metadata actions.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Singleton Pattern — One Row, Many Versions
-- ============================================================================
-- A "singleton" table holds exactly one row that gets updated in place.
-- This is common for application config, feature flags, or system state.
-- In Delta, each UPDATE creates a new version in the transaction log,
-- so you get full version history even for a single row.
--
-- After setup, the config_singleton table has version 1 (the baseline):

ASSERT ROW_COUNT = 1
ASSERT VALUE version = 1
ASSERT VALUE updated_by = 'admin'
SELECT config_key, config_value, version, updated_by, updated_at
FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- EVOLVE: Update Config — Increase Timeout
-- ============================================================================
-- The ops team doubles the timeout from 5000ms to 10000ms and bumps
-- the version. This UPDATE replaces the single Parquet file and creates
-- a new Delta transaction log entry — version 2.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.config_singleton
SET config_value = '{"max_connections":100,"timeout_ms":10000,"debug":false}',
    version = 2,
    updated_by = 'ops-team',
    updated_at = '2025-01-15 10:00:00';


-- ============================================================================
-- EVOLVE: Optimistic Locking — Update Only If Version Matches
-- ============================================================================
-- The dev lead wants to enable debug mode, but only if no one else has
-- changed the config since they last read it. The WHERE version = 2
-- clause acts as an optimistic lock — if another process had already
-- bumped the version, this UPDATE would affect 0 rows instead of 1.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.config_singleton
SET config_value = '{"max_connections":200,"timeout_ms":10000,"debug":true}',
    version = 3,
    updated_by = 'dev-lead',
    updated_at = '2025-02-01 14:30:00'
WHERE version = 2;


-- ============================================================================
-- EVOLVE: Verify Optimistic Lock
-- ============================================================================
-- The UPDATE matched 1 row (version was indeed 2), so the lock succeeded.
-- If version had already been bumped by another process, the UPDATE above
-- would have returned ROW_COUNT = 0 — a signal to retry or abort.

ASSERT ROW_COUNT = 1
ASSERT VALUE version = 3
ASSERT VALUE updated_by = 'dev-lead'
SELECT version, updated_by, config_value
FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- EVOLVE: Full Row Replace — DELETE + Re-INSERT
-- ============================================================================
-- Sometimes you want to replace the entire row rather than update
-- individual columns — for example, when migrating to a new config
-- schema. Delta handles this as two separate transactions: a DELETE
-- (marking the old Parquet file as removed) and an INSERT (writing
-- a new one).

-- Delta's rows_affected counts all versioned row entries (one per UPDATE),
-- not just the current snapshot row count. After 2 UPDATEs, expect >= 1.
ASSERT ROW_COUNT >= 1
DELETE FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- EVOLVE: Re-INSERT with New Config Schema
-- ============================================================================
-- Now insert a fresh row with a completely new configuration structure.
-- The new config adds a "region" field that didn't exist before — this
-- kind of schema migration is why DELETE + re-INSERT is sometimes
-- preferred over UPDATE.

ASSERT ROW_COUNT = 1
INSERT INTO {{zone_name}}.delta_demos.config_singleton VALUES
    ('app_settings', '{"max_connections":250,"timeout_ms":3000,"debug":true,"region":"us-east-1"}', 1, 'sre-team', '2025-03-15 12:00:00');


-- ============================================================================
-- LEARN: Aggregation on a Single Row
-- ============================================================================
-- Aggregation functions work correctly on a 1-row table. COUNT returns 1,
-- and AVG/SUM/MIN/MAX all return the single value. This matters for
-- dashboards that compute averages — a singleton config or status table
-- should not break an aggregation pipeline.

ASSERT ROW_COUNT = 1
ASSERT VALUE cnt = 1
ASSERT VALUE avg_version = 1
ASSERT VALUE sum_version = 1
SELECT COUNT(*) AS cnt,
       AVG(version) AS avg_version,
       SUM(version) AS sum_version,
       MIN(updated_by) AS min_updater,
       MAX(updated_at) AS max_update_time
FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- EXPLORE: Wide Tables — 30 Columns of KPI Data
-- ============================================================================
-- Delta tables handle wide schemas without issue. The column metadata is
-- stored in the transaction log's schema action, and Parquet's columnar
-- format means queries that only access a few columns skip reading the rest.
-- Let's look at a subset of the 30 columns:

ASSERT ROW_COUNT = 20
SELECT id, name, m01_revenue, m03_profit, m04_margin_pct,
       m12_satisfaction, m16_uptime_pct
FROM {{zone_name}}.delta_demos.wide_metrics
ORDER BY id;


-- ============================================================================
-- EVOLVE: Surgical Column Update — Correct a Single Metric
-- ============================================================================
-- Wide tables often need targeted corrections — fix one metric without
-- touching the other 29 columns. Delta rewrites only the affected Parquet
-- file, not the entire table. Here we correct January's revenue figure
-- and recalculate the derived columns:

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.wide_metrics
SET m01_revenue = 131000.0,
    m03_profit = 46000.0,
    m04_margin_pct = 35.1
WHERE id = 1;


-- ============================================================================
-- EVOLVE: Pruning Rows — Remove Provisional Data
-- ============================================================================
-- The last two months (Jul-2025, Aug-2025) were provisional estimates.
-- Now that actuals are in, we delete the provisional rows before loading
-- the real data. This is a common pattern in financial reporting pipelines.

ASSERT ROW_COUNT = 2
DELETE FROM {{zone_name}}.delta_demos.wide_metrics
WHERE id > 18;


-- ============================================================================
-- LEARN: Cross-Column Analytics on a Wide Table
-- ============================================================================
-- With 18 remaining months and 30 columns, let's compute a blended
-- summary that spans revenue, profit, satisfaction, and error metrics.
-- This demonstrates that wide Delta tables support complex analytical
-- queries across many columns efficiently:

ASSERT ROW_COUNT = 1
ASSERT VALUE months = 18
ASSERT VALUE total_revenue = 2782000.0
ASSERT VALUE total_profit = 1039000.0
ASSERT VALUE blended_margin_pct = 37.3
SELECT COUNT(*) AS months,
       SUM(m01_revenue) AS total_revenue,
       SUM(m03_profit) AS total_profit,
       ROUND(SUM(m03_profit) * 100.0 / SUM(m01_revenue), 1) AS blended_margin_pct,
       ROUND(AVG(m12_satisfaction), 2) AS avg_satisfaction,
       SUM(m05_units_sold) AS total_units,
       MIN(m19_error_rate) AS best_error_rate,
       MAX(m01_revenue) AS peak_revenue
FROM {{zone_name}}.delta_demos.wide_metrics;


-- ============================================================================
-- EXPLORE: Empty Tables — Schema Without Data
-- ============================================================================
-- An empty Delta table has a valid transaction log with schema metadata
-- but zero data files. Queries against empty tables return zero rows
-- (not errors), and aggregates return NULL — exactly what downstream
-- pipelines expect:

ASSERT ROW_COUNT = 1
ASSERT VALUE row_count = 0
ASSERT VALUE max_id IS NULL
SELECT COUNT(*) AS row_count,
       MAX(id) AS max_id,
       MIN(source_system) AS first_source
FROM {{zone_name}}.delta_demos.empty_staging;


-- ============================================================================
-- EVOLVE: Populate the Staging Table — Empty to Loaded
-- ============================================================================
-- The staging table receives its first batch of incoming events. Delta
-- writes the first Parquet data file and advances the transaction log
-- from version 0 (schema-only) to version 1 (schema + data):

ASSERT ROW_COUNT = 3
INSERT INTO {{zone_name}}.delta_demos.empty_staging VALUES
    (1, 'crm',  '{"event":"signup","user":"alice"}',   'pending', '2025-03-15 08:00:00'),
    (2, 'erp',  '{"event":"order","amount":249.99}',   'pending', '2025-03-15 08:01:00'),
    (3, 'crm',  '{"event":"login","user":"bob"}',      'pending', '2025-03-15 08:02:00');


-- ============================================================================
-- EVOLVE: Clear the Staging Table — Back to Empty
-- ============================================================================
-- After the staging data has been processed downstream, we clear the
-- table for the next batch. The DELETE creates a new transaction log
-- version that removes all data files. The table is empty again but
-- retains its full schema and version history.

ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.delta_demos.empty_staging;


-- ============================================================================
-- LEARN: Empty Again — Same Behavior, Higher Version
-- ============================================================================
-- The table is now back to zero rows, but its transaction log has
-- grown: version 0 (create), version 1 (insert 3 rows), version 2
-- (delete all). Aggregates return NULL again, just like before the
-- first insert — proving that Delta's empty-table semantics are
-- consistent regardless of history:

ASSERT ROW_COUNT = 1
ASSERT VALUE row_count = 0
ASSERT VALUE max_id IS NULL
SELECT COUNT(*) AS row_count,
       MAX(id) AS max_id,
       MIN(source_system) AS first_source
FROM {{zone_name}}.delta_demos.empty_staging;


-- ============================================================================
-- EDGE: No-Op Update — WHERE Matches Zero Rows
-- ============================================================================
-- A WHERE clause that matches zero rows is not an error — it's a valid
-- no-op. Delta still creates a new transaction log entry (a commit with
-- zero actions), but no Parquet files are rewritten. This is important
-- for idempotent pipelines that run UPDATE statements unconditionally:

ASSERT ROW_COUNT = 0
UPDATE {{zone_name}}.delta_demos.wide_metrics
SET m01_revenue = 999999.0
WHERE id = 999;


-- ============================================================================
-- EDGE: Cross-Table JOIN — Wide Data Meets Empty Table
-- ============================================================================
-- LEFT JOIN with an empty table produces NULL for all right-side
-- columns. This is expected SQL behavior, but it's worth confirming
-- that Delta handles it correctly across two separate transaction
-- logs. Pipelines often JOIN a fact table with a staging or lookup
-- table that may not have data yet:

ASSERT ROW_COUNT = 3
ASSERT VALUE staging_source IS NULL
SELECT w.id, w.name, w.m01_revenue,
       s.source_system AS staging_source
FROM {{zone_name}}.delta_demos.wide_metrics w
LEFT JOIN {{zone_name}}.delta_demos.empty_staging s ON w.id = s.id
WHERE w.id <= 3
ORDER BY w.id;


-- ============================================================================
-- VERIFY: Singleton State
-- ============================================================================

-- Verify singleton_row_count: exactly 1 row after DELETE + re-INSERT
ASSERT ROW_COUNT = 1
ASSERT VALUE version = 1
ASSERT VALUE updated_by = 'sre-team'
ASSERT VALUE config_value = '{"max_connections":250,"timeout_ms":3000,"debug":true,"region":"us-east-1"}'
SELECT * FROM {{zone_name}}.delta_demos.config_singleton;


-- ============================================================================
-- VERIFY: Wide Table State
-- ============================================================================

-- Verify wide metrics after update + prune: 18 rows, corrected revenue
ASSERT VALUE cnt = 18
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.wide_metrics;


-- ============================================================================
-- VERIFY: Wide Table Values
-- ============================================================================

-- Verify key aggregations reflect all mutations
ASSERT VALUE corrected_revenue = 131000.0
ASSERT VALUE max_rev = 190000.0
ASSERT VALUE total_profit = 1039000.0
SELECT (SELECT m01_revenue FROM {{zone_name}}.delta_demos.wide_metrics WHERE id = 1) AS corrected_revenue,
       (SELECT MAX(m01_revenue) FROM {{zone_name}}.delta_demos.wide_metrics) AS max_rev,
       (SELECT SUM(m03_profit) FROM {{zone_name}}.delta_demos.wide_metrics) AS total_profit;


-- ============================================================================
-- VERIFY: Empty Table State
-- ============================================================================

-- Verify staging is empty after full lifecycle
ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.delta_demos.empty_staging;


-- ============================================================================
-- VERIFY: Empty Table Aggregates
-- ============================================================================

-- Verify NULL aggregates on empty table
ASSERT VALUE max_id IS NULL
SELECT MAX(id) AS max_id FROM {{zone_name}}.delta_demos.empty_staging;
