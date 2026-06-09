-- ============================================================================
-- Delta Protocol Feature Inspection — Educational Queries
-- ============================================================================
-- WHAT: Every Delta table carries protocol metadata that declares which
--       features (CDC, deletion vectors, constraints, etc.) are active.
-- WHY:  Before building downstream pipelines on an inherited table, you
--       must know what features are enabled — CDC tables produce change
--       data, constrained tables reject bad writes, and protocol versions
--       determine which engines can read/write the table.
-- HOW:  DESCRIBE DETAIL exposes min_reader_version, min_writer_version, and
--       table_features. SHOW TABLE PROPERTIES reveals TBLPROPERTIES config.
--       DESCRIBE HISTORY shows the operation log. Together, these three
--       commands give you a complete forensic picture of any Delta table.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Inherited Tables — What Do We Have?
-- ============================================================================
-- A colleague left three Delta tables behind. Before we can build ETL
-- pipelines, we need to understand what each table contains and what
-- protocol features are active.

ASSERT VALUE row_count = 15
ASSERT VALUE categories = 4
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT category) AS categories,
       COUNT(DISTINCT CAST(in_stock AS VARCHAR)) AS stock_states
FROM {{zone_name}}.delta_demos.inherited_plain;


-- ============================================================================
-- LEARN: DESCRIBE DETAIL — The Protocol Forensics Tool
-- ============================================================================
-- DESCRIBE DETAIL returns key-value pairs with protocol metadata.
-- For a plain table (no extra features), we expect minimal protocol
-- versions and no special table_features listed.

ASSERT VALUE value = '15' WHERE property = 'estimated_rows'
ASSERT ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.inherited_plain;


-- ============================================================================
-- LEARN: SHOW TABLE PROPERTIES — Configuration Inspection
-- ============================================================================
-- SHOW TABLE PROPERTIES reveals TBLPROPERTIES stored in the Delta
-- transaction log's metaData action. A plain table has no custom
-- properties — only system defaults.

ASSERT ROW_COUNT >= 0
SHOW TBLPROPERTIES {{zone_name}}.delta_demos.inherited_plain;


-- ============================================================================
-- EXPLORE: The CDC-Enabled Table — Customer Subscriptions
-- ============================================================================
-- The second table has enableChangeDataFeed = true. Let's first see
-- the business data, then inspect the protocol to confirm CDC is active.

ASSERT ROW_COUNT = 3
ASSERT VALUE customer_count = 6 WHERE plan = 'enterprise'
ASSERT VALUE customer_count = 4 WHERE plan = 'growth'
ASSERT VALUE customer_count = 2 WHERE plan = 'startup'
SELECT plan,
       COUNT(*) AS customer_count,
       ROUND(SUM(monthly_spend), 2) AS total_mrr,
       ROUND(AVG(monthly_spend), 2) AS avg_spend
FROM {{zone_name}}.delta_demos.inherited_cdc
GROUP BY plan
ORDER BY total_mrr DESC;


-- ============================================================================
-- LEARN: DESCRIBE DETAIL on a CDC Table
-- ============================================================================
-- With enableChangeDataFeed = true, the protocol records this as a
-- writer feature. The min_writer_version should reflect this requirement.
-- The table_features field will list 'changeDataFeed' (or similar).

ASSERT VALUE value = '12' WHERE property = 'estimated_rows'
ASSERT ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.inherited_cdc;


-- ============================================================================
-- LEARN: SHOW TABLE PROPERTIES Confirms CDC
-- ============================================================================
-- The TBLPROPERTIES should include delta.enableChangeDataFeed = true.
-- This is the configuration layer — DESCRIBE DETAIL shows the protocol
-- effect, SHOW TABLE PROPERTIES shows the configuration that caused it.

ASSERT ROW_COUNT >= 1
SHOW TBLPROPERTIES {{zone_name}}.delta_demos.inherited_cdc;


-- ============================================================================
-- EXPLORE: The Constrained Table — Building Supply Inventory
-- ============================================================================
-- The third table has CDC enabled. Let's see the business data.

ASSERT ROW_COUNT = 3
SELECT warehouse,
       COUNT(*) AS item_count,
       SUM(quantity) AS total_qty,
       ROUND(SUM(quantity * unit_price * (1.0 - discount_pct / 100.0)), 2) AS inventory_value
FROM {{zone_name}}.delta_demos.inherited_constrained
GROUP BY warehouse
ORDER BY inventory_value DESC;


-- ============================================================================
-- LEARN: DESCRIBE DETAIL on a CDC + Constraints Table
-- ============================================================================
-- This table has CDC enabled. DESCRIBE DETAIL will show the accumulated
-- protocol requirements from all enabled features.

ASSERT VALUE value = '10' WHERE property = 'estimated_rows'
ASSERT ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.inherited_constrained;


-- ============================================================================
-- LEARN: DESCRIBE HISTORY — Operation Audit Trail
-- ============================================================================
-- DESCRIBE HISTORY shows every committed transaction. For inherited tables,
-- this tells you WHEN and HOW the table was created and populated.
-- Each entry includes: version, timestamp, operation, metrics.

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.delta_demos.inherited_cdc;


-- ============================================================================
-- EXPLORE: Cross-Table Protocol Comparison
-- ============================================================================
-- Let's compare all three tables' business metrics side-by-side. This
-- confirms that the tables are healthy and queryable before we build
-- downstream pipelines.

ASSERT VALUE total_items = 15 WHERE table_name = 'inherited_plain'
ASSERT VALUE total_items = 12 WHERE table_name = 'inherited_cdc'
ASSERT VALUE total_items = 10 WHERE table_name = 'inherited_constrained'
ASSERT ROW_COUNT = 3
SELECT 'inherited_plain' AS table_name,
       COUNT(*) AS total_items
FROM {{zone_name}}.delta_demos.inherited_plain
UNION ALL
SELECT 'inherited_cdc' AS table_name,
       COUNT(*) AS total_items
FROM {{zone_name}}.delta_demos.inherited_cdc
UNION ALL
SELECT 'inherited_constrained' AS table_name,
       COUNT(*) AS total_items
FROM {{zone_name}}.delta_demos.inherited_constrained;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify inherited_plain has 15 rows, 4 categories
ASSERT VALUE plain_count = 15
SELECT COUNT(*) AS plain_count FROM {{zone_name}}.delta_demos.inherited_plain;

ASSERT VALUE plain_categories = 4
SELECT COUNT(DISTINCT category) AS plain_categories FROM {{zone_name}}.delta_demos.inherited_plain;

-- Verify inherited_cdc has 12 rows, 3 plans
ASSERT VALUE cdc_count = 12
SELECT COUNT(*) AS cdc_count FROM {{zone_name}}.delta_demos.inherited_cdc;

ASSERT VALUE cdc_plans = 3
SELECT COUNT(DISTINCT plan) AS cdc_plans FROM {{zone_name}}.delta_demos.inherited_cdc;

-- Verify inherited_constrained has 10 rows, 3 warehouses
ASSERT VALUE constrained_count = 10
SELECT COUNT(*) AS constrained_count FROM {{zone_name}}.delta_demos.inherited_constrained;

ASSERT VALUE constrained_warehouses = 3
SELECT COUNT(DISTINCT warehouse) AS constrained_warehouses FROM {{zone_name}}.delta_demos.inherited_constrained;

-- Verify total MRR across all enterprise customers
ASSERT VALUE enterprise_mrr = 26300.0
SELECT SUM(monthly_spend) AS enterprise_mrr FROM {{zone_name}}.delta_demos.inherited_cdc WHERE plan = 'enterprise';

-- Verify total inventory quantity
ASSERT VALUE total_qty = 4520
SELECT SUM(quantity) AS total_qty FROM {{zone_name}}.delta_demos.inherited_constrained;
