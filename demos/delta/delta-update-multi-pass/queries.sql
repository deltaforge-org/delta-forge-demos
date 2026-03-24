-- ============================================================================
-- Delta UPDATE Multi-Pass — ETL Pipeline Stages — Educational Queries
-- ============================================================================
-- WHAT: Shows how sequential UPDATE passes transform raw data through an ETL
--       pipeline, with each pass creating a new Delta version.
-- WHY:  Real ETL pipelines process data in stages — normalize, classify,
--       enrich. Delta's versioning makes each stage a checkpoint you can
--       inspect, compare, and roll back to.
-- HOW:  Three UPDATE passes on 30 e-commerce orders: (1) UPPER(TRIM()) for
--       normalization, (2) CASE WHEN for classification, (3) arithmetic for
--       enrichment. VERSION AS OF reveals the state at each stage.
-- ============================================================================


-- ============================================================================
-- BASELINE: Final Enriched State — All Pipeline Stages Complete
-- ============================================================================
-- After all three passes, every order has clean status codes, assigned
-- priority/shipping tiers, and computed financial fields.

ASSERT ROW_COUNT = 10
ASSERT VALUE status = 'PENDING' WHERE id = 1
ASSERT VALUE priority = 'HIGH' WHERE id = 2
ASSERT VALUE total_with_tax = 1340.63 WHERE id = 9
SELECT id, customer_name, status, subtotal, total_with_tax,
       priority, shipping_method, estimated_profit, region
FROM {{zone_name}}.pipeline_demos.order_pipeline
ORDER BY id
LIMIT 10;


-- ============================================================================
-- LEARN: VERSION AS OF 1 — Raw Data Before Any Transforms
-- ============================================================================
-- The original INSERT state. Notice how status values are messy: mixed case,
-- leading spaces (' confirmed'), trailing spaces ('shipped '). Priority and
-- shipping_method are empty strings. total_with_tax is 0.00.

ASSERT ROW_COUNT = 10
ASSERT VALUE status = 'pending' WHERE id = 1
ASSERT VALUE status = 'PENDING' WHERE id = 2
ASSERT VALUE status = ' confirmed' WHERE id = 3
ASSERT VALUE status = 'shipped ' WHERE id = 4
ASSERT VALUE status = 'Delivered' WHERE id = 5
SELECT id, customer_name, status, subtotal, priority, shipping_method,
       total_with_tax, estimated_profit
FROM {{zone_name}}.pipeline_demos.order_pipeline VERSION AS OF 1
ORDER BY id
LIMIT 10;


-- ============================================================================
-- LEARN: VERSION AS OF 2 — After Normalize Pass
-- ============================================================================
-- UPPER(TRIM(status)) has cleaned all 30 rows. The 5 messy variants collapsed
-- into 4 clean values: PENDING, CONFIRMED, SHIPPED, DELIVERED. But priority
-- and financial fields are still empty/zero — those come in later passes.

ASSERT ROW_COUNT = 4
ASSERT VALUE cnt = 12 WHERE status = 'PENDING'
ASSERT VALUE cnt = 6 WHERE status = 'CONFIRMED'
ASSERT VALUE cnt = 6 WHERE status = 'SHIPPED'
ASSERT VALUE cnt = 6 WHERE status = 'DELIVERED'
SELECT status, COUNT(*) AS cnt
FROM {{zone_name}}.pipeline_demos.order_pipeline VERSION AS OF 2
GROUP BY status
ORDER BY cnt DESC;


-- ============================================================================
-- LEARN: VERSION AS OF 3 — After Classify Pass
-- ============================================================================
-- CASE WHEN has assigned priority and shipping_method based on subtotal
-- thresholds. But total_with_tax is still 0.00 — the Enrich pass hasn't
-- run yet. This is the power of multi-pass: each stage does one job.

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 9 WHERE priority = 'HIGH'
ASSERT VALUE cnt = 14 WHERE priority = 'MEDIUM'
ASSERT VALUE cnt = 7 WHERE priority = 'LOW'
SELECT priority, COUNT(*) AS cnt
FROM {{zone_name}}.pipeline_demos.order_pipeline VERSION AS OF 3
GROUP BY priority
ORDER BY cnt DESC;


-- ============================================================================
-- OBSERVE: Final Priority Distribution with Revenue
-- ============================================================================
-- How order value distributes across priority tiers after the full pipeline.
-- HIGH-priority orders (subtotal > 500) generate the most revenue despite
-- being fewer in count.

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 9 WHERE priority = 'HIGH'
ASSERT VALUE order_count = 14 WHERE priority = 'MEDIUM'
ASSERT VALUE order_count = 7 WHERE priority = 'LOW'
ASSERT VALUE total_revenue = 10244.69 WHERE priority = 'HIGH'
ASSERT VALUE total_revenue = 3643.28 WHERE priority = 'MEDIUM'
ASSERT VALUE total_revenue = 560.55 WHERE priority = 'LOW'
SELECT priority,
       COUNT(*) AS order_count,
       SUM(total_with_tax) AS total_revenue,
       SUM(estimated_profit) AS total_profit
FROM {{zone_name}}.pipeline_demos.order_pipeline
GROUP BY priority
ORDER BY total_revenue DESC;


-- ============================================================================
-- OBSERVE: Revenue by Region with Tax
-- ============================================================================
-- Regional breakdown showing how total_with_tax (computed in the Enrich pass)
-- distributes across the four regions.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 5259.22 WHERE region = 'WEST'
ASSERT VALUE total_revenue = 2949.37 WHERE region = 'EAST'
ASSERT VALUE total_revenue = 4148.71 WHERE region = 'SOUTH'
ASSERT VALUE total_revenue = 2091.22 WHERE region = 'CENTRAL'
SELECT region,
       COUNT(*) AS order_count,
       SUM(total_with_tax) AS total_revenue,
       SUM(estimated_profit) AS total_profit
FROM {{zone_name}}.pipeline_demos.order_pipeline
GROUP BY region
ORDER BY total_revenue DESC;


-- ============================================================================
-- EXPLORE: Compare Raw vs Final for Specific Orders
-- ============================================================================
-- Side-by-side comparison of three orders at Version 1 (raw) vs the current
-- final state. Shows the full transformation chain: messy status -> clean,
-- empty priority -> assigned, zero financials -> computed.

ASSERT ROW_COUNT = 3
ASSERT VALUE final_status = 'PENDING' WHERE id = 1
ASSERT VALUE final_priority = 'HIGH' WHERE id = 2
ASSERT VALUE final_total_with_tax = 1340.63 WHERE id = 9
SELECT curr.id,
       raw.status AS raw_status,
       curr.status AS final_status,
       raw.priority AS raw_priority,
       curr.priority AS final_priority,
       raw.total_with_tax AS raw_total_with_tax,
       curr.total_with_tax AS final_total_with_tax
FROM {{zone_name}}.pipeline_demos.order_pipeline AS curr
JOIN {{zone_name}}.pipeline_demos.order_pipeline VERSION AS OF 1 AS raw
    ON curr.id = raw.id
WHERE curr.id IN (1, 2, 9)
ORDER BY curr.id;


-- ============================================================================
-- LEARN: DESCRIBE HISTORY — Pipeline Stages as Delta Versions
-- ============================================================================
-- The transaction log records each UPDATE as a separate version:
--   V0: CREATE TABLE
--   V1: INSERT (30 raw orders)
--   V2: Normalize pass (UPPER + TRIM)
--   V3: Classify pass (CASE WHEN)
--   V4: Enrich pass (arithmetic)

-- Non-deterministic: commit timestamps set at write time
ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.pipeline_demos.order_pipeline;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total rows: 30 orders (no inserts or deletes during pipeline)
ASSERT VALUE cnt = 30
SELECT COUNT(*) AS cnt FROM {{zone_name}}.pipeline_demos.order_pipeline;

-- Verify distinct statuses after normalization: exactly 4 clean values
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT status) AS cnt FROM {{zone_name}}.pipeline_demos.order_pipeline;

-- Verify total revenue (sum of total_with_tax) after enrichment
ASSERT VALUE total = 14448.52
SELECT SUM(total_with_tax) AS total FROM {{zone_name}}.pipeline_demos.order_pipeline;

-- Verify total estimated profit
ASSERT VALUE total = 2002.77
SELECT SUM(estimated_profit) AS total FROM {{zone_name}}.pipeline_demos.order_pipeline;

-- Verify all rows processed (processed_at is set)
ASSERT VALUE cnt = 30
SELECT COUNT(*) AS cnt FROM {{zone_name}}.pipeline_demos.order_pipeline WHERE processed_at = '2024-06-15 14:30:00';

-- Verify HIGH priority count
ASSERT VALUE cnt = 9
SELECT COUNT(*) AS cnt FROM {{zone_name}}.pipeline_demos.order_pipeline WHERE priority = 'HIGH';

-- Verify MEDIUM priority count
ASSERT VALUE cnt = 14
SELECT COUNT(*) AS cnt FROM {{zone_name}}.pipeline_demos.order_pipeline WHERE priority = 'MEDIUM';

-- Verify LOW priority count
ASSERT VALUE cnt = 7
SELECT COUNT(*) AS cnt FROM {{zone_name}}.pipeline_demos.order_pipeline WHERE priority = 'LOW';

-- Verify EXPRESS and HIGH are the same set (subtotal > 500)
ASSERT VALUE cnt = 9
SELECT COUNT(*) AS cnt FROM {{zone_name}}.pipeline_demos.order_pipeline WHERE shipping_method = 'EXPRESS';

-- Verify total subtotal unchanged through pipeline
ASSERT VALUE total = 13351.56
SELECT SUM(subtotal) AS total FROM {{zone_name}}.pipeline_demos.order_pipeline;

-- Verify 4 distinct regions
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT region) AS cnt FROM {{zone_name}}.pipeline_demos.order_pipeline;
