-- ============================================================================
-- Delta VACUUM & CDC Interaction -- Educational Queries
-- ============================================================================
-- WHAT: Change Data Feed (CDF) captures row-level changes as separate files;
--       VACUUM cleans up orphaned data files without destroying CDF records.
-- WHY:  Downstream pipelines need to replay incremental changes (CDC), but
--       storage costs grow as old data files accumulate after UPDATEs/DELETEs.
--       VACUUM reclaims space while CDF files remain within retention.
-- HOW:  CDF writes _change_data files in a separate directory. VACUUM only
--       removes superseded data files from the main data directory. The
--       retention period (default 7 days) protects both data and CDF files.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline state — all 40 orders are 'pending'
-- ============================================================================
-- After setup, every order starts as 'pending'. This is version 0 of the
-- Delta table. CDF recorded each INSERT with _change_type = 'insert'.

ASSERT VALUE status = 'pending'
ASSERT VALUE order_count = 40
ASSERT ROW_COUNT = 1
ASSERT VALUE total_amount = 9020.23
-- Non-deterministic: floating-point division in AVG may vary across platforms; ROUND mitigates but does not eliminate variance
ASSERT WARNING VALUE avg_amount BETWEEN 225.49 AND 225.53
SELECT status, COUNT(*) AS order_count,
       ROUND(SUM(amount), 2) AS total_amount,
       ROUND(AVG(amount), 2) AS avg_amount
FROM {{zone_name}}.delta_demos.order_lifecycle
GROUP BY status
ORDER BY status;


-- ============================================================================
-- V1: UPDATE — 15 orders move from 'pending' to 'processing'
-- ============================================================================
-- Orders 1-15 picked by warehouse for fulfillment.
-- CDF records both the pre-image (status='pending') and post-image
-- (status='processing') for each updated row.

ASSERT ROW_COUNT = 15
UPDATE {{zone_name}}.delta_demos.order_lifecycle
SET status = 'processing',
    updated_by = 'warehouse_bot',
    updated_at = '2025-01-22 08:00:00'
WHERE id BETWEEN 1 AND 15;


-- ============================================================================
-- EXPLORE: State after processing transition
-- ============================================================================
-- 15 orders moved to 'processing', 25 remain 'pending'.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 2
ASSERT VALUE order_count = 25 WHERE status = 'pending'
ASSERT VALUE order_count = 15 WHERE status = 'processing'
SELECT status, COUNT(*) AS order_count
FROM {{zone_name}}.delta_demos.order_lifecycle
GROUP BY status
ORDER BY CASE status
    WHEN 'pending' THEN 1
    WHEN 'processing' THEN 2
END;


-- ============================================================================
-- V2: UPDATE — 10 orders move from 'processing' to 'shipped'
-- ============================================================================
-- Orders 1-10 shipped out. 5 of the original 15 remain as 'processing'.
-- Each UPDATE creates new Parquet files (copy-on-write) and CDF change
-- records capturing the before/after state.

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.order_lifecycle
SET status = 'shipped',
    updated_by = 'shipping_agent',
    updated_at = '2025-01-23 14:30:00'
WHERE id BETWEEN 1 AND 10;


-- ============================================================================
-- V3: UPDATE — 5 orders move from 'shipped' to 'delivered'
-- ============================================================================
-- Orders 1-5 confirmed delivered. 5 of the shipped remain as 'shipped'.

ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.delta_demos.order_lifecycle
SET status = 'delivered',
    updated_by = 'delivery_confirm',
    updated_at = '2025-01-25 16:00:00'
WHERE id BETWEEN 1 AND 5;


-- ============================================================================
-- EXPLORE: The order lifecycle progression
-- ============================================================================
-- Each order's updated_by field shows WHO made the state transition, and
-- updated_at shows WHEN. This audit trail survives in the current data.
-- Behind the scenes, CDF also recorded the BEFORE and AFTER values for
-- each transition — enabling downstream replay.

ASSERT ROW_COUNT = 15
ASSERT VALUE status = 'delivered' WHERE id = 1
ASSERT VALUE status = 'processing' WHERE id = 15
ASSERT VALUE updated_by = 'delivery_confirm' WHERE id = 1
ASSERT VALUE updated_by = 'warehouse_bot' WHERE id = 15
SELECT id, order_id, customer, status, updated_by, updated_at
FROM {{zone_name}}.delta_demos.order_lifecycle
WHERE id <= 15
ORDER BY id;


-- ============================================================================
-- V4: DELETE — 5 cancelled orders removed
-- ============================================================================
-- Orders 36-40 cancelled by customers and purged from active table.
-- CDF records these with _change_type = 'delete', capturing the full
-- pre-deletion state so downstream consumers know what was removed.

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.order_lifecycle
WHERE id BETWEEN 36 AND 40;


-- ============================================================================
-- EXPLORE: Current state after all transitions and deletions
-- ============================================================================
-- Orders have progressed through multiple states: pending -> processing ->
-- shipped -> delivered. 5 orders were deleted (cancelled). Each state
-- transition was an UPDATE that created new Parquet files (copy-on-write)
-- and CDF change records.

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 20 WHERE status = 'pending'
ASSERT VALUE order_count = 5 WHERE status = 'processing'
ASSERT VALUE order_count = 5 WHERE status = 'shipped'
ASSERT VALUE order_count = 5 WHERE status = 'delivered'
ASSERT VALUE total_amount = 4600.35 WHERE status = 'pending'
ASSERT VALUE total_amount = 1291.90 WHERE status = 'shipped'
ASSERT VALUE total_amount = 961.69 WHERE status = 'delivered'
SELECT status, COUNT(*) AS order_count,
       ROUND(SUM(amount), 2) AS total_amount,
       ROUND(AVG(amount), 2) AS avg_amount
FROM {{zone_name}}.delta_demos.order_lifecycle
GROUP BY status
ORDER BY CASE status
    WHEN 'pending' THEN 1
    WHEN 'processing' THEN 2
    WHEN 'shipped' THEN 3
    WHEN 'delivered' THEN 4
END;


-- ============================================================================
-- LEARN: How CDF-enabled tables differ from regular Delta tables
-- ============================================================================
-- This table was created with: TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
-- When CDF is enabled, every INSERT/UPDATE/DELETE writes additional files in
-- a _change_data directory alongside the normal data files. These files contain:
--   _change_type: 'insert', 'update_preimage', 'update_postimage', or 'delete'
--   _commit_version: the Delta version number of the change
--   _commit_timestamp: when the change occurred
-- This metadata lets consumers ask: "What changed between version X and version Y?"

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 5
SELECT id, order_id, status, updated_by, amount
FROM {{zone_name}}.delta_demos.order_lifecycle
WHERE status = 'delivered'
ORDER BY id;


-- ============================================================================
-- V5: VACUUM — clean up orphaned data files
-- ============================================================================
-- After 5 versions of changes (INSERT, 3 UPDATEs, DELETE), many old Parquet
-- files have been superseded. VACUUM removes these old files to reclaim
-- storage. But CDF _change_data files are in a SEPARATE directory tree and
-- are not touched by VACUUM — they remain available for downstream consumers
-- until they exceed the retention period.

VACUUM {{zone_name}}.delta_demos.order_lifecycle;


-- ============================================================================
-- LEARN: Why VACUUM is safe with CDF
-- ============================================================================
-- After VACUUM, the data is fully intact. VACUUM only removed superseded
-- data files from the main data directory. The CDF _change_data files
-- remain untouched, so downstream pipelines can still replay incremental
-- changes for any version within the retention window.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 10
ASSERT VALUE amount = 149.99 WHERE id = 1
ASSERT VALUE status = 'delivered' WHERE id = 1
ASSERT VALUE amount = 345.60 WHERE id = 10
ASSERT VALUE status = 'shipped' WHERE id = 10
SELECT id, order_id, customer, product, amount, status
FROM {{zone_name}}.delta_demos.order_lifecycle
ORDER BY id
LIMIT 10;


-- ============================================================================
-- LEARN: Deleted orders and VACUUM interaction
-- ============================================================================
-- Orders 36-40 were deleted (cancelled). The DELETE created CDF records with
-- _change_type = 'delete', capturing the pre-deletion state. VACUUM removed
-- the old data files that contained these rows, but the CDF delete records
-- persist for consumers to learn that these orders were cancelled.

ASSERT VALUE remaining_orders = 35
ASSERT ROW_COUNT = 1
ASSERT VALUE total_value = 8007.48
SELECT COUNT(*) AS remaining_orders,
       ROUND(SUM(amount), 2) AS total_value
FROM {{zone_name}}.delta_demos.order_lifecycle;


-- ============================================================================
-- LEARN: Data integrity verification post-VACUUM
-- ============================================================================
-- The most critical property: VACUUM must NEVER corrupt current data.
-- After VACUUM, every column value for every remaining row must be identical
-- to what it was before VACUUM ran. This is because VACUUM only removes
-- files that are no longer referenced by the latest Delta version.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 5
ASSERT VALUE amount = 149.99 WHERE id = 1
ASSERT VALUE status = 'delivered' WHERE id = 1
ASSERT VALUE amount = 345.60 WHERE id = 10
ASSERT VALUE status = 'shipped' WHERE id = 10
ASSERT VALUE amount = 215.50 WHERE id = 35
ASSERT VALUE status = 'pending' WHERE id = 35
SELECT id, order_id, amount, status
FROM {{zone_name}}.delta_demos.order_lifecycle
WHERE id IN (1, 10, 20, 30, 35)
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 35 (40 - 5 cancelled)
ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.delta_demos.order_lifecycle;

-- Verify 20 orders still pending
ASSERT VALUE pending_count = 20
SELECT COUNT(*) AS pending_count FROM {{zone_name}}.delta_demos.order_lifecycle WHERE status = 'pending';

-- Verify 5 orders in processing state
ASSERT VALUE processing_count = 5
SELECT COUNT(*) AS processing_count FROM {{zone_name}}.delta_demos.order_lifecycle WHERE status = 'processing';

-- Verify 5 orders shipped
ASSERT VALUE shipped_count = 5
SELECT COUNT(*) AS shipped_count FROM {{zone_name}}.delta_demos.order_lifecycle WHERE status = 'shipped';

-- Verify 5 orders delivered
ASSERT VALUE delivered_count = 5
SELECT COUNT(*) AS delivered_count FROM {{zone_name}}.delta_demos.order_lifecycle WHERE status = 'delivered';

-- Verify cancelled orders (ids 36-40) are gone
ASSERT VALUE cancelled_count = 0
SELECT COUNT(*) AS cancelled_count FROM {{zone_name}}.delta_demos.order_lifecycle WHERE id BETWEEN 36 AND 40;

-- Verify total amount is 8007.48
ASSERT VALUE total_amount = 8007.48
SELECT ROUND(SUM(amount), 2) AS total_amount FROM {{zone_name}}.delta_demos.order_lifecycle;

-- Verify delivered order (id=1) amount is intact
ASSERT VALUE amount = 149.99
SELECT amount FROM {{zone_name}}.delta_demos.order_lifecycle WHERE id = 1;
