-- ============================================================================
-- Partition-Scoped Maintenance — Educational Queries
-- ============================================================================
-- WHAT: Delta Lake tables partitioned by a business dimension (warehouse
--       datacenter) store each partition's data files in a separate
--       directory. OPTIMIZE can target a single partition via WHERE,
--       compacting only that partition's files and merging its deletion
--       vectors — leaving all other partitions untouched.
-- WHY:  In global systems, each region has different maintenance windows.
--       Partition-scoped OPTIMIZE lets you compact us-east-dc during its
--       low-traffic window without locking eu-central-dc or ap-south-dc.
-- HOW:  DELETE/UPDATE create deletion vector (.bin) sidecar files within
--       the affected partition directory. OPTIMIZE WHERE warehouse = 'X'
--       rewrites only that partition's data files, physically removing
--       DV-marked rows. Other partitions see zero I/O.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Per-Warehouse Revenue & Order Distribution
-- ============================================================================
-- The warehouse_orders table is partitioned by warehouse datacenter.
-- Each partition has 25 orders. Let's see the starting distribution:
-- total revenue, order count, and product mix per warehouse.

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 25 WHERE warehouse = 'us-east-dc'
ASSERT VALUE order_count = 25 WHERE warehouse = 'eu-central-dc'
ASSERT VALUE order_count = 25 WHERE warehouse = 'ap-south-dc'
ASSERT VALUE total_revenue = 8552.96 WHERE warehouse = 'us-east-dc'
ASSERT VALUE total_revenue = 8302.37 WHERE warehouse = 'eu-central-dc'
ASSERT VALUE total_revenue = 8631.80 WHERE warehouse = 'ap-south-dc'
SELECT warehouse,
       COUNT(*) AS order_count,
       COUNT(DISTINCT product) AS products,
       SUM(quantity * unit_price) AS total_revenue
FROM {{zone_name}}.delta_demos.warehouse_orders
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- STEP 1: DELETE — Remove 5 cancelled/low-value orders from us-east-dc
-- ============================================================================
-- These orders were flagged as cancelled or below the profitability
-- threshold. The DELETE writes deletion vector sidecar files ONLY in the
-- warehouse=us-east-dc/ partition directory. The eu-central-dc and
-- ap-south-dc partition directories are completely untouched.
--
-- Deleted: id 3 (food $12.50x10), 5 (toys $24.99x3), 12 (clothing
-- $29.99x3), 15 (toys $19.99x4), 18 (food $11.99x20) — $609.70 removed.

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE id IN (3, 5, 12, 15, 18);


-- ============================================================================
-- STEP 2: DELETE — Remove 5 cancelled orders from eu-central-dc
-- ============================================================================
-- A separate batch of cancellations hits the European warehouse. Again,
-- deletion vectors are written ONLY in warehouse=eu-central-dc/.
--
-- Deleted: id 28 (food $15.99x12), 33 (food $11.49x9), 40 (toys
-- $54.99x4), 42 (clothing $34.99x5), 48 (food $19.99x11) — $910.09 removed.

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE id IN (28, 33, 40, 42, 48);


-- ============================================================================
-- STEP 3: UPDATE — Expedite 3 orders in ap-south-dc to overnight priority
-- ============================================================================
-- A regional logistics change requires upgrading three standard-priority
-- orders to overnight shipping. UPDATEs in Delta create deletion vectors
-- (marking old row versions) plus new data files with the updated values,
-- all scoped to warehouse=ap-south-dc/.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.warehouse_orders
SET priority = 'overnight'
WHERE id IN (52, 58, 66);


-- ============================================================================
-- LEARN: Verify Per-Partition State — DVs Created, Counts Correct
-- ============================================================================
-- After the mutations:
--   us-east-dc:    25 - 5 deleted = 20 orders
--   eu-central-dc: 25 - 5 deleted = 20 orders
--   ap-south-dc:   25 (3 updated, none deleted) = 25 orders
--   Total: 65 orders
--
-- Each partition now has pending deletion vectors from its respective
-- operations. The data files still physically contain the deleted rows,
-- but readers apply DVs as filters so they appear gone.

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 20 WHERE warehouse = 'us-east-dc'
ASSERT VALUE order_count = 20 WHERE warehouse = 'eu-central-dc'
ASSERT VALUE order_count = 25 WHERE warehouse = 'ap-south-dc'
SELECT warehouse,
       COUNT(*) AS order_count,
       SUM(quantity * unit_price) AS total_revenue,
       COUNT(DISTINCT product) AS products
FROM {{zone_name}}.delta_demos.warehouse_orders
GROUP BY warehouse
ORDER BY warehouse;


-- Confirm the deleted orders are truly invisible to readers
ASSERT VALUE deleted_found = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS deleted_found
FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE id IN (3, 5, 12, 15, 18, 28, 33, 40, 42, 48);


-- Confirm the priority updates took effect in ap-south-dc
ASSERT ROW_COUNT = 3
ASSERT VALUE priority = 'overnight' WHERE id = 52
ASSERT VALUE priority = 'overnight' WHERE id = 58
ASSERT VALUE priority = 'overnight' WHERE id = 66
SELECT id, order_id, product, priority
FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE id IN (52, 58, 66)
ORDER BY id;


-- ============================================================================
-- STEP 4: OPTIMIZE us-east-dc ONLY — Selective partition maintenance
-- ============================================================================
-- This is the key operation. By adding WHERE warehouse = 'us-east-dc',
-- OPTIMIZE reads and rewrites ONLY the data files under the
-- warehouse=us-east-dc/ partition directory. It:
--   1. Merges small data files into optimally-sized files
--   2. Physically removes the 5 DV-marked rows from the compacted files
--   3. Deletes the now-unnecessary .bin deletion vector sidecar files
--
-- The eu-central-dc and ap-south-dc partition directories experience
-- ZERO I/O — their files, deletion vectors, and data remain untouched.

OPTIMIZE {{zone_name}}.delta_demos.warehouse_orders WHERE warehouse = 'us-east-dc';


-- ============================================================================
-- EXPLORE: Post-Selective-Optimize — All Three Partitions Intact
-- ============================================================================
-- After optimizing only us-east-dc, let's verify all three partitions
-- still have the correct data. The selective OPTIMIZE changed nothing
-- about eu-central-dc or ap-south-dc — their DVs are still pending.

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 20 WHERE warehouse = 'us-east-dc'
ASSERT VALUE total_revenue = 7943.26 WHERE warehouse = 'us-east-dc'
ASSERT VALUE order_count = 20 WHERE warehouse = 'eu-central-dc'
ASSERT VALUE total_revenue = 7392.28 WHERE warehouse = 'eu-central-dc'
ASSERT VALUE order_count = 25 WHERE warehouse = 'ap-south-dc'
ASSERT VALUE total_revenue = 8631.80 WHERE warehouse = 'ap-south-dc'
SELECT warehouse,
       COUNT(*) AS order_count,
       SUM(quantity * unit_price) AS total_revenue,
       COUNT(DISTINCT product) AS products
FROM {{zone_name}}.delta_demos.warehouse_orders
GROUP BY warehouse
ORDER BY warehouse;


-- Per-product breakdown across all warehouses — verify data integrity
ASSERT ROW_COUNT = 5
SELECT product,
       COUNT(*) AS order_count,
       SUM(quantity * unit_price) AS revenue,
       SUM(quantity) AS total_units
FROM {{zone_name}}.delta_demos.warehouse_orders
GROUP BY product
ORDER BY revenue DESC;


-- ============================================================================
-- STEP 5: OPTIMIZE remaining partitions
-- ============================================================================
-- Now we optimize eu-central-dc and ap-south-dc in their respective
-- maintenance windows. Each OPTIMIZE touches only its target partition.

OPTIMIZE {{zone_name}}.delta_demos.warehouse_orders WHERE warehouse = 'eu-central-dc';

OPTIMIZE {{zone_name}}.delta_demos.warehouse_orders WHERE warehouse = 'ap-south-dc';


-- ============================================================================
-- VERIFY: Final Comprehensive Checks
-- ============================================================================
-- All three partitions are now fully compacted with no pending DVs.
-- Let's run every verification to confirm data integrity.

-- Total count: 75 - 5 (us-east) - 5 (eu-central) = 65
ASSERT ROW_COUNT = 65
SELECT * FROM {{zone_name}}.delta_demos.warehouse_orders;

-- Per-partition counts
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.warehouse_orders WHERE warehouse = 'us-east-dc';

ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.warehouse_orders WHERE warehouse = 'eu-central-dc';

ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.warehouse_orders WHERE warehouse = 'ap-south-dc';

-- Deleted orders gone
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE id IN (3, 5, 12, 15, 18, 28, 33, 40, 42, 48);

-- Updated priorities persisted through OPTIMIZE
ASSERT VALUE priority = 'overnight'
SELECT priority FROM {{zone_name}}.delta_demos.warehouse_orders WHERE id = 52;

ASSERT VALUE priority = 'overnight'
SELECT priority FROM {{zone_name}}.delta_demos.warehouse_orders WHERE id = 58;

-- Total revenue after all mutations
ASSERT VALUE total_revenue = 23967.34
SELECT SUM(quantity * unit_price) AS total_revenue
FROM {{zone_name}}.delta_demos.warehouse_orders;

-- Overnight priority count in ap-south-dc (was 5, now 8 after 3 updates)
ASSERT VALUE overnight_count = 8
SELECT COUNT(*) AS overnight_count
FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE warehouse = 'ap-south-dc' AND priority = 'overnight';

-- Warehouse count
ASSERT VALUE cnt = 3
SELECT COUNT(DISTINCT warehouse) AS cnt FROM {{zone_name}}.delta_demos.warehouse_orders;

-- ID range integrity
ASSERT VALUE min_id = 1
ASSERT VALUE max_id = 75
SELECT MIN(id) AS min_id, MAX(id) AS max_id
FROM {{zone_name}}.delta_demos.warehouse_orders;
