-- ============================================================================
-- E-Commerce Order Tracking — Indexed UPDATE / DELETE / MERGE
-- ============================================================================
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │              WHY UPDATE / DELETE / MERGE LOVE INDEXES                │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │                                                                      │
--  │ A SQL `UPDATE table SET col = X WHERE key = Y` looks like one        │
--  │ operation, but the engine actually does TWO things:                  │
--  │                                                                      │
--  │   1. LOCATE — find the file (and row group) containing the row      │
--  │   2. REWRITE — produce a new file with the changed value            │
--  │                                                                      │
--  │ Without an index on `key`, step 1 forces a full-table scan: the      │
--  │ engine has to open every file and look inside to find the row.       │
--  │ For a 5M-row table spread across hundreds of files, that means       │
--  │ hundreds of file opens and reads — JUST TO FIND THE ROW.             │
--  │                                                                      │
--  │ With an index on `key`, step 1 becomes a targeted read. Step 2       │
--  │ still happens — you can't change a parquet file in place — but it    │
--  │ is no longer dwarfed by the locate cost.                             │
--  │                                                                      │
--  │ The same logic applies to DELETE (locate, then mark/rewrite) and     │
--  │ MERGE (locate the source rows in the target, then update/insert).    │
--  │ MERGE multiplies the savings across many source rows.                │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                  WHEN TO INDEX A MUTATION COLUMN                     │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ ✓ Workload includes UPDATE / DELETE / MERGE keyed on the column     │
--  │ ✓ The mutation predicate is selective (a single row, or a small    │
--  │   batch) — broad mutations like UPDATE ... WHERE region = 'EU'      │
--  │   touch most files anyway and don't benefit                          │
--  │ ✓ The column is high-cardinality                                    │
--  │ ✓ The column is NOT already covered by Delta's built-in stats        │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                  PAIRING WITH DELETION VECTORS                       │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ The table below uses 'delta.enableDeletionVectors' = 'true'. With    │
--  │ deletion vectors, an UPDATE/DELETE no longer rewrites the entire     │
--  │ file containing the row — instead it writes a tiny bitmap marking   │
--  │ which rows are logically removed. The index identifies the row to    │
--  │ mark; the deletion vector avoids the full-file rewrite.              │
--  │                                                                      │
--  │ Index + deletion vectors together is the standard recipe for cheap   │
--  │ single-row mutations on big Delta tables.                            │
--  └──────────────────────────────────────────────────────────────────────┘
--
-- This demo runs four mutation shapes back to back: single-row UPDATE,
-- batched IN-list UPDATE, DELETE, then MERGE upsert. Each query asserts
-- the post-mutation state.
-- ============================================================================


-- ============================================================================
-- BUILD: Create the Index
-- ============================================================================
-- Carrier events arrive keyed by tracking_number. Without an index every
-- UPDATE / DELETE / MERGE has to scan every file just to find the row
-- before rewriting it. The index makes the locate step a targeted read.

CREATE INDEX idx_tracking
    ON TABLE {{zone_name}}.delta_demos.shipment_orders (tracking_number)
    WITH (auto_update = true);


-- ============================================================================
-- EXPLORE: Initial Status Distribution
-- ============================================================================
-- 50 active orders across two batches. Most are still in_transit;
-- a few are out_for_delivery or preparing.

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 33 WHERE status = 'in_transit'
ASSERT VALUE order_count = 13 WHERE status = 'preparing'
ASSERT VALUE order_count = 4  WHERE status = 'out_for_delivery'
SELECT status,
       COUNT(*)                AS order_count,
       COUNT(DISTINCT carrier) AS carrier_count
FROM {{zone_name}}.delta_demos.shipment_orders
GROUP BY status
ORDER BY status;


-- ============================================================================
-- LEARN: Single-Row UPDATE on Indexed Column — The Headline ACID Case
-- ============================================================================
-- Carrier event: tracking 1Z9X7K0008H has been delivered. Without the
-- index this UPDATE has to scan every file to find row 0008H. With the
-- index it routes straight to the slice carrying that key.

UPDATE {{zone_name}}.delta_demos.shipment_orders
   SET status = 'delivered'
 WHERE tracking_number = '1Z9X7K0008H';

ASSERT ROW_COUNT = 1
ASSERT VALUE status = 'delivered'
ASSERT VALUE order_id = 5008
ASSERT VALUE destination = 'Lisbon'
SELECT order_id, tracking_number, status, destination
FROM {{zone_name}}.delta_demos.shipment_orders
WHERE tracking_number = '1Z9X7K0008H';


-- ============================================================================
-- LEARN: Batched UPDATE — IN List Through the Index
-- ============================================================================
-- Four shipments hit `out_for_delivery` in the same routing event.
-- The index resolves the IN list to exactly the slices containing
-- those keys.

UPDATE {{zone_name}}.delta_demos.shipment_orders
   SET status = 'out_for_delivery'
 WHERE tracking_number IN ('1Z9X7K0009J', '1Z9X7K0017S', '1Z9X7K0040R', '1Z9X7K0048Z');

ASSERT ROW_COUNT = 8
ASSERT VALUE order_count = 8
SELECT COUNT(*) AS order_count
FROM {{zone_name}}.delta_demos.shipment_orders
WHERE status = 'out_for_delivery';


-- ============================================================================
-- LEARN: DELETE on Indexed Column — Cancelled Shipment
-- ============================================================================
-- Shipment 1Z9X7K0029E is cancelled. The index identifies the row to
-- mark; with deletion vectors enabled, the row is logically removed
-- without rewriting the entire file that contained it.

DELETE FROM {{zone_name}}.delta_demos.shipment_orders
 WHERE tracking_number = '1Z9X7K0029E';

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 49
ASSERT VALUE preparing_count = 12
SELECT COUNT(*)                                      AS total_rows,
       COUNT(*) FILTER (WHERE status = 'preparing')  AS preparing_count
FROM {{zone_name}}.delta_demos.shipment_orders;


-- ============================================================================
-- LEARN: MERGE Upsert — Carrier Event Stream
-- ============================================================================
-- A carrier event batch arrives: two existing shipments advance status,
-- one is a brand-new pickup. The MERGE matches on tracking_number;
-- the index makes both the lookup and the routing of the source rows
-- inexpensive. The new row gets indexed automatically because
-- auto_update is on.

MERGE INTO {{zone_name}}.delta_demos.shipment_orders AS t
USING (
    SELECT '1Z9X7K0011L' AS tracking_number, 'out_for_delivery' AS new_status, NULL AS new_order
    UNION ALL
    SELECT '1Z9X7K0023Y',                    'delivered',                       NULL
    UNION ALL
    SELECT '1Z9X7K0051C',                    'preparing',                       1
) AS s
ON t.tracking_number = s.tracking_number
WHEN MATCHED THEN
    UPDATE SET status = s.new_status
WHEN NOT MATCHED THEN
    INSERT (order_id, tracking_number, customer_id, carrier, status, weight_kg, destination, placed_at, eta_date)
    VALUES (5051, '1Z9X7K0051C', 4502, 'NorthStar', 'preparing', 1.65, 'Porto', '2026-04-03', '2026-04-10');

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 2  WHERE status = 'delivered'
ASSERT VALUE order_count = 9  WHERE status = 'out_for_delivery'
ASSERT VALUE order_count = 13 WHERE status = 'preparing'
ASSERT VALUE order_count = 26 WHERE status = 'in_transit'
SELECT status, COUNT(*) AS order_count
FROM {{zone_name}}.delta_demos.shipment_orders
GROUP BY status
ORDER BY status;


-- ============================================================================
-- LEARN: Post-Mutation Lookup
-- ============================================================================
-- The new shipment appears in the lookup result the instant the MERGE
-- commits, because auto_update kept the index in sync.

ASSERT ROW_COUNT = 1
ASSERT VALUE order_id = 5051
ASSERT VALUE status = 'preparing'
ASSERT VALUE carrier = 'NorthStar'
SELECT order_id, tracking_number, carrier, status, destination
FROM {{zone_name}}.delta_demos.shipment_orders
WHERE tracking_number = '1Z9X7K0051C';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- After all mutations: 50 rows total (49 from initial - 1 deleted + 1
-- inserted via MERGE), distributed across four statuses.

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 26 WHERE status = 'in_transit'
ASSERT VALUE order_count = 13 WHERE status = 'preparing'
ASSERT VALUE order_count = 9  WHERE status = 'out_for_delivery'
ASSERT VALUE order_count = 2  WHERE status = 'delivered'
SELECT status,
       COUNT(*)                       AS order_count,
       ROUND(SUM(weight_kg), 2)       AS total_weight
FROM {{zone_name}}.delta_demos.shipment_orders
GROUP BY status
ORDER BY status;
