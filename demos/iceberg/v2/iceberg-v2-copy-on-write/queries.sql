-- ============================================================================
-- Iceberg V2 Copy-on-Write — Queries
-- ============================================================================
-- Validates that DeltaForge correctly reads an Iceberg V2 table using
-- copy-on-write mode. The table had 120 shipments initially; 20 were updated
-- from "In Transit" to "Delivered" (overwrite), then 10 were deleted
-- (overwrite). Copy-on-write rewrites data files — NO delete files exist.
-- The current snapshot references a single clean data file with 110 rows.
-- ============================================================================


-- ============================================================================
-- Query 1: Full Scan — Row Count
-- ============================================================================
-- 120 initial - 10 deleted = 110 rows. Copy-on-write means the current
-- data file already reflects all updates and deletes.

ASSERT ROW_COUNT = 110
ASSERT VALUE status = 'Delivered' WHERE shipment_id = 'SHP-0001'
ASSERT VALUE carrier = 'FedEx' WHERE shipment_id = 'SHP-0001'
ASSERT VALUE status = 'Processing' WHERE shipment_id = 'SHP-0060'
ASSERT VALUE status = 'Returned' WHERE shipment_id = 'SHP-0120'
SELECT * FROM {{zone_name}}.iceberg_demos.shipments;


-- ============================================================================
-- Query 2: Status Breakdown
-- ============================================================================
-- After updating 20 "In Transit" → "Delivered" and deleting 10 mixed:
-- Delivered=70, In Transit=15, Processing=14, Returned=11

ASSERT ROW_COUNT = 4
ASSERT VALUE shipment_count = 70 WHERE status = 'Delivered'
ASSERT VALUE shipment_count = 15 WHERE status = 'In Transit'
ASSERT VALUE shipment_count = 14 WHERE status = 'Processing'
ASSERT VALUE shipment_count = 11 WHERE status = 'Returned'
SELECT
    status,
    COUNT(*) AS shipment_count
FROM {{zone_name}}.iceberg_demos.shipments
GROUP BY status
ORDER BY status;


-- ============================================================================
-- Query 3: Carrier Analysis
-- ============================================================================
-- Shipment counts and average shipping cost per carrier.

ASSERT ROW_COUNT = 4
ASSERT VALUE shipment_count = 30 WHERE carrier = 'DHL'
ASSERT VALUE shipment_count = 32 WHERE carrier = 'FedEx'
ASSERT VALUE shipment_count = 24 WHERE carrier = 'UPS'
ASSERT VALUE shipment_count = 24 WHERE carrier = 'USPS'
ASSERT VALUE avg_cost = 230.84 WHERE carrier = 'DHL'
ASSERT VALUE avg_cost = 265.77 WHERE carrier = 'FedEx'
ASSERT VALUE avg_cost = 295.16 WHERE carrier = 'UPS'
ASSERT VALUE avg_cost = 259.04 WHERE carrier = 'USPS'
SELECT
    carrier,
    COUNT(*) AS shipment_count,
    ROUND(AVG(shipping_cost), 2) AS avg_cost
FROM {{zone_name}}.iceberg_demos.shipments
GROUP BY carrier
ORDER BY carrier;


-- ============================================================================
-- Query 4: Verify Deleted Shipments Absent
-- ============================================================================
-- These 10 shipments were deleted in snapshot 3. Copy-on-write means
-- they simply don't exist in the rewritten data file.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.shipments
WHERE shipment_id IN (
    'SHP-0009', 'SHP-0012', 'SHP-0015', 'SHP-0017', 'SHP-0020',
    'SHP-0022', 'SHP-0029', 'SHP-0031', 'SHP-0032', 'SHP-0036'
);


-- ============================================================================
-- Query 5: Verify Updated Shipments Have Delivery Dates
-- ============================================================================
-- 50 originally delivered + 20 newly marked delivered = 70 with actual_delivery.

ASSERT ROW_COUNT = 1
ASSERT VALUE delivered_with_date = 70
SELECT
    COUNT(*) AS delivered_with_date
FROM {{zone_name}}.iceberg_demos.shipments
WHERE actual_delivery IS NOT NULL;


-- ============================================================================
-- Query 6: Priority Breakdown
-- ============================================================================
-- Distribution across Standard, Express, Overnight priorities.

ASSERT ROW_COUNT = 3
ASSERT VALUE shipment_count = 43 WHERE priority = 'Express'
ASSERT VALUE shipment_count = 33 WHERE priority = 'Overnight'
ASSERT VALUE shipment_count = 34 WHERE priority = 'Standard'
SELECT
    priority,
    COUNT(*) AS shipment_count
FROM {{zone_name}}.iceberg_demos.shipments
GROUP BY priority
ORDER BY priority;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check. Confirms row count, carrier/status/priority
-- diversity, delivery dates, and aggregate financials.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 110
ASSERT VALUE carrier_count = 4
ASSERT VALUE status_count = 4
ASSERT VALUE priority_count = 3
ASSERT VALUE delivered_with_date = 70
ASSERT VALUE total_shipping_cost = 28730.35
ASSERT VALUE avg_weight = 67.44
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT carrier) AS carrier_count,
    COUNT(DISTINCT status) AS status_count,
    COUNT(DISTINCT priority) AS priority_count,
    SUM(CASE WHEN actual_delivery IS NOT NULL THEN 1 ELSE 0 END) AS delivered_with_date,
    ROUND(SUM(shipping_cost), 2) AS total_shipping_cost,
    ROUND(AVG(weight_kg), 2) AS avg_weight
FROM {{zone_name}}.iceberg_demos.shipments;
