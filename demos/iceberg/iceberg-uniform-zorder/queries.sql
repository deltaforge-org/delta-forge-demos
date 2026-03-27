-- ============================================================================
-- Iceberg UniForm Z-ORDER Spatial Optimization — Queries
-- ============================================================================
-- HOW UNIFORM WORKS
-- -----------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically by the post-commit hook.
-- Each DML operation creates both a new Delta version and a new Iceberg
-- snapshot.
--
-- WHAT THIS DEMO SHOWS
-- --------------------
-- 1. Baseline queries on 36 seed deliveries
-- 2. Two additional INSERT batches to create file fragmentation
-- 3. OPTIMIZE ZORDER BY (latitude, longitude) for spatial locality
-- 4. Post-ZORDER verification: data integrity preserved
-- 5. Spatial bounding-box query to demonstrate co-location benefit
-- 6. Cross-format verification via Iceberg read-back
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline State (36 seed deliveries)
-- ============================================================================

ASSERT ROW_COUNT = 36
SELECT * FROM {{zone_name}}.iceberg_demos.delivery_tracking ORDER BY delivery_id;


-- ============================================================================
-- Query 1: Per-City Delivery Counts — Baseline
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE delivery_count = 6 WHERE city = 'Chicago'
ASSERT VALUE delivery_count = 6 WHERE city = 'Houston'
ASSERT VALUE delivery_count = 6 WHERE city = 'Los Angeles'
ASSERT VALUE delivery_count = 6 WHERE city = 'New York'
ASSERT VALUE delivery_count = 6 WHERE city = 'Philadelphia'
ASSERT VALUE delivery_count = 6 WHERE city = 'Phoenix'
SELECT
    city,
    COUNT(*) AS delivery_count
FROM {{zone_name}}.iceberg_demos.delivery_tracking
GROUP BY city
ORDER BY city;


-- ============================================================================
-- Query 2: Per-City Total Fees — Baseline
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE total_fee = 56.48 WHERE city = 'Chicago'
ASSERT VALUE total_fee = 64.48 WHERE city = 'Houston'
ASSERT VALUE total_fee = 68.47 WHERE city = 'Los Angeles'
ASSERT VALUE total_fee = 55.47 WHERE city = 'New York'
ASSERT VALUE total_fee = 57.98 WHERE city = 'Philadelphia'
ASSERT VALUE total_fee = 61.47 WHERE city = 'Phoenix'
SELECT
    city,
    ROUND(SUM(delivery_fee), 2) AS total_fee
FROM {{zone_name}}.iceberg_demos.delivery_tracking
GROUP BY city
ORDER BY city;


-- ============================================================================
-- LEARN: INSERT Batch 2 — Create File Fragmentation (18 more deliveries)
-- ============================================================================
-- Each INSERT creates a new data file. Multiple small files scatter
-- geographically nearby records across different files — exactly the
-- problem Z-ORDER solves.

INSERT INTO {{zone_name}}.iceberg_demos.delivery_tracking VALUES
    (37, 'DRV-107', 40.7614,  -73.9776,  'delivered',  1.8,  7.50,  '2025-02-06', 'New York'),
    (38, 'DRV-108', 40.7061,  -74.0131,  'in_transit', 3.5,  10.99, '2025-02-06', 'New York'),
    (39, 'DRV-109', 40.7831,  -73.9712,  'delivered',  2.0,  8.50,  '2025-02-06', 'New York'),
    (40, 'DRV-207', 34.0983,  -118.3267, 'delivered',  4.0,  11.99, '2025-02-06', 'Los Angeles'),
    (41, 'DRV-208', 33.9850,  -118.4695, 'pending',    2.5,  8.99,  '2025-02-06', 'Los Angeles'),
    (42, 'DRV-209', 34.0736,  -118.4004, 'delivered',  3.2,  10.50, '2025-02-06', 'Los Angeles'),
    (43, 'DRV-307', 41.8953,  -87.6387,  'delivered',  1.5,  6.50,  '2025-02-07', 'Chicago'),
    (44, 'DRV-308', 41.7943,  -87.5867,  'delivered',  4.5,  12.99, '2025-02-07', 'Chicago'),
    (45, 'DRV-309', 41.9103,  -87.6770,  'in_transit', 2.8,  9.50,  '2025-02-07', 'Chicago'),
    (46, 'DRV-407', 29.7540,  -95.3600,  'delivered',  3.0,  9.99,  '2025-02-07', 'Houston'),
    (47, 'DRV-408', 29.6830,  -95.2950,  'delivered',  5.2,  14.50, '2025-02-07', 'Houston'),
    (48, 'DRV-409', 29.8020,  -95.4100,  'pending',    1.0,  5.99,  '2025-02-07', 'Houston'),
    (49, 'DRV-507', 33.4800,  -112.0700, 'delivered',  2.0,  7.99,  '2025-02-08', 'Phoenix'),
    (50, 'DRV-508', 33.3520,  -111.7890, 'delivered',  6.0,  15.50, '2025-02-08', 'Phoenix'),
    (51, 'DRV-509', 33.5300,  -112.1100, 'in_transit', 3.5,  10.50, '2025-02-08', 'Phoenix'),
    (52, 'DRV-607', 39.9530,  -75.1680,  'delivered',  2.5,  8.50,  '2025-02-08', 'Philadelphia'),
    (53, 'DRV-608', 40.0020,  -75.1180,  'delivered',  3.8,  11.99, '2025-02-08', 'Philadelphia'),
    (54, 'DRV-609', 39.9340,  -75.1920,  'pending',    1.5,  6.50,  '2025-02-08', 'Philadelphia');


-- ============================================================================
-- LEARN: INSERT Batch 3 — More Fragmentation (18 more deliveries)
-- ============================================================================
-- A third data file is created. Geographically nearby deliveries are now
-- scattered across 3+ separate Parquet files.

INSERT INTO {{zone_name}}.iceberg_demos.delivery_tracking VALUES
    (55, 'DRV-110', 40.7350,  -74.0000,  'delivered',  2.2,  8.99,  '2025-02-09', 'New York'),
    (56, 'DRV-111', 40.6950,  -73.9840,  'delivered',  3.8,  11.50, '2025-02-09', 'New York'),
    (57, 'DRV-112', 40.7700,  -73.9500,  'in_transit', 1.5,  6.99,  '2025-02-09', 'New York'),
    (58, 'DRV-210', 34.0400,  -118.2500, 'delivered',  4.5,  12.99, '2025-02-09', 'Los Angeles'),
    (59, 'DRV-211', 34.1000,  -118.3400, 'delivered',  2.8,  9.50,  '2025-02-09', 'Los Angeles'),
    (60, 'DRV-212', 33.9600,  -118.4200, 'pending',    5.0,  13.99, '2025-02-09', 'Los Angeles'),
    (61, 'DRV-310', 41.8700,  -87.6500,  'delivered',  1.8,  7.50,  '2025-02-10', 'Chicago'),
    (62, 'DRV-311', 41.9400,  -87.7200,  'delivered',  3.5,  10.50, '2025-02-10', 'Chicago'),
    (63, 'DRV-312', 41.8100,  -87.6000,  'in_transit', 4.2,  12.50, '2025-02-10', 'Chicago'),
    (64, 'DRV-410', 29.7700,  -95.3500,  'delivered',  2.5,  8.50,  '2025-02-10', 'Houston'),
    (65, 'DRV-411', 29.7200,  -95.3800,  'delivered',  3.0,  9.99,  '2025-02-10', 'Houston'),
    (66, 'DRV-412', 29.6600,  -95.2600,  'pending',    4.8,  13.50, '2025-02-10', 'Houston'),
    (67, 'DRV-510', 33.4600,  -112.0500, 'delivered',  1.5,  6.99,  '2025-02-11', 'Phoenix'),
    (68, 'DRV-511', 33.3800,  -111.9200, 'delivered',  4.0,  11.99, '2025-02-11', 'Phoenix'),
    (69, 'DRV-512', 33.5500,  -112.1300, 'in_transit', 2.8,  9.50,  '2025-02-11', 'Phoenix'),
    (70, 'DRV-610', 39.9700,  -75.1500,  'delivered',  2.0,  7.99,  '2025-02-11', 'Philadelphia'),
    (71, 'DRV-611', 39.9100,  -75.2000,  'delivered',  5.2,  14.50, '2025-02-11', 'Philadelphia'),
    (72, 'DRV-612', 39.9900,  -75.1400,  'pending',    1.0,  5.50,  '2025-02-11', 'Philadelphia');


-- ============================================================================
-- Query 3: Pre-ZORDER Row Count — All 72 Deliveries Present
-- ============================================================================

ASSERT ROW_COUNT = 72
SELECT * FROM {{zone_name}}.iceberg_demos.delivery_tracking ORDER BY delivery_id;


-- ============================================================================
-- Query 4: Pre-ZORDER Per-City Counts — 12 Per City
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE delivery_count = 12 WHERE city = 'Chicago'
ASSERT VALUE delivery_count = 12 WHERE city = 'Houston'
ASSERT VALUE delivery_count = 12 WHERE city = 'Los Angeles'
ASSERT VALUE delivery_count = 12 WHERE city = 'New York'
ASSERT VALUE delivery_count = 12 WHERE city = 'Philadelphia'
ASSERT VALUE delivery_count = 12 WHERE city = 'Phoenix'
SELECT
    city,
    COUNT(*) AS delivery_count
FROM {{zone_name}}.iceberg_demos.delivery_tracking
GROUP BY city
ORDER BY city;


-- ============================================================================
-- Query 5: Pre-ZORDER Aggregates — Snapshot Before Optimization
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_deliveries = 72
ASSERT VALUE total_fees = 726.19
ASSERT VALUE total_weight = 233.2
SELECT
    COUNT(*) AS total_deliveries,
    ROUND(SUM(delivery_fee), 2) AS total_fees,
    ROUND(SUM(package_weight), 1) AS total_weight
FROM {{zone_name}}.iceberg_demos.delivery_tracking;


-- ============================================================================
-- LEARN: OPTIMIZE ZORDER — Spatial Locality Optimization
-- ============================================================================
-- Z-ORDER interleaves the bits of latitude and longitude values to create a
-- space-filling curve. Records that are geographically close are co-located
-- in the same data files. This dramatically improves spatial range queries
-- by reducing the number of files that need to be scanned.

OPTIMIZE {{zone_name}}.iceberg_demos.delivery_tracking ZORDER BY (latitude, longitude);


-- ============================================================================
-- Query 6: Post-ZORDER Row Count — Data Integrity Preserved
-- ============================================================================
-- After Z-ORDER optimization, all 72 rows must still be present.

ASSERT ROW_COUNT = 72
SELECT * FROM {{zone_name}}.iceberg_demos.delivery_tracking ORDER BY delivery_id;


-- ============================================================================
-- Query 7: Post-ZORDER Aggregates — Values Unchanged
-- ============================================================================
-- Z-ORDER must not alter any data values. Same totals as pre-ZORDER.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_deliveries = 72
ASSERT VALUE total_fees = 726.19
ASSERT VALUE total_weight = 233.2
SELECT
    COUNT(*) AS total_deliveries,
    ROUND(SUM(delivery_fee), 2) AS total_fees,
    ROUND(SUM(package_weight), 1) AS total_weight
FROM {{zone_name}}.iceberg_demos.delivery_tracking;


-- ============================================================================
-- Query 8: Post-ZORDER Per-City Fees — Still Correct
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE total_fee = 115.97 WHERE city = 'Chicago'
ASSERT VALUE total_fee = 126.95 WHERE city = 'Houston'
ASSERT VALUE total_fee = 136.43 WHERE city = 'Los Angeles'
ASSERT VALUE total_fee = 109.94 WHERE city = 'New York'
ASSERT VALUE total_fee = 112.96 WHERE city = 'Philadelphia'
ASSERT VALUE total_fee = 123.94 WHERE city = 'Phoenix'
SELECT
    city,
    ROUND(SUM(delivery_fee), 2) AS total_fee
FROM {{zone_name}}.iceberg_demos.delivery_tracking
GROUP BY city
ORDER BY city;


-- ============================================================================
-- Query 9: Post-ZORDER Status Distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE status_count = 48 WHERE delivery_status = 'delivered'
ASSERT VALUE status_count = 12 WHERE delivery_status = 'in_transit'
ASSERT VALUE status_count = 12 WHERE delivery_status = 'pending'
SELECT
    delivery_status,
    COUNT(*) AS status_count
FROM {{zone_name}}.iceberg_demos.delivery_tracking
GROUP BY delivery_status
ORDER BY delivery_status;


-- ============================================================================
-- Query 10: Spatial Range Query — Manhattan / Central NYC Bounding Box
-- ============================================================================
-- After Z-ORDER, records within this geographic bounding box are co-located
-- in fewer data files. The query scans lat 40.65–40.80, lon -74.05 to -73.95
-- which covers central Manhattan and surrounding areas.

ASSERT ROW_COUNT = 10
SELECT
    delivery_id,
    driver_id,
    latitude,
    longitude,
    delivery_fee,
    city
FROM {{zone_name}}.iceberg_demos.delivery_tracking
WHERE latitude BETWEEN 40.65 AND 40.80
  AND longitude BETWEEN -74.05 AND -73.95
ORDER BY delivery_id;


-- ============================================================================
-- Query 10b: NYC Bounding Box Aggregate
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE bbox_deliveries = 10
ASSERT VALUE bbox_total_fee = 88.94
SELECT
    COUNT(*) AS bbox_deliveries,
    ROUND(SUM(delivery_fee), 2) AS bbox_total_fee
FROM {{zone_name}}.iceberg_demos.delivery_tracking
WHERE latitude BETWEEN 40.65 AND 40.80
  AND longitude BETWEEN -74.05 AND -73.95;


-- ============================================================================
-- Query 11: Spatial Range Query — Greater Los Angeles Bounding Box
-- ============================================================================
-- A wider bounding box covering the LA metro area (lat 33.0–35.0,
-- lon -119.0 to -118.0).

ASSERT ROW_COUNT = 1
ASSERT VALUE delivery_count = 12
ASSERT VALUE total_fee = 136.43
SELECT
    COUNT(*) AS delivery_count,
    ROUND(SUM(delivery_fee), 2) AS total_fee
FROM {{zone_name}}.iceberg_demos.delivery_tracking
WHERE latitude BETWEEN 33.0 AND 35.0
  AND longitude BETWEEN -119.0 AND -118.0;


-- ============================================================================
-- Query 12: DESCRIBE DETAIL — File Layout After Z-ORDER
-- ============================================================================
-- Shows the number of files after Z-ORDER compaction. The file count should
-- be reduced compared to the 3+ files created by the separate INSERT batches.

ASSERT WARNING ROW_COUNT >= 1
DESCRIBE DETAIL {{zone_name}}.iceberg_demos.delivery_tracking;


-- ============================================================================
-- VERIFY: Cross-Cutting Sanity Check
-- ============================================================================
-- Confirms the final state after: INSERT (36) → INSERT (18) → INSERT (18)
-- → OPTIMIZE ZORDER BY (latitude, longitude).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_deliveries = 72
ASSERT VALUE city_count = 6
ASSERT VALUE total_fees = 726.19
ASSERT VALUE total_weight = 233.2
ASSERT VALUE delivered_count = 48
SELECT
    COUNT(*) AS total_deliveries,
    COUNT(DISTINCT city) AS city_count,
    ROUND(SUM(delivery_fee), 2) AS total_fees,
    ROUND(SUM(package_weight), 1) AS total_weight,
    COUNT(*) FILTER (WHERE delivery_status = 'delivered') AS delivered_count
FROM {{zone_name}}.iceberg_demos.delivery_tracking;


-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata remains consistent after Z-ORDER optimization.
-- The Iceberg sort-order spec should reflect the Z-ORDER columns.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.delivery_tracking_iceberg
USING ICEBERG
LOCATION '{{data_path}}/delivery_tracking';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.delivery_tracking_iceberg TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg_demos.delivery_tracking_iceberg;


-- ============================================================================
-- Iceberg Verify 1: Spot-Check Individual Rows — Data Fidelity
-- ============================================================================
-- Verify specific rows survive the Delta → Iceberg round-trip with exact
-- values. Covers one row from each INSERT batch (seed, batch 2, batch 3).

ASSERT ROW_COUNT = 1
ASSERT VALUE driver_id = 'DRV-101'
ASSERT VALUE latitude = 40.7128
ASSERT VALUE delivery_status = 'delivered'
ASSERT VALUE delivery_fee = 8.99
ASSERT VALUE city = 'New York'
SELECT * FROM {{zone_name}}.iceberg_demos.delivery_tracking_iceberg
WHERE delivery_id = 1;


-- ============================================================================
-- Iceberg Verify 2: Batch 2 Row — delivery_id 37
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE driver_id = 'DRV-107'
ASSERT VALUE latitude = 40.7614
ASSERT VALUE delivery_status = 'delivered'
ASSERT VALUE delivery_fee = 7.50
ASSERT VALUE city = 'New York'
SELECT * FROM {{zone_name}}.iceberg_demos.delivery_tracking_iceberg
WHERE delivery_id = 37;


-- ============================================================================
-- Iceberg Verify 3: Batch 3 Row — delivery_id 72 (last row)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE driver_id = 'DRV-612'
ASSERT VALUE latitude = 39.99
ASSERT VALUE delivery_status = 'pending'
ASSERT VALUE delivery_fee = 5.50
ASSERT VALUE city = 'Philadelphia'
SELECT * FROM {{zone_name}}.iceberg_demos.delivery_tracking_iceberg
WHERE delivery_id = 72;


-- ============================================================================
-- Iceberg Verify 4: Per-City Fees & Weights — Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE total_fee = 115.97 WHERE city = 'Chicago'
ASSERT VALUE total_fee = 126.95 WHERE city = 'Houston'
ASSERT VALUE total_fee = 136.43 WHERE city = 'Los Angeles'
ASSERT VALUE total_fee = 109.94 WHERE city = 'New York'
ASSERT VALUE total_fee = 112.96 WHERE city = 'Philadelphia'
ASSERT VALUE total_fee = 123.94 WHERE city = 'Phoenix'
ASSERT VALUE total_weight = 36.8 WHERE city = 'Chicago'
ASSERT VALUE total_weight = 42.2 WHERE city = 'Houston'
ASSERT VALUE total_weight = 47.0 WHERE city = 'Los Angeles'
ASSERT VALUE total_weight = 32.4 WHERE city = 'New York'
ASSERT VALUE total_weight = 34.3 WHERE city = 'Philadelphia'
ASSERT VALUE total_weight = 40.5 WHERE city = 'Phoenix'
SELECT
    city,
    ROUND(SUM(delivery_fee), 2) AS total_fee,
    ROUND(SUM(package_weight), 1) AS total_weight
FROM {{zone_name}}.iceberg_demos.delivery_tracking_iceberg
GROUP BY city
ORDER BY city;


-- ============================================================================
-- Iceberg Verify 5: Status Distribution — Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE status_count = 48 WHERE delivery_status = 'delivered'
ASSERT VALUE status_count = 12 WHERE delivery_status = 'in_transit'
ASSERT VALUE status_count = 12 WHERE delivery_status = 'pending'
SELECT
    delivery_status,
    COUNT(*) AS status_count
FROM {{zone_name}}.iceberg_demos.delivery_tracking_iceberg
GROUP BY delivery_status
ORDER BY delivery_status;


-- ============================================================================
-- Iceberg Verify 6: Grand Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_deliveries = 72
ASSERT VALUE total_fees = 726.19
ASSERT VALUE total_weight = 233.2
ASSERT VALUE city_count = 6
ASSERT VALUE delivered_count = 48
SELECT
    COUNT(*) AS total_deliveries,
    ROUND(SUM(delivery_fee), 2) AS total_fees,
    ROUND(SUM(package_weight), 1) AS total_weight,
    COUNT(DISTINCT city) AS city_count,
    COUNT(*) FILTER (WHERE delivery_status = 'delivered') AS delivered_count
FROM {{zone_name}}.iceberg_demos.delivery_tracking_iceberg;


-- ============================================================================
-- Iceberg Verify 7: Spatial Range Query — NYC Box via Iceberg
-- ============================================================================
-- Same bounding box as Query 10, but read through Iceberg metadata.
-- Verifies spatial filtering works identically through Iceberg read path.

ASSERT ROW_COUNT = 10
SELECT
    delivery_id,
    driver_id,
    delivery_fee,
    city
FROM {{zone_name}}.iceberg_demos.delivery_tracking_iceberg
WHERE latitude BETWEEN 40.65 AND 40.80
  AND longitude BETWEEN -74.05 AND -73.95
ORDER BY delivery_id;


-- ============================================================================
-- Iceberg Verify 8: NYC Box Aggregate via Iceberg
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE bbox_deliveries = 10
ASSERT VALUE bbox_total_fee = 88.94
SELECT
    COUNT(*) AS bbox_deliveries,
    ROUND(SUM(delivery_fee), 2) AS bbox_total_fee
FROM {{zone_name}}.iceberg_demos.delivery_tracking_iceberg
WHERE latitude BETWEEN 40.65 AND 40.80
  AND longitude BETWEEN -74.05 AND -73.95;


-- ============================================================================
-- Iceberg Verify 9: LA Box via Iceberg — All 12 LA Deliveries
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE delivery_count = 12
ASSERT VALUE total_fee = 136.43
SELECT
    COUNT(*) AS delivery_count,
    ROUND(SUM(delivery_fee), 2) AS total_fee
FROM {{zone_name}}.iceberg_demos.delivery_tracking_iceberg
WHERE latitude BETWEEN 33.0 AND 35.0
  AND longitude BETWEEN -119.0 AND -118.0;
