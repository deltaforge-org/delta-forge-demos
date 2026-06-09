-- ============================================================================
-- Demo: Freight Shipping Manifest
-- ============================================================================
-- Validates protobuf features not covered by existing demos: boolean fields,
-- int64 monetary values, float weights/dimensions, 3-level message nesting
-- (Shipment → Package → Dimensions), multiple enums (ShipmentStatus,
-- PackageClass), and multiple repeated fields per message.
-- ============================================================================


-- ============================================================================
-- Query 1: SHIPMENT OVERVIEW — 12 shipments across 3 carrier files
-- ============================================================================
-- Verifies the join-mode table reads all shipments with correct enum decoding
-- and boolean field values.

ASSERT ROW_COUNT = 12
ASSERT VALUE status = 'DELIVERED' WHERE shipment_id = 'SHIP-A001'
ASSERT VALUE origin = 'New York, NY' WHERE shipment_id = 'SHIP-A001'
ASSERT VALUE status = 'CREATED' WHERE shipment_id = 'SHIP-B003'
ASSERT VALUE is_express = true WHERE shipment_id = 'SHIP-A001'
ASSERT VALUE is_insured = true WHERE shipment_id = 'SHIP-A002'
SELECT shipment_id, origin, destination, status, is_express, is_insured,
       total_cost_cents, package_ids, created_at
FROM {{zone_name}}.protobuf_freight.shipments
ORDER BY shipment_id;


-- ============================================================================
-- Query 2: PACKAGE INVENTORY — 24 packages with 3-level nested dimensions
-- ============================================================================
-- Verifies that the explode_paths correctly produces one row per package and
-- that 3-level nesting (Shipment → Package → Dimensions) is correctly
-- flattened to top-level float columns.

ASSERT ROW_COUNT = 24
ASSERT VALUE weight_kg = 2.5 WHERE package_id = 'PKG-A001-1'
ASSERT VALUE length_cm = 40.0 WHERE package_id = 'PKG-A001-1'
ASSERT VALUE width_cm = 30.0 WHERE package_id = 'PKG-A001-1'
ASSERT VALUE height_cm = 20.0 WHERE package_id = 'PKG-A001-1'
ASSERT VALUE package_class = 'FRAGILE' WHERE package_id = 'PKG-A001-1'
ASSERT VALUE requires_signature = true WHERE package_id = 'PKG-A001-1'
ASSERT VALUE declared_value_cents = 89900 WHERE package_id = 'PKG-A001-1'
SELECT package_id, description, weight_kg, length_cm, width_cm, height_cm,
       package_class, requires_signature, declared_value_cents, shipment_id
FROM {{zone_name}}.protobuf_freight.shipment_packages
ORDER BY package_id;


-- ============================================================================
-- Query 3: TRACKING TIMELINE — 39 tracking events across all shipments
-- ============================================================================
-- Verifies the second repeated field (tracking) is correctly exploded
-- independently from packages.

ASSERT ROW_COUNT = 12
ASSERT VALUE event_count = 5 WHERE shipment_id = 'SHIP-A005'
ASSERT VALUE event_count = 1 WHERE shipment_id = 'SHIP-B003'
SELECT shipment_id,
       COUNT(*) AS event_count
FROM {{zone_name}}.protobuf_freight.shipment_tracking
GROUP BY shipment_id
ORDER BY event_count DESC, shipment_id;


-- ============================================================================
-- Query 4: SHIPMENT STATUS DISTRIBUTION — 5 distinct enum values
-- ============================================================================
-- Verifies that all ShipmentStatus enum values are decoded to string labels.

ASSERT ROW_COUNT = 5
ASSERT VALUE shipment_count = 5 WHERE status = 'DELIVERED'
ASSERT VALUE shipment_count = 3 WHERE status = 'IN_TRANSIT'
ASSERT VALUE shipment_count = 2 WHERE status = 'PICKED_UP'
ASSERT VALUE shipment_count = 1 WHERE status = 'CREATED'
ASSERT VALUE shipment_count = 1 WHERE status = 'RETURNED'
SELECT status,
       COUNT(*) AS shipment_count
FROM {{zone_name}}.protobuf_freight.shipments
GROUP BY status
ORDER BY shipment_count DESC, status;


-- ============================================================================
-- Query 5: PACKAGE CLASS DISTRIBUTION — 4 distinct enum values
-- ============================================================================
-- Verifies that PackageClass enum (second enum in the schema) is decoded.

ASSERT ROW_COUNT = 4
ASSERT VALUE pkg_count = 11 WHERE package_class = 'STANDARD'
ASSERT VALUE pkg_count = 7 WHERE package_class = 'FRAGILE'
ASSERT VALUE pkg_count = 4 WHERE package_class = 'PERISHABLE'
ASSERT VALUE pkg_count = 2 WHERE package_class = 'HAZMAT'
SELECT package_class,
       COUNT(*) AS pkg_count
FROM {{zone_name}}.protobuf_freight.shipment_packages
GROUP BY package_class
ORDER BY pkg_count DESC;


-- ============================================================================
-- Query 6: EXPRESS vs STANDARD — Bool field analysis with int64 aggregation
-- ============================================================================
-- Uses the is_express boolean field to segment shipments and aggregates
-- total_cost_cents (int64) per segment.

ASSERT ROW_COUNT = 2
ASSERT VALUE shipment_count = 5 WHERE is_express = true
ASSERT VALUE shipment_count = 7 WHERE is_express = false
ASSERT VALUE total_cost = 315000 WHERE is_express = true
ASSERT VALUE total_cost = 327000 WHERE is_express = false
SELECT is_express,
       COUNT(*) AS shipment_count,
       SUM(total_cost_cents) AS total_cost
FROM {{zone_name}}.protobuf_freight.shipments
GROUP BY is_express
ORDER BY is_express DESC;


-- ============================================================================
-- Query 7: SIGNATURE REQUIRED — Package-level bool field
-- ============================================================================
-- Verifies the requires_signature boolean on the Package message, which is
-- nested inside Shipment (2nd level bool).

ASSERT ROW_COUNT = 2
ASSERT VALUE pkg_count = 12 WHERE requires_signature = true
ASSERT VALUE pkg_count = 12 WHERE requires_signature = false
SELECT requires_signature,
       COUNT(*) AS pkg_count
FROM {{zone_name}}.protobuf_freight.shipment_packages
GROUP BY requires_signature
ORDER BY requires_signature DESC;


-- ============================================================================
-- Query 8: HIGH-VALUE PACKAGES — Int64 field filtering and aggregation
-- ============================================================================
-- Filters on declared_value_cents (int64) > 100000 ($1,000.00) and verifies
-- exact monetary totals.

ASSERT ROW_COUNT = 1
ASSERT VALUE row_count = 8
ASSERT VALUE total_value = 2150000
ASSERT VALUE max_value = 500000
SELECT COUNT(*) AS row_count,
       SUM(declared_value_cents) AS total_value,
       MAX(declared_value_cents) AS max_value
FROM {{zone_name}}.protobuf_freight.shipment_packages
WHERE declared_value_cents > 100000;


-- ============================================================================
-- Query 9: WEIGHT ANALYSIS — Float field aggregation
-- ============================================================================
-- Aggregates weight_kg (float) across all packages. Tests float precision
-- with SUM, MIN, MAX, and AVG.

ASSERT ROW_COUNT = 1
ASSERT VALUE max_weight = 15.0
ASSERT VALUE min_weight = 0.3
SELECT ROUND(CAST(SUM(weight_kg) AS DOUBLE), 1) AS total_weight,
       MAX(weight_kg) AS max_weight,
       MIN(weight_kg) AS min_weight,
       ROUND(CAST(AVG(weight_kg) AS DOUBLE), 1) AS avg_weight
FROM {{zone_name}}.protobuf_freight.shipment_packages;


-- ============================================================================
-- Query 10: VOLUME CALCULATION — 3-level nesting arithmetic
-- ============================================================================
-- Computes package volume from Dimensions fields (length × width × height),
-- proving that 3-level nested float values are correctly extracted and usable
-- in expressions. Returns top 5 by volume.

ASSERT ROW_COUNT = 5
ASSERT VALUE package_id = 'PKG-A005-1' WHERE rank_num = 1
ASSERT VALUE package_id = 'PKG-A005-2' WHERE rank_num = 2
SELECT ROW_NUMBER() OVER (ORDER BY CAST(length_cm AS DOUBLE) * CAST(width_cm AS DOUBLE) * CAST(height_cm AS DOUBLE) DESC) AS rank_num,
       package_id,
       description,
       ROUND(CAST(length_cm AS DOUBLE) * CAST(width_cm AS DOUBLE) * CAST(height_cm AS DOUBLE), 0) AS volume_cm3,
       length_cm,
       width_cm,
       height_cm
FROM {{zone_name}}.protobuf_freight.shipment_packages
ORDER BY volume_cm3 DESC
LIMIT 5;


-- ============================================================================
-- Query 11: CARRIER FILE DISTRIBUTION — Multi-file and file metadata
-- ============================================================================
-- Verifies that all 3 carrier files are read and shipments per file match.

ASSERT ROW_COUNT = 3
ASSERT VALUE shipment_count = 5 WHERE df_file_name LIKE '%carrier_alpha%'
ASSERT VALUE shipment_count = 4 WHERE df_file_name LIKE '%carrier_beta%'
ASSERT VALUE shipment_count = 3 WHERE df_file_name LIKE '%carrier_gamma%'
SELECT df_file_name,
       COUNT(*) AS shipment_count
FROM {{zone_name}}.protobuf_freight.shipments
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- Query 12: INSURED SHIPMENTS — Combined bool + int64 analysis
-- ============================================================================
-- Combines is_insured (bool) filtering with total_cost_cents (int64) to
-- analyze insured shipment costs.

ASSERT ROW_COUNT = 1
ASSERT VALUE insured_count = 7
ASSERT VALUE insured_total = 486000
SELECT COUNT(*) AS insured_count,
       SUM(total_cost_cents) AS insured_total
FROM {{zone_name}}.protobuf_freight.shipments
WHERE is_insured = true;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check covering key data invariants across all three
-- tables and all protobuf feature areas.

ASSERT ROW_COUNT = 8
ASSERT VALUE result = 'PASS' WHERE check_name = 'shipment_count_12'
ASSERT VALUE result = 'PASS' WHERE check_name = 'package_rows_24'
ASSERT VALUE result = 'PASS' WHERE check_name = 'tracking_rows_39'
ASSERT VALUE result = 'PASS' WHERE check_name = 'five_statuses'
ASSERT VALUE result = 'PASS' WHERE check_name = 'four_package_classes'
ASSERT VALUE result = 'PASS' WHERE check_name = 'five_express'
ASSERT VALUE result = 'PASS' WHERE check_name = 'twelve_require_sig'
ASSERT VALUE result = 'PASS' WHERE check_name = 'three_source_files'
SELECT check_name, result FROM (

    -- Check 1: Total shipments = 12
    SELECT 'shipment_count_12' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.protobuf_freight.shipments) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Exploded package rows = 24
    SELECT 'package_rows_24' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.protobuf_freight.shipment_packages) = 24
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Exploded tracking rows = 39
    SELECT 'tracking_rows_39' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.protobuf_freight.shipment_tracking) = 39
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: 5 distinct shipment statuses (enum decoding)
    SELECT 'five_statuses' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT status) FROM {{zone_name}}.protobuf_freight.shipments) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: 4 distinct package classes (second enum)
    SELECT 'four_package_classes' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT package_class) FROM {{zone_name}}.protobuf_freight.shipment_packages) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: 5 express shipments (bool field)
    SELECT 'five_express' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf_freight.shipments
               WHERE is_express = true
           ) = 5 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: 12 packages require signature (nested bool)
    SELECT 'twelve_require_sig' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf_freight.shipment_packages
               WHERE requires_signature = true
           ) = 12 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: 3 source files
    SELECT 'three_source_files' AS check_name,
           CASE WHEN (
               SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.protobuf_freight.shipments
           ) = 3 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
