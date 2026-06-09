-- ============================================================================
-- Iceberg V3 Deletion Vectors (Puffin) — Queries
-- ============================================================================
-- Demonstrates native Iceberg format-version 3 deletion vector support:
-- Puffin file parsing, row-position bitmap application, and post-delete
-- aggregations over a supply-chain logistics dataset. A faulty barcode
-- scanner (SCAN-ERR) produced 36 corrupt records that were retracted via
-- a Puffin deletion vector, leaving 504 valid shipments across 3 regions.
-- All queries are read-only and operate on the post-delete view.
-- ============================================================================


-- ============================================================================
-- Query 1: Post-Delete Row Count
-- ============================================================================
-- Verifies that DeltaForge correctly applies the Puffin deletion vector.
-- The original data file contains 540 rows; the DV retracts 36, leaving 504.

ASSERT ROW_COUNT = 504
SELECT * FROM {{zone_name}}.iceberg_demos.shipment_manifests;


-- ============================================================================
-- Query 2: Faulty Scanner Completely Removed
-- ============================================================================
-- All 36 rows from the faulty barcode scanner SCAN-ERR should be absent
-- after applying the deletion vector. Zero rows expected.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.shipment_manifests
WHERE scanner_id = 'SCAN-ERR';


-- ============================================================================
-- Query 3: Per-Region Shipment Counts
-- ============================================================================
-- Three regions with post-delete counts reflecting proportional removal.

ASSERT ROW_COUNT = 3
ASSERT VALUE shipment_count = 144 WHERE region = 'Americas'
ASSERT VALUE shipment_count = 180 WHERE region = 'EMEA'
ASSERT VALUE shipment_count = 180 WHERE region = 'APAC'
SELECT
    region,
    COUNT(*) AS shipment_count
FROM {{zone_name}}.iceberg_demos.shipment_manifests
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 4: Distinct Scanner Count
-- ============================================================================
-- After deleting SCAN-ERR rows, 15 valid scanners remain (from original 16).

ASSERT VALUE scanner_count = 15
SELECT
    COUNT(DISTINCT scanner_id) AS scanner_count
FROM {{zone_name}}.iceberg_demos.shipment_manifests;


-- ============================================================================
-- Query 5: Per-Category Distribution
-- ============================================================================
-- Six product categories with post-delete shipment counts.

ASSERT ROW_COUNT = 6
ASSERT VALUE shipment_count = 86 WHERE product_category = 'Automotive-Parts'
ASSERT VALUE shipment_count = 107 WHERE product_category = 'Electronics'
ASSERT VALUE shipment_count = 73 WHERE product_category = 'Heavy-Machinery'
ASSERT VALUE shipment_count = 82 WHERE product_category = 'Perishable-Foods'
ASSERT VALUE shipment_count = 77 WHERE product_category = 'Pharmaceuticals'
ASSERT VALUE shipment_count = 79 WHERE product_category = 'Textiles'
SELECT
    product_category,
    COUNT(*) AS shipment_count
FROM {{zone_name}}.iceberg_demos.shipment_manifests
GROUP BY product_category
ORDER BY product_category;


-- ============================================================================
-- Query 6: Hazardous Shipments by Region
-- ============================================================================
-- Boolean column aggregation — counts hazardous shipments per region.

ASSERT ROW_COUNT = 3
ASSERT VALUE hazardous_count = 17 WHERE region = 'Americas'
ASSERT VALUE hazardous_count = 21 WHERE region = 'EMEA'
ASSERT VALUE hazardous_count = 21 WHERE region = 'APAC'
SELECT
    region,
    SUM(CASE WHEN is_hazardous THEN 1 ELSE 0 END) AS hazardous_count
FROM {{zone_name}}.iceberg_demos.shipment_manifests
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 7: Average Weight by Region
-- ============================================================================
-- Floating-point aggregation across post-delete rows per region.

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_weight = 1276.76 WHERE region = 'Americas'
ASSERT VALUE avg_weight = 1344.52 WHERE region = 'EMEA'
ASSERT VALUE avg_weight = 1216.02 WHERE region = 'APAC'
SELECT
    region,
    ROUND(AVG(weight_kg), 2) AS avg_weight
FROM {{zone_name}}.iceberg_demos.shipment_manifests
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 8: Weight Range (Overall)
-- ============================================================================
-- Min, max, and average weight across all valid shipments.

ASSERT ROW_COUNT = 1
ASSERT VALUE min_weight = 1.29
ASSERT VALUE max_weight = 2499.03
ASSERT VALUE avg_weight = 1279.27
SELECT
    ROUND(MIN(weight_kg), 2) AS min_weight,
    ROUND(MAX(weight_kg), 2) AS max_weight,
    ROUND(AVG(weight_kg), 2) AS avg_weight
FROM {{zone_name}}.iceberg_demos.shipment_manifests;


-- ============================================================================
-- Query 9: Top Carriers by Volume
-- ============================================================================
-- All 12 carriers ranked by shipment count after deletion vector applied.

ASSERT ROW_COUNT = 12
ASSERT VALUE shipment_count = 58 WHERE carrier = 'Maersk'
ASSERT VALUE shipment_count = 48 WHERE carrier = 'Kuehne-Nagel'
ASSERT VALUE shipment_count = 46 WHERE carrier = 'FedEx'
ASSERT VALUE shipment_count = 45 WHERE carrier = 'UPS'
SELECT
    carrier,
    COUNT(*) AS shipment_count
FROM {{zone_name}}.iceberg_demos.shipment_manifests
GROUP BY carrier
ORDER BY shipment_count DESC, carrier;


-- ============================================================================
-- Query 10: Low-Value Shipments (<$500)
-- ============================================================================
-- Predicate pushdown on declared_value — identifies shipments with
-- declared value below $500.

ASSERT ROW_COUNT = 5
SELECT
    shipment_id,
    region,
    carrier,
    product_category,
    declared_value
FROM {{zone_name}}.iceberg_demos.shipment_manifests
WHERE declared_value < 500
ORDER BY declared_value ASC;


-- ============================================================================
-- Query 11: Distinct Entity Counts
-- ============================================================================
-- Exercises COUNT(DISTINCT ...) across the post-delete dataset.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_shipments = 504
ASSERT VALUE distinct_countries = 18
ASSERT VALUE distinct_carriers = 12
ASSERT VALUE distinct_categories = 6
SELECT
    COUNT(DISTINCT shipment_id) AS distinct_shipments,
    COUNT(DISTINCT destination_country) AS distinct_countries,
    COUNT(DISTINCT carrier) AS distinct_carriers,
    COUNT(DISTINCT product_category) AS distinct_categories
FROM {{zone_name}}.iceberg_demos.shipment_manifests;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, deletion vector effectiveness,
-- and key invariants. A user who runs only this query can verify the
-- Iceberg V3 deletion vector reader works end-to-end.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 504
ASSERT VALUE region_count = 3
ASSERT VALUE scanner_count = 15
ASSERT VALUE faulty_rows = 0
ASSERT VALUE total_hazardous = 59
ASSERT VALUE category_count = 6
ASSERT VALUE country_count = 18
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT scanner_id) AS scanner_count,
    SUM(CASE WHEN scanner_id = 'SCAN-ERR' THEN 1 ELSE 0 END) AS faulty_rows,
    SUM(CASE WHEN is_hazardous THEN 1 ELSE 0 END) AS total_hazardous,
    COUNT(DISTINCT product_category) AS category_count,
    COUNT(DISTINCT destination_country) AS country_count
FROM {{zone_name}}.iceberg_demos.shipment_manifests;
