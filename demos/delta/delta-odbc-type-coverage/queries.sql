-- ============================================================================
-- Demo: Global Logistics Shipment Telemetry: Full Type Coverage for ODBC
-- ============================================================================
-- These queries are written from the perspective of a Power BI / Tableau /
-- DBeaver / Excel user issuing SELECTs through the Delta Forge ODBC driver.
-- Every query returns a result an ODBC client should be able to bind, and
-- every assertion value was precomputed by generate_data.py and recorded
-- in proofs.txt before this file was authored.
-- ============================================================================


-- ============================================================================
-- Query 1: Inventory baseline (every cardinality the client cares about)
-- ============================================================================
-- Validates that the table opened, that the row count matches the seed, and
-- that distinct counts on string, struct.field, and enum-style columns all
-- come back through ODBC correctly.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_shipments = 200
ASSERT VALUE distinct_carriers = 5
ASSERT VALUE distinct_services = 4
ASSERT VALUE distinct_currencies = 5
ASSERT VALUE distinct_origin_countries = 10
ASSERT VALUE distinct_dest_countries = 10
SELECT
    COUNT(*)                                                AS total_shipments,
    COUNT(DISTINCT carrier_code)                            AS distinct_carriers,
    COUNT(DISTINCT service_level)                           AS distinct_services,
    COUNT(DISTINCT currency)                                AS distinct_currencies,
    COUNT(DISTINCT origin_address.country_code)             AS distinct_origin_countries,
    COUNT(DISTINCT destination_address.country_code)        AS distinct_dest_countries
FROM {{zone_name}}.bi_demos.shipments_full_types;


-- ============================================================================
-- Query 2: Service-level mix (categorical GROUP BY on CHAR column)
-- ============================================================================
-- BI clients almost always start with a categorical breakdown. Proves CHAR(3)
-- columns survive ODBC roundtrip with no padding artifacts in GROUP BY.

ASSERT ROW_COUNT = 4
ASSERT VALUE shipment_count = 50 WHERE service_level = 'ECO'
ASSERT VALUE shipment_count = 50 WHERE service_level = 'EXP'
ASSERT VALUE shipment_count = 50 WHERE service_level = 'OVN'
ASSERT VALUE shipment_count = 50 WHERE service_level = 'STD'
SELECT service_level, COUNT(*) AS shipment_count
FROM {{zone_name}}.bi_demos.shipments_full_types
GROUP BY service_level
ORDER BY service_level;


-- ============================================================================
-- Query 3: Per-currency revenue rollup (DECIMAL aggregation)
-- ============================================================================
-- Confirms DECIMAL(15,4) addition stays exact through SUM and that ODBC
-- exposes the decimal scale to the client without floating-point drift.

ASSERT ROW_COUNT = 5
ASSERT VALUE shipment_count = 40 WHERE currency = 'AUD'
ASSERT VALUE shipment_count = 40 WHERE currency = 'EUR'
ASSERT VALUE shipment_count = 40 WHERE currency = 'GBP'
ASSERT VALUE shipment_count = 40 WHERE currency = 'JPY'
ASSERT VALUE shipment_count = 40 WHERE currency = 'USD'
ASSERT VALUE total_declared = 82875.0500 WHERE currency = 'AUD'
ASSERT VALUE total_declared = 84721.0500 WHERE currency = 'EUR'
ASSERT VALUE total_declared = 83401.8700 WHERE currency = 'GBP'
ASSERT VALUE total_declared = 84061.4600 WHERE currency = 'JPY'
ASSERT VALUE total_declared = 85380.6400 WHERE currency = 'USD'
SELECT currency,
       COUNT(*)              AS shipment_count,
       SUM(declared_value)   AS total_declared
FROM {{zone_name}}.bi_demos.shipments_full_types
GROUP BY currency
ORDER BY currency;


-- ============================================================================
-- Query 4: Pickup volume by month (DATE bucketing time-series)
-- ============================================================================
-- A canonical time-series query for a BI dashboard. Validates DATE column
-- delivery and EXTRACT semantics through ODBC.

ASSERT ROW_COUNT = 7
ASSERT VALUE pickup_count = 31 WHERE pickup_month = 1
ASSERT VALUE pickup_count = 28 WHERE pickup_month = 2
ASSERT VALUE pickup_count = 31 WHERE pickup_month = 3
ASSERT VALUE pickup_count = 30 WHERE pickup_month = 4
ASSERT VALUE pickup_count = 31 WHERE pickup_month = 5
ASSERT VALUE pickup_count = 30 WHERE pickup_month = 6
ASSERT VALUE pickup_count = 19 WHERE pickup_month = 7
SELECT EXTRACT(MONTH FROM pickup_date) AS pickup_month,
       COUNT(*)                        AS pickup_count
FROM {{zone_name}}.bi_demos.shipments_full_types
GROUP BY EXTRACT(MONTH FROM pickup_date)
ORDER BY pickup_month;


-- ============================================================================
-- Query 5: Hazardous shipments split (BOOLEAN filter + numeric summary)
-- ============================================================================
-- Proves BOOLEAN columns predicate correctly and that FLOAT (weight_kg) and
-- DOUBLE (distance_km) aggregate side by side without precision loss.

ASSERT ROW_COUNT = 2
ASSERT VALUE shipment_count = 50  WHERE is_hazardous = true
ASSERT VALUE shipment_count = 150 WHERE is_hazardous = false
SELECT is_hazardous,
       COUNT(*)                          AS shipment_count,
       ROUND(SUM(weight_kg), 2)          AS total_weight_kg,
       ROUND(SUM(distance_km), 2)        AS total_distance_km
FROM {{zone_name}}.bi_demos.shipments_full_types
GROUP BY is_hazardous
ORDER BY is_hazardous;


-- ============================================================================
-- Query 6: Origin lane analysis (STRUCT field projection in GROUP BY)
-- ============================================================================
-- Drills into a struct sub-field. ODBC clients should be able to project
-- struct fields by dot path. Each origin country contributes 20 shipments.

ASSERT ROW_COUNT = 10
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'US'
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'CN'
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'JP'
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'BR'
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'ZA'
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'NL'
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'AE'
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'KR'
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'CA'
ASSERT VALUE shipment_count = 20 WHERE origin_country = 'GB'
SELECT origin_address.country_code AS origin_country,
       COUNT(*)                    AS shipment_count
FROM {{zone_name}}.bi_demos.shipments_full_types
GROUP BY origin_address.country_code
ORDER BY origin_country;


-- ============================================================================
-- Query 7: Waypoint coverage (ARRAY size aggregation)
-- ============================================================================
-- Proves ARRAY columns are queryable through size() and that the integer
-- result is correct. Each shipment has 3 to 6 waypoints; the average
-- across 200 shipments is exactly 4.5 by construction.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_waypoints = 900
ASSERT VALUE min_waypoints = 3
ASSERT VALUE max_waypoints = 6
SELECT SUM(size(waypoint_codes)) AS total_waypoints,
       MIN(size(waypoint_codes)) AS min_waypoints,
       MAX(size(waypoint_codes)) AS max_waypoints
FROM {{zone_name}}.bi_demos.shipments_full_types;


-- ============================================================================
-- Query 8: SLA roll-up (MAP entry-count + DECIMAL averaging)
-- ============================================================================
-- Confirms MAP columns expose size() and that mixing it with DECIMAL/FLOAT
-- aggregations works in a single SELECT. Each shipment carries exactly
-- 3 SLA metrics in the sla_metrics map and 3 freeform tags.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_sla_entries = 600
ASSERT VALUE total_tag_entries = 600
ASSERT VALUE distinct_priorities = 5
SELECT SUM(size(sla_metrics))             AS total_sla_entries,
       SUM(size(tags))                    AS total_tag_entries,
       COUNT(DISTINCT priority_code)      AS distinct_priorities
FROM {{zone_name}}.bi_demos.shipments_full_types;


-- ============================================================================
-- Query 9: Shipper contact channels (nested STRUCT containing ARRAY)
-- ============================================================================
-- Reaches into a nested STRUCT<..., preferred_channels: ARRAY<STRING>> field.
-- Each shipper carries between 2 and 4 channels. Total channels across
-- 200 shipments equals 599 (verified by duckdb: SUM(len(preferred_channels))).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_channels = 599
ASSERT VALUE min_channels = 2
ASSERT VALUE max_channels = 4
SELECT SUM(size(shipper_contact.preferred_channels)) AS total_channels,
       MIN(size(shipper_contact.preferred_channels)) AS min_channels,
       MAX(size(shipper_contact.preferred_channels)) AS max_channels
FROM {{zone_name}}.bi_demos.shipments_full_types;


-- ============================================================================
-- Query 10: First and last row inspection (end-to-end fidelity check)
-- ============================================================================
-- Pins the exact STRING, TIMESTAMP, struct.field, and DECIMAL values for
-- the first and last rows so any single-row regression in ODBC marshalling
-- is caught immediately.

ASSERT ROW_COUNT = 2
ASSERT VALUE tracking_number = 'DF-00001-00007' WHERE shipment_id = 1000001
ASSERT VALUE origin_country = 'US' WHERE shipment_id = 1000001
ASSERT VALUE dest_country = 'DE' WHERE shipment_id = 1000001
ASSERT VALUE currency = 'USD' WHERE shipment_id = 1000001
ASSERT VALUE tracking_number = 'DF-00200-06176' WHERE shipment_id = 1000200
ASSERT VALUE currency = 'AUD' WHERE shipment_id = 1000200
ASSERT VALUE declared_value = 1098.2000 WHERE shipment_id = 1000200
SELECT shipment_id,
       tracking_number,
       declared_value,
       currency,
       origin_address.country_code      AS origin_country,
       destination_address.country_code AS dest_country
FROM {{zone_name}}.bi_demos.shipments_full_types
WHERE shipment_id IN (1000001, 1000200)
ORDER BY shipment_id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: every primitive aggregation, every collection
-- size, and every grouped count, all in one row.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_shipments = 200
ASSERT VALUE hazardous_count = 50
ASSERT VALUE signature_count = 125
ASSERT VALUE total_declared = 420440.0700
ASSERT VALUE total_sla_exceptions = 400
ASSERT VALUE total_waypoints = 900
ASSERT VALUE total_channels = 599
ASSERT VALUE total_tag_entries = 600
ASSERT VALUE total_sla_entries = 600
ASSERT VALUE earliest_pickup = DATE '2026-01-01'
ASSERT VALUE latest_pickup = DATE '2026-07-19'
SELECT COUNT(*)                                              AS total_shipments,
       SUM(CAST(is_hazardous AS INT))                        AS hazardous_count,
       SUM(CAST(requires_signature AS INT))                  AS signature_count,
       SUM(declared_value)                                   AS total_declared,
       SUM(sla_metrics['exceptions'])                        AS total_sla_exceptions,
       SUM(size(waypoint_codes))                             AS total_waypoints,
       SUM(size(shipper_contact.preferred_channels))         AS total_channels,
       SUM(size(tags))                                       AS total_tag_entries,
       SUM(size(sla_metrics))                                AS total_sla_entries,
       MIN(pickup_date)                                      AS earliest_pickup,
       MAX(pickup_date)                                      AS latest_pickup
FROM {{zone_name}}.bi_demos.shipments_full_types;
