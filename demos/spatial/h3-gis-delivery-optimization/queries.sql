-- ============================================================================
-- Demo: H3+GIS Delivery Optimization — Cross-Function Spatial Analytics
-- ============================================================================
-- Combines H3 hexagonal indexing with GIS distance/bearing functions for
-- delivery logistics optimization. Tests cross-function patterns that no
-- existing spatial demo covers: H3 cell assignment + GIS distance ranking,
-- grid topology + haversine comparison, multi-resolution hierarchy + bearing.
--
-- H3 functions: h3_latlng_to_cell, h3_cell_to_string, h3_cell_to_lat,
--   h3_cell_to_lng, h3_get_resolution, h3_is_valid_cell, h3_is_pentagon,
--   h3_is_res_class_iii, h3_cell_to_parent, h3_cell_to_children,
--   h3_cell_to_center_child, h3_grid_distance, h3_hex_ring, h3_hex_disk,
--   h3_cell_area, h3_cell_area_km2, h3_cell_to_boundary
-- GIS functions: st_distance, st_distance_vincenty, st_bearing,
--   st_make_point, st_x, st_y
-- ============================================================================


-- ============================================================================
-- Query 1: Warehouse & Store Catalog — Baseline
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE warehouse_name = 'Chicago Distribution Center' WHERE warehouse_id = 1
ASSERT VALUE capacity_pallets = 50000 WHERE warehouse_id = 1
SELECT warehouse_id, warehouse_name, lat, lng, capacity_pallets
FROM {{zone_name}}.logistics.warehouses
ORDER BY warehouse_id;


-- ============================================================================
-- Query 2: Store Catalog — Baseline
-- ============================================================================

ASSERT ROW_COUNT = 15
ASSERT VALUE store_name = 'Milwaukee Store' WHERE store_id = 1
ASSERT VALUE warehouse_id = 1 WHERE store_id = 1
ASSERT VALUE store_name = 'Raleigh Store' WHERE store_id = 15
SELECT store_id, store_name, lat, lng, warehouse_id, monthly_orders
FROM {{zone_name}}.logistics.stores
ORDER BY store_id;


-- ============================================================================
-- Query 3: H3 Cell Assignment — Warehouses at Resolution 7
-- ============================================================================
-- Assign each warehouse an H3 cell at resolution 7 (~5.2 km² per cell).
-- Verifies cell validity, resolution, and center coordinate round-trip.

ASSERT ROW_COUNT = 3
ASSERT VALUE h3_hex = '872664c1affffff' WHERE warehouse_id = 1
ASSERT VALUE is_valid = true WHERE warehouse_id = 1
ASSERT VALUE resolution = 7 WHERE warehouse_id = 1
ASSERT VALUE is_pentagon = false WHERE warehouse_id = 1
SELECT
    warehouse_id,
    warehouse_name,
    h3_cell_to_string(h3_latlng_to_cell(lat, lng, 7)) AS h3_hex,
    h3_is_valid_cell(h3_latlng_to_cell(lat, lng, 7)) AS is_valid,
    h3_get_resolution(h3_latlng_to_cell(lat, lng, 7)) AS resolution,
    h3_is_pentagon(h3_latlng_to_cell(lat, lng, 7)) AS is_pentagon,
    h3_is_res_class_iii(h3_latlng_to_cell(lat, lng, 7)) AS is_class_iii,
    ROUND(h3_cell_to_lat(h3_latlng_to_cell(lat, lng, 7)), 4) AS cell_center_lat,
    ROUND(h3_cell_to_lng(h3_latlng_to_cell(lat, lng, 7)), 4) AS cell_center_lng
FROM {{zone_name}}.logistics.warehouses
ORDER BY warehouse_id;


-- ============================================================================
-- Query 4: GIS Distance — Warehouse to Assigned Stores (km)
-- ============================================================================
-- Compute haversine great-circle distance from each warehouse to its
-- assigned stores. This is the core GIS metric for delivery routing.

ASSERT ROW_COUNT = 15
ASSERT VALUE distance_km = 131 WHERE store_name = 'Milwaukee Store'
ASSERT VALUE distance_km = 362 WHERE store_name = 'Houston Store'
ASSERT VALUE distance_km = 226 WHERE store_name = 'Birmingham Store'
SELECT
    w.warehouse_name,
    s.store_name,
    ROUND(st_distance(w.lat, w.lng, s.lat, s.lng) / 1000.0, 0) AS distance_km,
    ROUND(st_bearing(w.lat, w.lng, s.lat, s.lng), 2) AS bearing_deg
FROM {{zone_name}}.logistics.stores s
JOIN {{zone_name}}.logistics.warehouses w ON w.warehouse_id = s.warehouse_id
ORDER BY w.warehouse_id, s.store_id;


-- ============================================================================
-- Query 5: H3 Grid Distance — Warehouse to Stores at Resolution 5
-- ============================================================================
-- H3 grid_distance counts the minimum number of cell hops between two cells.
-- Using resolution 5 (~252 km² cells) for longer-range grid topology.

ASSERT ROW_COUNT = 15
ASSERT VALUE grid_dist = 9 WHERE store_name = 'Milwaukee Store'
ASSERT VALUE grid_dist = 23 WHERE store_name = 'Houston Store'
ASSERT VALUE grid_dist = 15 WHERE store_name = 'Birmingham Store'
SELECT
    w.warehouse_name,
    s.store_name,
    h3_grid_distance(
        h3_cell_to_parent(h3_latlng_to_cell(w.lat, w.lng, 7), 5),
        h3_cell_to_parent(h3_latlng_to_cell(s.lat, s.lng, 7), 5)
    ) AS grid_dist,
    ROUND(st_distance(w.lat, w.lng, s.lat, s.lng) / 1000.0, 0) AS gis_dist_km
FROM {{zone_name}}.logistics.stores s
JOIN {{zone_name}}.logistics.warehouses w ON w.warehouse_id = s.warehouse_id
ORDER BY w.warehouse_id, s.store_id;


-- ============================================================================
-- Query 6: Multi-Resolution Hierarchy — Chicago Warehouse
-- ============================================================================
-- Navigate the H3 hierarchy: parent cells at coarser resolutions (regional
-- grouping), children at finer resolution (local detail).

ASSERT ROW_COUNT = 7
ASSERT VALUE h3_is_valid_cell = true WHERE child_index = 1
SELECT
    ROW_NUMBER() OVER (ORDER BY child) AS child_index,
    child,
    h3_cell_to_string(child) AS child_hex,
    h3_is_valid_cell(child) AS h3_is_valid_cell,
    h3_get_resolution(child) AS resolution
FROM (
    SELECT UNNEST(h3_cell_to_children(h3_latlng_to_cell(41.8781, -87.6298, 7), 8)) AS child
)
ORDER BY child;


-- ============================================================================
-- Query 7: H3 Cell Properties — Area and Topology
-- ============================================================================
-- Compare cell area across warehouse locations. Area varies slightly by
-- latitude due to H3's icosahedral projection (cells closer to poles are
-- slightly different in geodesic area).

ASSERT ROW_COUNT = 1
ASSERT VALUE area_km2 > 4
ASSERT VALUE area_km2 < 6
ASSERT VALUE ring_k1 = 6
ASSERT VALUE disk_k1 = 7
ASSERT VALUE disk_k2 = 19
SELECT
    ROUND(h3_cell_area_km2(h3_latlng_to_cell(41.8781, -87.6298, 7)), 2) AS area_km2,
    (SELECT COUNT(*) FROM (SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(41.8781, -87.6298, 7), 1)) AS c)) AS ring_k1,
    (SELECT COUNT(*) FROM (SELECT UNNEST(h3_hex_ring(h3_latlng_to_cell(41.8781, -87.6298, 7), 2)) AS c)) AS ring_k2,
    (SELECT COUNT(*) FROM (SELECT UNNEST(h3_hex_disk(h3_latlng_to_cell(41.8781, -87.6298, 7), 1)) AS c)) AS disk_k1,
    (SELECT COUNT(*) FROM (SELECT UNNEST(h3_hex_disk(h3_latlng_to_cell(41.8781, -87.6298, 7), 2)) AS c)) AS disk_k2;


-- ============================================================================
-- Query 8: H3 String Conversion Round-Trip
-- ============================================================================
-- Convert cell ID to hex string and back. Verifies lossless conversion.

ASSERT ROW_COUNT = 3
ASSERT VALUE roundtrip_match = true WHERE warehouse_id = 1
ASSERT VALUE roundtrip_match = true WHERE warehouse_id = 2
ASSERT VALUE roundtrip_match = true WHERE warehouse_id = 3
SELECT
    warehouse_id,
    h3_latlng_to_cell(lat, lng, 7) AS cell_id,
    h3_cell_to_string(h3_latlng_to_cell(lat, lng, 7)) AS hex_str,
    h3_string_to_cell(h3_cell_to_string(h3_latlng_to_cell(lat, lng, 7))) AS roundtrip_cell,
    h3_latlng_to_cell(lat, lng, 7) = h3_string_to_cell(h3_cell_to_string(h3_latlng_to_cell(lat, lng, 7))) AS roundtrip_match
FROM {{zone_name}}.logistics.warehouses
ORDER BY warehouse_id;


-- ============================================================================
-- Query 9: H3 Cell Boundary — WKT Polygon Extraction
-- ============================================================================
-- Extract the hexagonal boundary as a WKT polygon for each warehouse cell.
-- h3_cell_to_boundary returns a POLYGON with 7 vertices (6 unique + closing).

ASSERT ROW_COUNT = 1
ASSERT VALUE boundary_wkt LIKE 'POLYGON%'
SELECT
    h3_cell_to_boundary(h3_latlng_to_cell(41.8781, -87.6298, 7)) AS boundary_wkt;


-- ============================================================================
-- Query 10: Suboptimal Assignment Detection — H3+GIS Combined
-- ============================================================================
-- Find stores where the assigned warehouse is NOT the nearest by GIS distance.
-- Memphis (store 10) is assigned to Dallas but Atlanta is closer.
-- Combines st_distance ranking with H3 cell assignment.

ASSERT ROW_COUNT = 1
ASSERT VALUE store_name = 'Memphis Store' WHERE store_id = 10
ASSERT VALUE assigned_warehouse = 'Dallas Fulfillment Hub' WHERE store_id = 10
ASSERT VALUE nearest_warehouse = 'Atlanta Logistics Park' WHERE store_id = 10
WITH distance_ranked AS (
    SELECT
        s.store_id,
        s.store_name,
        s.warehouse_id AS assigned_wh_id,
        w_assigned.warehouse_name AS assigned_warehouse,
        w_all.warehouse_id AS candidate_wh_id,
        w_all.warehouse_name AS candidate_warehouse,
        ROUND(st_distance(s.lat, s.lng, w_all.lat, w_all.lng) / 1000.0, 0) AS distance_km,
        h3_cell_to_string(h3_latlng_to_cell(s.lat, s.lng, 7)) AS store_h3,
        ROW_NUMBER() OVER (PARTITION BY s.store_id ORDER BY st_distance(s.lat, s.lng, w_all.lat, w_all.lng)) AS rn
    FROM {{zone_name}}.logistics.stores s
    JOIN {{zone_name}}.logistics.warehouses w_assigned ON w_assigned.warehouse_id = s.warehouse_id
    CROSS JOIN {{zone_name}}.logistics.warehouses w_all
)
SELECT
    store_id,
    store_name,
    assigned_warehouse,
    candidate_warehouse AS nearest_warehouse,
    distance_km AS nearest_distance_km,
    store_h3
FROM distance_ranked
WHERE rn = 1 AND candidate_wh_id != assigned_wh_id
ORDER BY store_id;


-- ============================================================================
-- Query 11: Delivery Route Scoring — Combined H3 + GIS Metrics
-- ============================================================================
-- Score each warehouse-store delivery route using both GIS distance and
-- H3 grid topology. Routes with shorter GIS distance AND fewer H3 hops
-- indicate better logistics positioning.

ASSERT ROW_COUNT = 15
ASSERT VALUE gis_km = 131 WHERE store_name = 'Milwaukee Store'
ASSERT VALUE h3_hops = 9 WHERE store_name = 'Milwaukee Store'
ASSERT VALUE gis_km = 676 WHERE store_name = 'Memphis Store'
ASSERT VALUE h3_hops = 40 WHERE store_name = 'Memphis Store'
SELECT
    w.warehouse_name,
    s.store_name,
    ROUND(st_distance(w.lat, w.lng, s.lat, s.lng) / 1000.0, 0) AS gis_km,
    h3_grid_distance(
        h3_cell_to_parent(h3_latlng_to_cell(w.lat, w.lng, 7), 5),
        h3_cell_to_parent(h3_latlng_to_cell(s.lat, s.lng, 7), 5)
    ) AS h3_hops,
    ROUND(st_bearing(w.lat, w.lng, s.lat, s.lng), 0) AS bearing_deg,
    st_make_point(s.lng, s.lat) AS store_point
FROM {{zone_name}}.logistics.stores s
JOIN {{zone_name}}.logistics.warehouses w ON w.warehouse_id = s.warehouse_id
ORDER BY w.warehouse_id, gis_km;


-- ============================================================================
-- Query 12: Warehouse Coverage Summary — Aggregate Analytics
-- ============================================================================
-- Per-warehouse aggregation: store count, total orders, average GIS distance.

ASSERT ROW_COUNT = 3
ASSERT VALUE store_count = 5 WHERE warehouse_name = 'Chicago Distribution Center'
ASSERT VALUE total_orders = 16200 WHERE warehouse_name = 'Chicago Distribution Center'
ASSERT VALUE avg_distance_km = 354 WHERE warehouse_name = 'Chicago Distribution Center'
ASSERT VALUE total_orders = 16300 WHERE warehouse_name = 'Dallas Fulfillment Hub'
ASSERT VALUE total_orders = 15900 WHERE warehouse_name = 'Atlanta Logistics Park'
SELECT
    w.warehouse_name,
    COUNT(*) AS store_count,
    SUM(s.monthly_orders) AS total_orders,
    ROUND(AVG(st_distance(w.lat, w.lng, s.lat, s.lng) / 1000.0), 0) AS avg_distance_km,
    ROUND(MIN(st_distance(w.lat, w.lng, s.lat, s.lng) / 1000.0), 0) AS min_distance_km,
    ROUND(MAX(st_distance(w.lat, w.lng, s.lat, s.lng) / 1000.0), 0) AS max_distance_km
FROM {{zone_name}}.logistics.warehouses w
JOIN {{zone_name}}.logistics.stores s ON s.warehouse_id = w.warehouse_id
GROUP BY w.warehouse_id, w.warehouse_name
ORDER BY w.warehouse_id;


-- ============================================================================
-- VERIFY: All Checks — Cross-Cutting Sanity
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_warehouses = 3
ASSERT VALUE total_stores = 15
ASSERT VALUE total_monthly_orders = 48400
ASSERT VALUE suboptimal_count = 1
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.logistics.warehouses) AS total_warehouses,
    (SELECT COUNT(*) FROM {{zone_name}}.logistics.stores) AS total_stores,
    (SELECT SUM(monthly_orders) FROM {{zone_name}}.logistics.stores) AS total_monthly_orders,
    (SELECT COUNT(*)
     FROM (
         SELECT
             s.store_id,
             s.warehouse_id AS assigned_wh,
             w_all.warehouse_id AS nearest_wh,
             ROW_NUMBER() OVER (PARTITION BY s.store_id ORDER BY st_distance(s.lat, s.lng, w_all.lat, w_all.lng)) AS rn
         FROM {{zone_name}}.logistics.stores s
         CROSS JOIN {{zone_name}}.logistics.warehouses w_all
     ) ranked
     WHERE rn = 1 AND nearest_wh != assigned_wh
    ) AS suboptimal_count;
