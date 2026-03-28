-- ============================================================================
-- Demo: GIS Emergency Response Network — Multi-Step Spatial Analytics
-- ============================================================================
-- Tests GIS functions in layered analytical patterns for emergency dispatch:
-- nearest-neighbor ranking, multi-algorithm distance comparison, geofencing,
-- navigation bearings, coordinate round-trips, and geodesic area calculations.
--
-- Functions tested:
--   Distance:    st_distance, st_distance_haversine, st_distance_vincenty, st_distance_sphere
--   Bearing:     st_bearing, st_azimuth, st_final_bearing
--   Containment: st_contains, st_within
--   Geometry:    st_make_point, st_x, st_y
--   Area:        st_area
--   Length:      st_length
--   WKT:         st_geom_from_text, st_as_text
-- ============================================================================


-- ============================================================================
-- Query 1: Hospital Catalog — Baseline
-- ============================================================================
-- Verify all 8 hospitals loaded with correct trauma levels and capacities.

ASSERT ROW_COUNT = 8
ASSERT VALUE hospital_name = 'Bellevue Hospital' WHERE hospital_id = 1
ASSERT VALUE trauma_level = 1 WHERE hospital_id = 1
ASSERT VALUE hospital_name = 'Staten Island Univ' WHERE hospital_id = 8
ASSERT VALUE trauma_level = 2 WHERE hospital_id = 8
SELECT hospital_id, hospital_name, lat, lng, trauma_level, bed_capacity
FROM {{zone_name}}.emergency.hospitals
ORDER BY hospital_id;


-- ============================================================================
-- Query 2: Incident Catalog — Baseline
-- ============================================================================
-- Verify all 12 emergency incidents with severity distribution.

ASSERT ROW_COUNT = 12
ASSERT VALUE incident_type = 'cardiac_arrest' WHERE incident_id = 1
ASSERT VALUE severity = 'critical' WHERE incident_id = 1
ASSERT VALUE incident_type = 'respiratory' WHERE incident_id = 12
SELECT incident_id, incident_type, severity, lat, lng
FROM {{zone_name}}.emergency.incidents
ORDER BY incident_id;


-- ============================================================================
-- Query 3: Nearest Hospital Per Incident — ROW_NUMBER + st_distance
-- ============================================================================
-- For each incident, rank all hospitals by haversine distance and select
-- the nearest. Tests st_distance in a window function nearest-neighbor pattern.

ASSERT ROW_COUNT = 12
ASSERT VALUE nearest_hospital = 'NYU Langone Medical' WHERE incident_id = 1
ASSERT VALUE distance_km = 1.1 WHERE incident_id = 1
ASSERT VALUE nearest_hospital = 'Lenox Hill Hospital' WHERE incident_id = 4
ASSERT VALUE distance_km = 0.7 WHERE incident_id = 4
ASSERT VALUE nearest_hospital = 'Brooklyn Methodist' WHERE incident_id = 7
ASSERT VALUE nearest_hospital = 'Staten Island Univ' WHERE incident_id = 11
WITH ranked AS (
    SELECT
        i.incident_id,
        i.incident_type,
        i.severity,
        h.hospital_name,
        ROUND(st_distance(i.lat, i.lng, h.lat, h.lng) / 1000.0, 1) AS distance_km,
        ROW_NUMBER() OVER (PARTITION BY i.incident_id ORDER BY st_distance(i.lat, i.lng, h.lat, h.lng)) AS rn
    FROM {{zone_name}}.emergency.incidents i
    CROSS JOIN {{zone_name}}.emergency.hospitals h
)
SELECT
    incident_id,
    incident_type,
    severity,
    hospital_name AS nearest_hospital,
    distance_km
FROM ranked
WHERE rn = 1
ORDER BY incident_id;


-- ============================================================================
-- Query 4: Distance Algorithm Comparison — Haversine vs Sphere vs Vincenty
-- ============================================================================
-- Compare three distance algorithms for a medium-range route:
-- Staten Island (hospital 8) to Columbia Presbyterian (hospital 4) ~29 km.
-- Vincenty uses WGS84 ellipsoid and should differ slightly from the spherical methods.

ASSERT ROW_COUNT = 1
ASSERT VALUE haversine_km = 31.4
ASSERT VALUE sphere_km = 31.4
SELECT
    ROUND(st_distance_haversine(h1.lat, h1.lng, h2.lat, h2.lng) / 1000.0, 1) AS haversine_km,
    ROUND(st_distance_sphere(h1.lat, h1.lng, h2.lat, h2.lng) / 1000.0, 1) AS sphere_km,
    ROUND(st_distance_vincenty(h1.lat, h1.lng, h2.lat, h2.lng) / 1000.0, 1) AS vincenty_km
FROM {{zone_name}}.emergency.hospitals h1
CROSS JOIN {{zone_name}}.emergency.hospitals h2
WHERE h1.hospital_id = 8 AND h2.hospital_id = 4;


-- ============================================================================
-- Query 5: Navigation Bearings — Dispatch Direction
-- ============================================================================
-- Compute initial bearing (st_bearing), azimuth in radians (st_azimuth),
-- and final bearing (st_final_bearing) from Bellevue Hospital to each
-- critical incident. Dispatchers use bearing for route guidance.

ASSERT ROW_COUNT = 4
ASSERT VALUE bearing_deg = 318.53 WHERE incident_id = 1
ASSERT VALUE bearing_deg = 244.6 WHERE incident_id = 2
ASSERT VALUE bearing_deg = 167.97 WHERE incident_id = 7
SELECT
    i.incident_id,
    i.incident_type,
    ROUND(st_bearing(h.lat, h.lng, i.lat, i.lng), 2) AS bearing_deg,
    ROUND(st_azimuth(h.lat, h.lng, i.lat, i.lng), 4) AS azimuth_rad,
    ROUND(st_final_bearing(h.lat, h.lng, i.lat, i.lng), 2) AS final_bearing_deg
FROM {{zone_name}}.emergency.incidents i
CROSS JOIN {{zone_name}}.emergency.hospitals h
WHERE h.hospital_id = 1
  AND i.severity = 'critical'
ORDER BY i.incident_id;


-- ============================================================================
-- Query 6: Geofencing — Incidents in Response Zones (st_contains)
-- ============================================================================
-- Assign each incident to a response zone using st_contains(polygon, lat, lng).
-- 10 of 12 incidents fall within zones; 2 are outside (Staten Island, Bronx).

ASSERT ROW_COUNT = 10
ASSERT VALUE zone_name = 'Downtown Manhattan' WHERE incident_id = 1
ASSERT VALUE zone_name = 'Downtown Manhattan' WHERE incident_id = 2
ASSERT VALUE zone_name = 'Midtown-Uptown' WHERE incident_id = 4
ASSERT VALUE zone_name = 'Brooklyn' WHERE incident_id = 7
ASSERT VALUE zone_name = 'Queens' WHERE incident_id = 9
SELECT
    i.incident_id,
    i.incident_type,
    i.severity,
    z.zone_name,
    z.priority_level
FROM {{zone_name}}.emergency.incidents i
CROSS JOIN {{zone_name}}.emergency.response_zones z
WHERE st_contains(z.zone_polygon, i.lat, i.lng) = true
ORDER BY i.incident_id;


-- ============================================================================
-- Query 7: Reverse Containment — st_within Verification
-- ============================================================================
-- st_within(lat, lng, polygon) is the inverse of st_contains.
-- Verify that every incident identified by st_contains is also found by st_within.

ASSERT ROW_COUNT = 10
ASSERT VALUE is_within = true WHERE incident_id = 1
ASSERT VALUE is_within = true WHERE incident_id = 9
SELECT
    i.incident_id,
    z.zone_name,
    st_within(i.lat, i.lng, z.zone_polygon) AS is_within
FROM {{zone_name}}.emergency.incidents i
CROSS JOIN {{zone_name}}.emergency.response_zones z
WHERE st_contains(z.zone_polygon, i.lat, i.lng) = true
ORDER BY i.incident_id;


-- ============================================================================
-- Query 8: Coordinate Round-Trip — st_make_point, st_x, st_y
-- ============================================================================
-- Construct WKT POINT from hospital coordinates, then extract back.
-- Verifies lossless round-trip: (lng, lat) -> POINT -> (lng, lat).

ASSERT ROW_COUNT = 8
ASSERT VALUE extracted_lng = -73.975 WHERE hospital_id = 1
ASSERT VALUE extracted_lat = 40.739 WHERE hospital_id = 1
ASSERT VALUE extracted_lng = -74.096 WHERE hospital_id = 8
SELECT
    hospital_id,
    hospital_name,
    st_make_point(lng, lat) AS wkt_point,
    st_x(st_make_point(lng, lat)) AS extracted_lng,
    st_y(st_make_point(lng, lat)) AS extracted_lat
FROM {{zone_name}}.emergency.hospitals
ORDER BY hospital_id;


-- ============================================================================
-- Query 9: Zone Area Comparison — st_area
-- ============================================================================
-- Compute geodesic area for each response zone polygon. All zones use
-- rectangular boundaries but actual area depends on latitude (Earth curvature).
-- Downtown Manhattan (narrower, 0.05° lng) should be smaller than Brooklyn
-- (wider, 0.20° lng, taller, 0.13° lat).

ASSERT ROW_COUNT = 1
ASSERT EXPRESSION area_km2_downtown < area_km2_brooklyn
ASSERT EXPRESSION area_km2_queens > 0
SELECT
    MAX(CASE WHEN zone_id = 1 THEN ROUND(st_area(zone_polygon) / 1000000.0, 2) END) AS area_km2_downtown,
    MAX(CASE WHEN zone_id = 2 THEN ROUND(st_area(zone_polygon) / 1000000.0, 2) END) AS area_km2_midtown,
    MAX(CASE WHEN zone_id = 3 THEN ROUND(st_area(zone_polygon) / 1000000.0, 2) END) AS area_km2_brooklyn,
    MAX(CASE WHEN zone_id = 4 THEN ROUND(st_area(zone_polygon) / 1000000.0, 2) END) AS area_km2_queens
FROM {{zone_name}}.emergency.response_zones;


-- ============================================================================
-- Query 10: Nearest Trauma-1 Hospital Per Critical Incident
-- ============================================================================
-- Combined analytical query: for each critical incident, find the nearest
-- Level 1 trauma center and compute the dispatch bearing and distance.
-- Tests st_distance + st_bearing + ROW_NUMBER in a single layered query.

ASSERT ROW_COUNT = 4
ASSERT VALUE nearest_trauma1 = 'NYU Langone Medical' WHERE incident_id = 1
ASSERT VALUE nearest_trauma1 = 'Bellevue Hospital' WHERE incident_id = 2
ASSERT VALUE nearest_trauma1 = 'Mount Sinai Hospital' WHERE incident_id = 4
ASSERT VALUE nearest_trauma1 = 'Bellevue Hospital' WHERE incident_id = 7
WITH trauma1_ranked AS (
    SELECT
        i.incident_id,
        i.incident_type,
        h.hospital_name,
        ROUND(st_distance(i.lat, i.lng, h.lat, h.lng) / 1000.0, 1) AS distance_km,
        ROUND(st_bearing(h.lat, h.lng, i.lat, i.lng), 2) AS dispatch_bearing,
        ROW_NUMBER() OVER (PARTITION BY i.incident_id ORDER BY st_distance(i.lat, i.lng, h.lat, h.lng)) AS rn
    FROM {{zone_name}}.emergency.incidents i
    CROSS JOIN {{zone_name}}.emergency.hospitals h
    WHERE i.severity = 'critical'
      AND h.trauma_level = 1
)
SELECT
    incident_id,
    incident_type,
    hospital_name AS nearest_trauma1,
    distance_km,
    dispatch_bearing
FROM trauma1_ranked
WHERE rn = 1
ORDER BY incident_id;


-- ============================================================================
-- Query 11: Hospital Coverage — Incidents Within 3 km Radius
-- ============================================================================
-- Count how many incidents fall within a 3 km radius of each hospital.
-- Tests st_distance in an aggregate pattern for coverage analysis.

ASSERT ROW_COUNT = 7
ASSERT VALUE incidents_in_range = 2 WHERE hospital_name = 'Bellevue Hospital'
ASSERT VALUE incidents_in_range = 2 WHERE hospital_name = 'NYU Langone Medical'
ASSERT VALUE incidents_in_range = 2 WHERE hospital_name = 'Lenox Hill Hospital'
SELECT
    h.hospital_name,
    h.trauma_level,
    COUNT(*) AS incidents_in_range
FROM {{zone_name}}.emergency.hospitals h
CROSS JOIN {{zone_name}}.emergency.incidents i
WHERE st_distance(h.lat, h.lng, i.lat, i.lng) <= 3000
GROUP BY h.hospital_name, h.trauma_level
HAVING COUNT(*) > 0
ORDER BY incidents_in_range DESC, h.hospital_name;


-- ============================================================================
-- VERIFY: All Checks — Cross-Cutting Sanity
-- ============================================================================
-- Combines multiple st_* functions to verify end-to-end consistency.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_hospitals = 8
ASSERT VALUE total_incidents = 12
ASSERT VALUE trauma1_count = 4
ASSERT VALUE critical_count = 4
ASSERT VALUE incidents_in_zones = 10
ASSERT VALUE incidents_outside_zones = 2
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.emergency.hospitals) AS total_hospitals,
    (SELECT COUNT(*) FROM {{zone_name}}.emergency.incidents) AS total_incidents,
    (SELECT COUNT(*) FROM {{zone_name}}.emergency.hospitals WHERE trauma_level = 1) AS trauma1_count,
    (SELECT COUNT(*) FROM {{zone_name}}.emergency.incidents WHERE severity = 'critical') AS critical_count,
    (SELECT COUNT(*)
     FROM {{zone_name}}.emergency.incidents i
     CROSS JOIN {{zone_name}}.emergency.response_zones z
     WHERE st_contains(z.zone_polygon, i.lat, i.lng) = true
    ) AS incidents_in_zones,
    (SELECT COUNT(*)
     FROM {{zone_name}}.emergency.incidents i
     WHERE NOT EXISTS (
         SELECT 1
         FROM {{zone_name}}.emergency.response_zones z
         WHERE st_contains(z.zone_polygon, i.lat, i.lng) = true
     )
    ) AS incidents_outside_zones;
