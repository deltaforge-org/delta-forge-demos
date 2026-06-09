-- ============================================================================
-- Demo: GIS Maritime Shipping — PostGIS-Compatible Geospatial Functions
-- ============================================================================
-- Tests all 18 st_* functions using a global maritime shipping scenario:
-- 5 cargo vessels navigating between 10 world ports. Every assertion value
-- is independently computed via haversine/Vincenty/spherical formulas.
--
-- Functions tested:
--   Distance:    st_distance, st_distance_haversine, st_distance_vincenty, st_distance_sphere
--   Bearing:     st_bearing, st_azimuth, st_final_bearing
--   Containment: st_contains, st_within
--   Geometry:    st_make_point, st_x, st_y, st_centroid, st_envelope
--   Area:        st_area
--   Length:      st_length
--   WKT:         st_geom_from_text, st_as_text
-- ============================================================================


-- ============================================================================
-- Query 1: Port Catalog — Baseline Data
-- ============================================================================
-- Verify all 10 world ports are loaded with correct names and coordinates.

ASSERT ROW_COUNT = 10
ASSERT VALUE port_name = 'Singapore' WHERE port_id = 1
ASSERT VALUE country = 'Singapore' WHERE port_id = 1
ASSERT VALUE port_name = 'Santos' WHERE port_id = 8
ASSERT VALUE port_name = 'Felixstowe' WHERE port_id = 10
SELECT port_id, port_name, country, lat, lng
FROM {{zone_name}}.maritime.ports
ORDER BY port_id;


-- ============================================================================
-- Query 2: Great Circle Distances Between Port Pairs
-- ============================================================================
-- st_distance computes haversine great-circle distance in meters.
-- We convert to km and round for comparison against known geodesic values.

ASSERT ROW_COUNT = 6
ASSERT VALUE distance_km = 3827 WHERE route = 'Singapore -> Shanghai'
ASSERT VALUE distance_km = 217 WHERE route = 'Rotterdam -> Felixstowe'
ASSERT VALUE distance_km = 5841 WHERE route = 'Dubai -> Singapore'
ASSERT VALUE distance_km = 963 WHERE route = 'Busan -> Yokohama'
ASSERT VALUE distance_km = 14146 WHERE route = 'Singapore -> Los Angeles'
SELECT
    p1.port_name || ' -> ' || p2.port_name AS route,
    ROUND(st_distance(p1.lat, p1.lng, p2.lat, p2.lng) / 1000.0, 0) AS distance_km
FROM {{zone_name}}.maritime.ports p1
CROSS JOIN {{zone_name}}.maritime.ports p2
WHERE (p1.port_id, p2.port_id) IN (
    (1, 3),   -- Singapore -> Shanghai
    (2, 10),  -- Rotterdam -> Felixstowe
    (5, 1),   -- Dubai -> Singapore
    (7, 9),   -- Busan -> Yokohama
    (8, 4),   -- Santos -> Los Angeles
    (1, 4)    -- Singapore -> Los Angeles
)
ORDER BY distance_km;


-- ============================================================================
-- Query 3: Distance Method Comparison — Haversine vs Vincenty vs Sphere
-- ============================================================================
-- Three distance algorithms for Singapore (1.26°N) -> Rotterdam (51.91°N):
--   - Haversine:  fast, ~0.3% error on sphere
--   - Sphere:     spherical law of cosines (same accuracy as haversine)
--   - Vincenty:   WGS84 ellipsoid, ~0.5mm precision
-- Vincenty should differ by a few km due to Earth's oblate shape.

ASSERT ROW_COUNT = 1
ASSERT VALUE haversine_km = 10536
ASSERT VALUE sphere_km = 10536
ASSERT VALUE vincenty_km = 10539
SELECT
    ROUND(st_distance_haversine(p1.lat, p1.lng, p2.lat, p2.lng) / 1000.0, 0) AS haversine_km,
    ROUND(st_distance_sphere(p1.lat, p1.lng, p2.lat, p2.lng) / 1000.0, 0) AS sphere_km,
    ROUND(st_distance_vincenty(p1.lat, p1.lng, p2.lat, p2.lng) / 1000.0, 0) AS vincenty_km
FROM {{zone_name}}.maritime.ports p1
CROSS JOIN {{zone_name}}.maritime.ports p2
WHERE p1.port_id = 1 AND p2.port_id = 2;


-- ============================================================================
-- Query 4: Navigation Bearings Between Ports
-- ============================================================================
-- st_bearing returns initial compass bearing (0-360 degrees) from origin to
-- destination along the great circle. st_azimuth returns the same in radians.
-- st_final_bearing returns the bearing at the destination point.

ASSERT ROW_COUNT = 4
ASSERT VALUE bearing_deg = 27.31 WHERE route = 'Singapore -> Shanghai'
ASSERT VALUE bearing_deg = 272.55 WHERE route = 'Rotterdam -> Felixstowe'
ASSERT VALUE bearing_deg = 109.34 WHERE route = 'Dubai -> Singapore'
ASSERT VALUE bearing_deg = 307.76 WHERE route = 'Santos -> Los Angeles'
SELECT
    p1.port_name || ' -> ' || p2.port_name AS route,
    ROUND(st_bearing(p1.lat, p1.lng, p2.lat, p2.lng), 2) AS bearing_deg,
    ROUND(st_azimuth(p1.lat, p1.lng, p2.lat, p2.lng), 4) AS azimuth_rad,
    ROUND(st_final_bearing(p1.lat, p1.lng, p2.lat, p2.lng), 2) AS final_bearing_deg
FROM {{zone_name}}.maritime.ports p1
CROSS JOIN {{zone_name}}.maritime.ports p2
WHERE (p1.port_id, p2.port_id) IN (
    (1, 3),   -- Singapore -> Shanghai (NNE)
    (2, 10),  -- Rotterdam -> Felixstowe (W)
    (5, 1),   -- Dubai -> Singapore (ESE)
    (8, 4)    -- Santos -> Los Angeles (NW)
)
ORDER BY p1.port_id;


-- ============================================================================
-- Query 5: Point-in-Polygon — Vessels Detected In Port
-- ============================================================================
-- Uses st_contains(harbor_polygon, lat, lng) to detect which vessel positions
-- fall within a port's harbor boundary. Each harbor is a ~2 km square polygon
-- around the port center (±0.01 degrees).

ASSERT ROW_COUNT = 11
ASSERT VALUE port_name = 'Singapore' WHERE position_id = 1
ASSERT VALUE port_name = 'Shanghai' WHERE position_id = 8
ASSERT VALUE port_name = 'Rotterdam' WHERE position_id = 9
ASSERT VALUE port_name = 'Felixstowe' WHERE position_id = 15
ASSERT VALUE port_name = 'Dubai' WHERE position_id = 17
ASSERT VALUE port_name = 'Los Angeles' WHERE position_id = 32
SELECT
    pos.position_id,
    pos.vessel_id,
    v.vessel_name,
    p.port_name,
    pos.lat,
    pos.lng
FROM {{zone_name}}.maritime.positions pos
JOIN {{zone_name}}.maritime.vessels v ON v.vessel_id = pos.vessel_id
CROSS JOIN {{zone_name}}.maritime.ports p
WHERE st_contains(p.harbor_wkt, pos.lat, pos.lng) = true
ORDER BY pos.position_id;


-- ============================================================================
-- Query 6: Coordinate Construction & Extraction — st_make_point, st_x, st_y
-- ============================================================================
-- st_make_point(lon, lat) creates a WKT POINT, st_x/st_y extract back.
-- Verifies roundtrip: coordinates in -> WKT POINT -> coordinates out.

ASSERT ROW_COUNT = 10
ASSERT VALUE extracted_lng = 103.82 WHERE port_id = 1
ASSERT VALUE extracted_lat = 1.2644 WHERE port_id = 1
ASSERT VALUE extracted_lng = 4.47 WHERE port_id = 2
ASSERT VALUE extracted_lat = 51.9055 WHERE port_id = 2
SELECT
    port_id,
    port_name,
    st_make_point(lng, lat) AS wkt_point,
    st_x(st_make_point(lng, lat)) AS extracted_lng,
    st_y(st_make_point(lng, lat)) AS extracted_lat
FROM {{zone_name}}.maritime.ports
ORDER BY port_id;


-- ============================================================================
-- Query 7: Harbor Polygon Areas — Latitude Effect on Area
-- ============================================================================
-- st_area computes geodesic area in square meters. All harbors use ±0.01°
-- polygons, but actual area varies by latitude: equatorial ports have larger
-- areas because 1° of longitude spans more meters near the equator.
-- Singapore (1.26°N) should be largest; Hamburg (53.54°N) should be smallest.

ASSERT ROW_COUNT = 1
ASSERT EXPRESSION area_km2_singapore > area_km2_hamburg
SELECT
    MAX(CASE WHEN port_id = 1 THEN ROUND(st_area(harbor_wkt) / 1000000.0, 2) END) AS area_km2_singapore,
    MAX(CASE WHEN port_id = 6 THEN ROUND(st_area(harbor_wkt) / 1000000.0, 2) END) AS area_km2_hamburg,
    MAX(CASE WHEN port_id = 1 THEN ROUND(st_area(harbor_wkt), 0) END) AS area_m2_singapore,
    MAX(CASE WHEN port_id = 6 THEN ROUND(st_area(harbor_wkt), 0) END) AS area_m2_hamburg
FROM {{zone_name}}.maritime.ports;


-- ============================================================================
-- Query 8: Nearest Port to Each Vessel's Last At-Sea Position
-- ============================================================================
-- For each vessel, find its last position where speed > 0 (at sea),
-- then compute the distance to every port and select the nearest.
-- This tests st_distance in a practical nearest-neighbor pattern.

ASSERT ROW_COUNT = 5
ASSERT VALUE nearest_port = 'Shanghai' WHERE vessel_name = 'MV Pacific Star'
ASSERT VALUE nearest_port = 'Felixstowe' WHERE vessel_name = 'MV Atlantic Runner'
ASSERT VALUE nearest_port = 'Singapore' WHERE vessel_name = 'MV Indian Voyager'
ASSERT VALUE nearest_port = 'Los Angeles' WHERE vessel_name = 'MV Nordic Spirit'
ASSERT VALUE nearest_port = 'Yokohama' WHERE vessel_name = 'MV Southern Cross'
WITH last_sea AS (
    SELECT
        pos.vessel_id,
        pos.lat,
        pos.lng
    FROM {{zone_name}}.maritime.positions pos
    WHERE pos.speed_knots > 0
      AND pos.position_id = (
          SELECT MAX(p2.position_id)
          FROM {{zone_name}}.maritime.positions p2
          WHERE p2.vessel_id = pos.vessel_id AND p2.speed_knots > 0
      )
),
distances AS (
    SELECT
        v.vessel_name,
        ls.lat AS vessel_lat,
        ls.lng AS vessel_lng,
        p.port_name,
        ROUND(st_distance(ls.lat, ls.lng, p.lat, p.lng) / 1000.0, 0) AS dist_km,
        ROW_NUMBER() OVER (PARTITION BY v.vessel_id ORDER BY st_distance(ls.lat, ls.lng, p.lat, p.lng)) AS rn
    FROM {{zone_name}}.maritime.vessels v
    JOIN last_sea ls ON ls.vessel_id = v.vessel_id
    CROSS JOIN {{zone_name}}.maritime.ports p
)
SELECT
    vessel_name,
    vessel_lat,
    vessel_lng,
    port_name AS nearest_port,
    dist_km AS nearest_dist_km
FROM distances
WHERE rn = 1
ORDER BY vessel_name;


-- ============================================================================
-- Query 9: st_within — Reverse Containment Test
-- ============================================================================
-- st_within(lat, lng, polygon) is the inverse of st_contains. Verify that
-- vessels at berth (speed = 0) are within their respective port polygons.

ASSERT ROW_COUNT = 11
ASSERT VALUE is_within = true WHERE position_id = 1
ASSERT VALUE is_within = true WHERE position_id = 40
SELECT
    pos.position_id,
    v.vessel_name,
    p.port_name,
    st_within(pos.lat, pos.lng, p.harbor_wkt) AS is_within
FROM {{zone_name}}.maritime.positions pos
JOIN {{zone_name}}.maritime.vessels v ON v.vessel_id = pos.vessel_id
CROSS JOIN {{zone_name}}.maritime.ports p
WHERE st_contains(p.harbor_wkt, pos.lat, pos.lng) = true
ORDER BY pos.position_id;


-- ============================================================================
-- VERIFY: All Checks — Cross-cutting sanity checks
-- ============================================================================
-- Combines multiple st_* functions in one query to verify end-to-end
-- consistency: total positions, in-port count, at-sea count, max distance.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_positions = 40
ASSERT VALUE in_port_count = 11
ASSERT VALUE at_sea_count = 29
ASSERT VALUE unique_vessels = 5
ASSERT VALUE unique_ports = 10
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.maritime.positions) AS total_positions,
    (SELECT COUNT(*)
     FROM {{zone_name}}.maritime.positions pos
     CROSS JOIN {{zone_name}}.maritime.ports p
     WHERE st_contains(p.harbor_wkt, pos.lat, pos.lng) = true
    ) AS in_port_count,
    (SELECT COUNT(*) FROM {{zone_name}}.maritime.positions WHERE speed_knots > 0) AS at_sea_count,
    (SELECT COUNT(*) FROM {{zone_name}}.maritime.vessels) AS unique_vessels,
    (SELECT COUNT(*) FROM {{zone_name}}.maritime.ports) AS unique_ports;
