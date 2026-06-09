-- ==========================================================================
-- Demo: GIS Maritime Shipping — PostGIS-Compatible Geospatial Functions
-- Feature: All 18 st_* functions (distance, bearing, containment, area)
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.maritime
    COMMENT 'Maritime shipping — ports, vessels, and GPS positions';
-- ==========================================================================
-- TABLE 1: ports — 10 major world ports with harbor polygon boundaries
-- ==========================================================================
-- Each port has a known lat/lng and a ~2 km rectangular harbor polygon
-- (±0.01 degrees) for point-in-polygon testing with st_contains.
-- ==========================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.maritime.ports (
    port_id      INT,
    port_name    VARCHAR,
    country      VARCHAR,
    lat          DOUBLE,
    lng          DOUBLE,
    harbor_wkt   VARCHAR
) LOCATION 'gis-maritime-shipping/maritime_ports';


INSERT INTO {{zone_name}}.maritime.ports VALUES
    (1,  'Singapore',   'Singapore',    1.2644,   103.8200, 'POLYGON((103.81 1.2544, 103.83 1.2544, 103.83 1.2744, 103.81 1.2744, 103.81 1.2544))'),
    (2,  'Rotterdam',   'Netherlands',  51.9055,    4.4700, 'POLYGON((4.46 51.8955, 4.48 51.8955, 4.48 51.9155, 4.46 51.9155, 4.46 51.8955))'),
    (3,  'Shanghai',    'China',        31.3500,  121.5000, 'POLYGON((121.49 31.34, 121.51 31.34, 121.51 31.36, 121.49 31.36, 121.49 31.34))'),
    (4,  'Los Angeles', 'USA',          33.7400, -118.2600, 'POLYGON((-118.27 33.73, -118.25 33.73, -118.25 33.75, -118.27 33.75, -118.27 33.73))'),
    (5,  'Dubai',       'UAE',          25.2697,   55.3095, 'POLYGON((55.2995 25.2597, 55.3195 25.2597, 55.3195 25.2797, 55.2995 25.2797, 55.2995 25.2597))'),
    (6,  'Hamburg',     'Germany',      53.5400,    9.9700, 'POLYGON((9.96 53.53, 9.98 53.53, 9.98 53.55, 9.96 53.55, 9.96 53.53))'),
    (7,  'Busan',       'South Korea',  35.1000,  129.0300, 'POLYGON((129.02 35.09, 129.04 35.09, 129.04 35.11, 129.02 35.11, 129.02 35.09))'),
    (8,  'Santos',      'Brazil',      -23.9500,  -46.3300, 'POLYGON((-46.34 -23.96, -46.32 -23.96, -46.32 -23.94, -46.34 -23.94, -46.34 -23.96))'),
    (9,  'Yokohama',    'Japan',        35.4500,  139.6400, 'POLYGON((139.63 35.44, 139.65 35.44, 139.65 35.46, 139.63 35.46, 139.63 35.44))'),
    (10, 'Felixstowe',  'UK',           51.9500,    1.3000, 'POLYGON((1.29 51.94, 1.31 51.94, 1.31 51.96, 1.29 51.96, 1.29 51.94))');

-- ==========================================================================
-- TABLE 2: vessels — 5 cargo vessels with metadata
-- ==========================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.maritime.vessels (
    vessel_id      INT,
    vessel_name    VARCHAR,
    vessel_type    VARCHAR,
    flag_country   VARCHAR,
    deadweight_tons INT
) LOCATION 'gis-maritime-shipping/maritime_vessels';


INSERT INTO {{zone_name}}.maritime.vessels VALUES
    (1, 'MV Pacific Star',    'Container Ship', 'Panama',           65000),
    (2, 'MV Atlantic Runner', 'Bulk Carrier',   'Liberia',          82000),
    (3, 'MV Indian Voyager',  'Tanker',         'Marshall Islands', 120000),
    (4, 'MV Nordic Spirit',   'Container Ship', 'Singapore',        58000),
    (5, 'MV Southern Cross',  'Cargo',          'Greece',           45000);

-- ==========================================================================
-- TABLE 3: positions — 40 GPS position reports (8 per vessel)
-- ==========================================================================
-- Each vessel has a route: departure port -> waypoints at sea -> arrival port.
-- Positions at port have speed_knots = 0 and coordinates matching a port.
-- ==========================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.maritime.positions (
    position_id  INT,
    vessel_id    INT,
    lat          DOUBLE,
    lng          DOUBLE,
    speed_knots  DOUBLE,
    recorded_at  VARCHAR
) LOCATION 'gis-maritime-shipping/maritime_positions';


INSERT INTO {{zone_name}}.maritime.positions VALUES
    -- Vessel 1 (Pacific Star): Singapore -> Shanghai
    (1,  1,   1.2644,  103.8200,  0.0, '2025-03-01 08:00:00'),
    (2,  1,   1.5000,  104.5000, 14.2, '2025-03-01 14:00:00'),
    (3,  1,   5.5000,  108.0000, 15.1, '2025-03-02 08:00:00'),
    (4,  1,  12.0000,  112.5000, 14.8, '2025-03-03 08:00:00'),
    (5,  1,  18.5000,  116.0000, 15.3, '2025-03-04 08:00:00'),
    (6,  1,  24.0000,  118.5000, 14.5, '2025-03-05 08:00:00'),
    (7,  1,  29.0000,  120.5000, 13.9, '2025-03-06 08:00:00'),
    (8,  1,  31.3500,  121.5000,  0.0, '2025-03-07 06:00:00'),

    -- Vessel 2 (Atlantic Runner): Rotterdam -> Felixstowe
    (9,  2,  51.9055,    4.4700,  0.0, '2025-03-01 06:00:00'),
    (10, 2,  51.9200,    4.2000, 10.5, '2025-03-01 08:00:00'),
    (11, 2,  51.9300,    3.5000, 11.2, '2025-03-01 12:00:00'),
    (12, 2,  51.9400,    2.8000, 10.8, '2025-03-01 16:00:00'),
    (13, 2,  51.9450,    2.1000, 11.0, '2025-03-01 20:00:00'),
    (14, 2,  51.9480,    1.5000, 10.3, '2025-03-02 00:00:00'),
    (15, 2,  51.9500,    1.3000,  0.0, '2025-03-02 04:00:00'),
    (16, 2,  51.9500,    1.3000,  0.0, '2025-03-02 08:00:00'),

    -- Vessel 3 (Indian Voyager): Dubai -> Singapore
    (17, 3,  25.2697,   55.3095,  0.0, '2025-03-01 10:00:00'),
    (18, 3,  24.5000,   56.0000, 13.0, '2025-03-01 18:00:00'),
    (19, 3,  20.0000,   62.0000, 14.5, '2025-03-03 08:00:00'),
    (20, 3,  15.0000,   70.0000, 15.0, '2025-03-05 08:00:00'),
    (21, 3,  10.0000,   78.0000, 14.8, '2025-03-07 08:00:00'),
    (22, 3,   5.0000,   86.0000, 15.2, '2025-03-09 08:00:00'),
    (23, 3,   2.5000,   95.0000, 14.0, '2025-03-11 08:00:00'),
    (24, 3,   1.2644,  103.8200,  0.0, '2025-03-13 06:00:00'),

    -- Vessel 4 (Nordic Spirit): Santos -> Los Angeles
    (25, 4, -23.9500,  -46.3300,  0.0, '2025-03-01 07:00:00'),
    (26, 4, -22.0000,  -47.0000, 16.0, '2025-03-01 18:00:00'),
    (27, 4, -15.0000,  -55.0000, 15.5, '2025-03-04 08:00:00'),
    (28, 4,  -5.0000,  -65.0000, 16.2, '2025-03-07 08:00:00'),
    (29, 4,   5.0000,  -78.0000, 15.8, '2025-03-10 08:00:00'),
    (30, 4,  15.0000,  -92.0000, 14.5, '2025-03-13 08:00:00'),
    (31, 4,  25.0000, -105.0000, 15.0, '2025-03-16 08:00:00'),
    (32, 4,  33.7400, -118.2600,  0.0, '2025-03-19 06:00:00'),

    -- Vessel 5 (Southern Cross): Busan -> Yokohama
    (33, 5,  35.1000,  129.0300,  0.0, '2025-03-01 05:00:00'),
    (34, 5,  35.2000,  130.0000, 12.0, '2025-03-01 10:00:00'),
    (35, 5,  35.3000,  132.0000, 13.5, '2025-03-01 20:00:00'),
    (36, 5,  35.3500,  134.0000, 12.8, '2025-03-02 06:00:00'),
    (37, 5,  35.4000,  136.0000, 13.0, '2025-03-02 16:00:00'),
    (38, 5,  35.4200,  137.5000, 12.5, '2025-03-03 02:00:00'),
    (39, 5,  35.4400,  139.0000, 11.0, '2025-03-03 12:00:00'),
    (40, 5,  35.4500,  139.6400,  0.0, '2025-03-03 18:00:00');

