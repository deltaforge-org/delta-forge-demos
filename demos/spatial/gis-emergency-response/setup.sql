-- ==========================================================================
-- Demo: GIS Emergency Response Network
-- Feature: Complex multi-step GIS analytics for emergency dispatch
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables for demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.emergency
    COMMENT 'NYC emergency response — hospitals, incidents, response zones';
-- ==========================================================================
-- TABLE 1: hospitals — 8 NYC-area hospitals with trauma levels
-- ==========================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.emergency.hospitals (
    hospital_id    INT,
    hospital_name  VARCHAR,
    lat            DOUBLE,
    lng            DOUBLE,
    trauma_level   INT,
    bed_capacity   INT
) LOCATION 'gis-emergency-response/emergency_hospitals';


INSERT INTO {{zone_name}}.emergency.hospitals VALUES
    (1, 'Bellevue Hospital',       40.7390, -73.9750, 1,  828),
    (2, 'Mount Sinai Hospital',    40.7900, -73.9525, 1, 1134),
    (3, 'NYU Langone Medical',     40.7425, -73.9740, 1,  806),
    (4, 'Columbia Presbyterian',   40.8400, -73.9420, 1,  862),
    (5, 'Brooklyn Methodist',      40.6710, -73.9790, 2,  651),
    (6, 'Queens General',          40.7145, -73.8165, 2,  439),
    (7, 'Lenox Hill Hospital',     40.7736, -73.9620, 2,  652),
    (8, 'Staten Island Univ',      40.5830, -74.0960, 2,  714);

DETECT SCHEMA FOR TABLE {{zone_name}}.emergency.hospitals;


-- ==========================================================================
-- TABLE 2: incidents — 12 emergency calls across NYC boroughs
-- ==========================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.emergency.incidents (
    incident_id     INT,
    incident_type   VARCHAR,
    lat             DOUBLE,
    lng             DOUBLE,
    severity        VARCHAR,
    reported_at     VARCHAR
) LOCATION 'gis-emergency-response/emergency_incidents';


INSERT INTO {{zone_name}}.emergency.incidents VALUES
    (1,  'cardiac_arrest',       40.7480, -73.9855, 'critical', '2026-03-15 08:23:00'),
    (2,  'multi_vehicle_crash',  40.7300, -74.0000, 'critical', '2026-03-15 09:45:00'),
    (3,  'fall_injury',          40.7100, -73.9800, 'moderate', '2026-03-15 10:12:00'),
    (4,  'stroke',               40.7800, -73.9600, 'critical', '2026-03-15 11:30:00'),
    (5,  'burn_injury',          40.8200, -73.9500, 'severe',   '2026-03-15 12:05:00'),
    (6,  'allergic_reaction',    40.7900, -73.9700, 'moderate', '2026-03-15 13:18:00'),
    (7,  'gunshot_wound',        40.6500, -73.9500, 'critical', '2026-03-15 14:00:00'),
    (8,  'chest_pain',           40.6800, -73.9700, 'moderate', '2026-03-15 14:45:00'),
    (9,  'seizure',              40.7100, -73.8000, 'severe',   '2026-03-15 15:30:00'),
    (10, 'fracture',             40.7500, -73.7800, 'moderate', '2026-03-15 16:10:00'),
    (11, 'hypothermia',          40.5500, -74.1200, 'severe',   '2026-03-15 17:00:00'),
    (12, 'respiratory',          40.8600, -73.8700, 'moderate', '2026-03-15 18:20:00');

DETECT SCHEMA FOR TABLE {{zone_name}}.emergency.incidents;


-- ==========================================================================
-- TABLE 3: response_zones — 4 borough-level dispatch zones with WKT polygons
-- ==========================================================================
-- Each zone is an axis-aligned rectangle covering a borough area.
-- Used for st_contains/st_within geofencing tests.
-- ==========================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.emergency.response_zones (
    zone_id         INT,
    zone_name       VARCHAR,
    zone_polygon    VARCHAR,
    priority_level  INT
) LOCATION 'gis-emergency-response/emergency_zones';


INSERT INTO {{zone_name}}.emergency.response_zones VALUES
    (1, 'Downtown Manhattan', 'POLYGON((-74.02 40.70, -73.97 40.70, -73.97 40.76, -74.02 40.76, -74.02 40.70))', 1),
    (2, 'Midtown-Uptown',     'POLYGON((-74.02 40.76, -73.93 40.76, -73.93 40.85, -74.02 40.85, -74.02 40.76))', 1),
    (3, 'Brooklyn',            'POLYGON((-74.05 40.57, -73.85 40.57, -73.85 40.70, -74.05 40.70, -74.05 40.57))', 2),
    (4, 'Queens',              'POLYGON((-73.85 40.65, -73.70 40.65, -73.70 40.80, -73.85 40.80, -73.85 40.65))', 2);

DETECT SCHEMA FOR TABLE {{zone_name}}.emergency.response_zones;

