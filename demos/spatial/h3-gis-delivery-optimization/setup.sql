-- ==========================================================================
-- Demo: H3+GIS Delivery Optimization — Cross-Function Spatial Analytics
-- Feature: Combines H3 hexagonal indexing with GIS distance/bearing functions
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables for demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.logistics
    COMMENT 'Delivery logistics — warehouses, stores, delivery analytics';


-- ==========================================================================
-- TABLE 1: warehouses — 3 regional distribution centers
-- ==========================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.logistics.warehouses (
    warehouse_id      INT,
    warehouse_name    VARCHAR,
    lat               DOUBLE,
    lng               DOUBLE,
    capacity_pallets  INT
) LOCATION 'h3-gis-delivery-optimization/logistics_warehouses';


INSERT INTO {{zone_name}}.logistics.warehouses VALUES
    (1, 'Chicago Distribution Center', 41.8781, -87.6298, 50000),
    (2, 'Dallas Fulfillment Hub',      32.7767, -96.7970, 35000),
    (3, 'Atlanta Logistics Park',      33.7490, -84.3880, 40000);

DETECT SCHEMA FOR TABLE {{zone_name}}.logistics.warehouses;


-- ==========================================================================
-- TABLE 2: stores — 15 retail stores across the US Midwest/South
-- ==========================================================================
-- Each store is assigned to a warehouse. Store 10 (Memphis) is intentionally
-- assigned to Dallas (WH2) despite Atlanta (WH3) being closer, to test
-- suboptimal assignment detection queries.
-- ==========================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.logistics.stores (
    store_id        INT,
    store_name      VARCHAR,
    lat             DOUBLE,
    lng             DOUBLE,
    warehouse_id    INT,
    monthly_orders  INT
) LOCATION 'h3-gis-delivery-optimization/logistics_stores';


INSERT INTO {{zone_name}}.logistics.stores VALUES
    (1,  'Milwaukee Store',      43.0389, -87.9065,  1, 2800),
    (2,  'Indianapolis Store',   39.7684, -86.1581,  1, 3200),
    (3,  'Detroit Store',        42.3314, -83.0458,  1, 4100),
    (4,  'Minneapolis Store',    44.9778, -93.2650,  1, 2500),
    (5,  'St Louis Store',       38.6270, -90.1994,  1, 3600),
    (6,  'Houston Store',        29.7604, -95.3698,  2, 5200),
    (7,  'San Antonio Store',    29.4241, -98.4936,  2, 2900),
    (8,  'Austin Store',         30.2672, -97.7431,  2, 3400),
    (9,  'Oklahoma City Store',  35.4676, -97.5164,  2, 2100),
    (10, 'Memphis Store',        35.1495, -90.0490,  2, 2700),
    (11, 'Nashville Store',      36.1627, -86.7816,  3, 3800),
    (12, 'Charlotte Store',      35.2271, -80.8431,  3, 4500),
    (13, 'Jacksonville Store',   30.3322, -81.6557,  3, 2600),
    (14, 'Birmingham Store',     33.5186, -86.8104,  3, 1900),
    (15, 'Raleigh Store',        35.7796, -78.6382,  3, 3100);

DETECT SCHEMA FOR TABLE {{zone_name}}.logistics.stores;
