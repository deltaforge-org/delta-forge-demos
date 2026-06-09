-- ============================================================================
-- Delta Table Properties — Configuration Lifecycle — Setup Script
-- ============================================================================
-- Demonstrates the full TBLPROPERTIES lifecycle: CREATE with properties,
-- SHOW TABLE PROPERTIES, ALTER SET/UNSET, and observing effects on DML.
-- A building-supply warehouse inventory provides the real-world context.
--
-- Tables created:
--   1. inventory_items — 13 final rows (15 initial, 2 deleted)
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE with TBLPROPERTIES
--   3. INSERT — 15 inventory items across 5 categories and 3 warehouses
--   4. UPDATE — 10% price increase for electrical category
--   5. UPDATE — Restock lumber category (+50 units)
--   6. DELETE — Discontinue items with quantity < 50
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: inventory_items — Building supply warehouse inventory
-- ============================================================================
-- Created with TBLPROPERTIES to demonstrate configuration at table creation.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.inventory_items (
    item_id       INT,
    sku           VARCHAR,
    product_name  VARCHAR,
    category      VARCHAR,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    warehouse     VARCHAR,
    last_updated  VARCHAR
) LOCATION 'delta-table-properties-lifecycle/inventory_items'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.checkpointInterval' = '5'
);


-- ============================================================================
-- VERSION 1: Initial inventory — 15 items across 5 categories, 3 warehouses
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.inventory_items VALUES
    (1,  'SKU-A100', 'Industrial Valve 3-inch',      'plumbing',    120, 45.99,  'warehouse-north', '2024-01-10 08:00:00'),
    (2,  'SKU-A101', 'Copper Fitting 1/2-inch',      'plumbing',    500, 3.25,   'warehouse-north', '2024-01-10 08:05:00'),
    (3,  'SKU-A102', 'PVC Pipe 10ft Schedule 40',    'plumbing',    250, 12.50,  'warehouse-south', '2024-01-10 08:10:00'),
    (4,  'SKU-B200', 'Circuit Breaker 20A',          'electrical',  80,  28.75,  'warehouse-north', '2024-01-11 09:00:00'),
    (5,  'SKU-B201', 'Romex Wire 14/2 250ft',        'electrical',  45,  89.99,  'warehouse-south', '2024-01-11 09:05:00'),
    (6,  'SKU-B202', 'LED Panel Light 2x4',          'electrical',  200, 34.50,  'warehouse-east',  '2024-01-11 09:10:00'),
    (7,  'SKU-C300', 'Concrete Mix 80lb Bag',        'masonry',     300, 6.99,   'warehouse-south', '2024-01-12 10:00:00'),
    (8,  'SKU-C301', 'Rebar #4 20ft',                'masonry',     150, 15.80,  'warehouse-east',  '2024-01-12 10:05:00'),
    (9,  'SKU-C302', 'Mortar Mix 60lb',              'masonry',     180, 8.45,   'warehouse-north', '2024-01-12 10:10:00'),
    (10, 'SKU-D400', 'Framing Lumber 2x4x8',        'lumber',      400, 4.25,   'warehouse-south', '2024-01-13 11:00:00'),
    (11, 'SKU-D401', 'Plywood 4x8 3/4-inch',        'lumber',      100, 42.00,  'warehouse-east',  '2024-01-13 11:05:00'),
    (12, 'SKU-D402', 'Treated Deck Board 5/4x6x12', 'lumber',      75,  18.99,  'warehouse-north', '2024-01-13 11:10:00'),
    (13, 'SKU-E500', 'Paint Primer 1-Gallon',        'finishes',    90,  24.99,  'warehouse-east',  '2024-01-14 08:00:00'),
    (14, 'SKU-E501', 'Wood Stain Quart',             'finishes',    60,  16.75,  'warehouse-south', '2024-01-14 08:05:00'),
    (15, 'SKU-E502', 'Polyurethane 1-Gallon',        'finishes',    40,  32.50,  'warehouse-north', '2024-01-14 08:10:00');


-- ============================================================================
-- VERSION 2: Price increase — electrical category +10%
-- ============================================================================
-- Supplier cost increase passed through to inventory pricing.
UPDATE {{zone_name}}.delta_demos.inventory_items
SET unit_price = ROUND(unit_price * 1.10, 2),
    last_updated = '2024-02-01 09:00:00'
WHERE category = 'electrical';


-- ============================================================================
-- VERSION 3: Restock — lumber category +50 units each
-- ============================================================================
-- Seasonal restock for spring building season.
UPDATE {{zone_name}}.delta_demos.inventory_items
SET quantity = quantity + 50,
    last_updated = '2024-02-15 10:00:00'
WHERE category = 'lumber';


-- ============================================================================
-- VERSION 4: Discontinue — remove items with quantity < 50
-- ============================================================================
-- Items below minimum stock threshold are discontinued and removed.
DELETE FROM {{zone_name}}.delta_demos.inventory_items
WHERE quantity < 50;
