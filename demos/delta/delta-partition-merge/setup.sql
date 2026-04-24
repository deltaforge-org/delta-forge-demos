-- ============================================================================
-- Delta Partition MERGE — Setup Script
-- ============================================================================
-- Creates a product catalog partitioned by category (Electronics, Clothing,
-- Home, Sports) with 60 baseline products (15 per category), plus a staging
-- table representing a supplier feed with 18 rows of updates and new items.
--
-- Table: product_catalog — 60 rows, partitioned by category
-- Table: supplier_feed  — 18 rows (13 updates + 5 new products)
--
-- The supplier feed touches Electronics (5 updates + 3 new), Clothing
-- (5 updates + 2 new), and Home (3 updates only). Sports has zero changes,
-- so its partition directory will remain completely untouched by the MERGE.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: product_catalog — 60 products across 4 categories
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.product_catalog (
    id         INT,
    sku        VARCHAR,
    name       VARCHAR,
    price      DOUBLE,
    stock      INT,
    supplier   VARCHAR,
    category   VARCHAR
) LOCATION 'product_catalog'
PARTITIONED BY (category);


-- Electronics: ids 1-15
INSERT INTO {{zone_name}}.delta_demos.product_catalog
SELECT * FROM (VALUES
    (1,  'SKU-E001', 'Wireless Mouse',       29.99,  150, 'TechCorp',   'Electronics'),
    (2,  'SKU-E002', 'USB-C Hub',            49.99,  80,  'TechCorp',   'Electronics'),
    (3,  'SKU-E003', 'Bluetooth Speaker',    79.99,  45,  'SoundMax',   'Electronics'),
    (4,  'SKU-E004', 'Webcam HD',            64.99,  120, 'TechCorp',   'Electronics'),
    (5,  'SKU-E005', 'Keyboard Mechanical',  129.99, 60,  'KeyMaster',  'Electronics'),
    (6,  'SKU-E006', 'Monitor Stand',        34.99,  200, 'DeskPro',    'Electronics'),
    (7,  'SKU-E007', 'HDMI Cable 6ft',       12.99,  500, 'CableCo',    'Electronics'),
    (8,  'SKU-E008', 'Laptop Sleeve 15in',   24.99,  90,  'BagIt',      'Electronics'),
    (9,  'SKU-E009', 'Wireless Charger',     39.99,  110, 'TechCorp',   'Electronics'),
    (10, 'SKU-E010', 'USB Flash Drive 64GB', 14.99,  300, 'DataSafe',   'Electronics'),
    (11, 'SKU-E011', 'Noise Cancelling Buds',89.99,  70,  'SoundMax',   'Electronics'),
    (12, 'SKU-E012', 'Power Strip Smart',    44.99,  85,  'PowerUp',    'Electronics'),
    (13, 'SKU-E013', 'Ethernet Adapter',     19.99,  160, 'CableCo',    'Electronics'),
    (14, 'SKU-E014', 'Screen Protector',     9.99,   400, 'ShieldTech', 'Electronics'),
    (15, 'SKU-E015', 'Portable SSD 1TB',    109.99,  40,  'DataSafe',   'Electronics')
) AS t(id, sku, name, price, stock, supplier, category);

-- Clothing: ids 16-30
INSERT INTO {{zone_name}}.delta_demos.product_catalog
SELECT * FROM (VALUES
    (16, 'SKU-C001', 'Cotton T-Shirt',      19.99,  300, 'ThreadCo',    'Clothing'),
    (17, 'SKU-C002', 'Denim Jeans Slim',    59.99,  120, 'DenimHouse',  'Clothing'),
    (18, 'SKU-C003', 'Running Shoes',       89.99,  80,  'StrideFit',   'Clothing'),
    (19, 'SKU-C004', 'Winter Jacket',       149.99, 45,  'OutdoorGear', 'Clothing'),
    (20, 'SKU-C005', 'Wool Beanie',         14.99,  250, 'ThreadCo',    'Clothing'),
    (21, 'SKU-C006', 'Polo Shirt',          34.99,  180, 'ThreadCo',    'Clothing'),
    (22, 'SKU-C007', 'Cargo Shorts',        29.99,  200, 'DenimHouse',  'Clothing'),
    (23, 'SKU-C008', 'Rain Poncho',         24.99,  150, 'OutdoorGear', 'Clothing'),
    (24, 'SKU-C009', 'Leather Belt',        39.99,  100, 'LeatherCraft','Clothing'),
    (25, 'SKU-C010', 'Athletic Socks 6pk',  12.99,  400, 'StrideFit',   'Clothing'),
    (26, 'SKU-C011', 'Fleece Hoodie',       49.99,  90,  'ThreadCo',    'Clothing'),
    (27, 'SKU-C012', 'Dress Shirt',         44.99,  110, 'FormalWear',  'Clothing'),
    (28, 'SKU-C013', 'Swim Trunks',         22.99,  160, 'AquaStyle',   'Clothing'),
    (29, 'SKU-C014', 'Hiking Boots',        119.99, 55,  'OutdoorGear', 'Clothing'),
    (30, 'SKU-C015', 'Silk Scarf',          34.99,  70,  'ThreadCo',    'Clothing')
) AS t(id, sku, name, price, stock, supplier, category);

-- Home: ids 31-45
INSERT INTO {{zone_name}}.delta_demos.product_catalog
SELECT * FROM (VALUES
    (31, 'SKU-H001', 'Scented Candle Set',  24.99,  200, 'HomeGlow',   'Home'),
    (32, 'SKU-H002', 'Throw Pillow 18in',   19.99,  150, 'CozyNest',   'Home'),
    (33, 'SKU-H003', 'Kitchen Timer Digital',14.99,  180, 'ChefTools',  'Home'),
    (34, 'SKU-H004', 'Bath Towel Set',      39.99,  100, 'SpaLux',     'Home'),
    (35, 'SKU-H005', 'Plant Pot Ceramic',   29.99,  120, 'GreenThumb', 'Home'),
    (36, 'SKU-H006', 'Wall Clock Modern',   44.99,  60,  'TimeDecor',  'Home'),
    (37, 'SKU-H007', 'Cutting Board Bamboo',22.99,  140, 'ChefTools',  'Home'),
    (38, 'SKU-H008', 'LED Desk Lamp',       54.99,  75,  'BrightLife', 'Home'),
    (39, 'SKU-H009', 'Storage Bins 3pk',    34.99,  90,  'OrgPro',     'Home'),
    (40, 'SKU-H010', 'Coffee Mug Ceramic',  12.99,  350, 'HomeGlow',   'Home'),
    (41, 'SKU-H011', 'Doormat Welcome',     17.99,  160, 'CozyNest',   'Home'),
    (42, 'SKU-H012', 'Spice Rack Rotating', 27.99,  110, 'ChefTools',  'Home'),
    (43, 'SKU-H013', 'Photo Frame 8x10',   15.99,   130, 'FrameIt',    'Home'),
    (44, 'SKU-H014', 'Shower Curtain',      21.99,  85,  'SpaLux',     'Home'),
    (45, 'SKU-H015', 'Bookend Set Metal',   32.99,  70,  'DeskPro',    'Home')
) AS t(id, sku, name, price, stock, supplier, category);

-- Sports: ids 46-60
INSERT INTO {{zone_name}}.delta_demos.product_catalog
SELECT * FROM (VALUES
    (46, 'SKU-S001', 'Yoga Mat Premium',     34.99,  100, 'FlexFit',    'Sports'),
    (47, 'SKU-S002', 'Resistance Bands Set', 24.99,  200, 'FlexFit',    'Sports'),
    (48, 'SKU-S003', 'Water Bottle 32oz',    18.99,  300, 'HydroGear',  'Sports'),
    (49, 'SKU-S004', 'Jump Rope Speed',      14.99,  250, 'CardioKing', 'Sports'),
    (50, 'SKU-S005', 'Foam Roller 18in',     29.99,  80,  'FlexFit',    'Sports'),
    (51, 'SKU-S006', 'Dumbbell Set 20lb',    79.99,  50,  'IronWorks',  'Sports'),
    (52, 'SKU-S007', 'Gym Bag Duffle',       39.99,  120, 'GearUp',     'Sports'),
    (53, 'SKU-S008', 'Cycling Gloves',       22.99,  90,  'RideOn',     'Sports'),
    (54, 'SKU-S009', 'Tennis Balls 3pk',     7.99,   400, 'CourtStar',  'Sports'),
    (55, 'SKU-S010', 'Swim Goggles',         16.99,  150, 'AquaStyle',  'Sports'),
    (56, 'SKU-S011', 'Basketball Indoor',    29.99,  60,  'CourtStar',  'Sports'),
    (57, 'SKU-S012', 'Compression Sleeve',   19.99,  170, 'FlexFit',    'Sports'),
    (58, 'SKU-S013', 'Climbing Chalk Bag',   12.99,  110, 'SummitGear', 'Sports'),
    (59, 'SKU-S014', 'Headband Sweat',       8.99,   350, 'CardioKing', 'Sports'),
    (60, 'SKU-S015', 'Fitness Tracker Band', 49.99,  65,  'TechCorp',   'Sports')
) AS t(id, sku, name, price, stock, supplier, category);


-- ============================================================================
-- TABLE: supplier_feed — 18 rows (staging table for MERGE)
-- ============================================================================
-- Represents a daily supplier price & stock sync. Contains:
--   Electronics: 5 price reductions + restocks, 3 new products
--   Clothing:    5 price reductions + restocks, 2 new products
--   Home:        3 price reductions + restocks, 0 new products
--   Sports:      nothing (no rows in feed → partition untouched)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.supplier_feed (
    id         INT,
    sku        VARCHAR,
    name       VARCHAR,
    price      DOUBLE,
    stock      INT,
    supplier   VARCHAR,
    category   VARCHAR
) LOCATION 'supplier_feed';


INSERT INTO {{zone_name}}.delta_demos.supplier_feed VALUES
    -- Electronics updates (5 existing products: price down, stock up)
    (1,  'SKU-E001', 'Wireless Mouse',       24.99,  180, 'TechCorp',   'Electronics'),
    (3,  'SKU-E003', 'Bluetooth Speaker',    69.99,  60,  'SoundMax',   'Electronics'),
    (5,  'SKU-E005', 'Keyboard Mechanical',  119.99, 75,  'KeyMaster',  'Electronics'),
    (10, 'SKU-E010', 'USB Flash Drive 64GB', 11.99,  350, 'DataSafe',   'Electronics'),
    (14, 'SKU-E014', 'Screen Protector',     7.99,   500, 'ShieldTech', 'Electronics'),
    -- Electronics new (3 products not in catalog)
    (61, 'SKU-E016', 'Webcam 4K Pro',        89.99,  30,  'TechCorp',   'Electronics'),
    (62, 'SKU-E017', 'USB-C Dock Station',   129.99, 25,  'TechCorp',   'Electronics'),
    (63, 'SKU-E018', 'Smart Light Strip',    34.99,  100, 'BrightLife', 'Electronics'),
    -- Clothing updates (5 existing)
    (17, 'SKU-C002', 'Denim Jeans Slim',     54.99,  140, 'DenimHouse', 'Clothing'),
    (19, 'SKU-C004', 'Winter Jacket',        129.99, 60,  'OutdoorGear','Clothing'),
    (21, 'SKU-C006', 'Polo Shirt',           29.99,  200, 'ThreadCo',   'Clothing'),
    (25, 'SKU-C010', 'Athletic Socks 6pk',   9.99,   500, 'StrideFit',  'Clothing'),
    (29, 'SKU-C014', 'Hiking Boots',         109.99, 70,  'OutdoorGear','Clothing'),
    -- Clothing new (2 products)
    (64, 'SKU-C016', 'UV Protection Hat',    27.99,  80,  'OutdoorGear','Clothing'),
    (65, 'SKU-C017', 'Thermal Leggings',     44.99,  90,  'ThreadCo',   'Clothing'),
    -- Home updates (3 existing, no new)
    (33, 'SKU-H003', 'Kitchen Timer Digital', 12.99,  200, 'ChefTools',  'Home'),
    (38, 'SKU-H008', 'LED Desk Lamp',        49.99,  90,  'BrightLife', 'Home'),
    (40, 'SKU-H010', 'Coffee Mug Ceramic',   9.99,   400, 'HomeGlow',   'Home');
