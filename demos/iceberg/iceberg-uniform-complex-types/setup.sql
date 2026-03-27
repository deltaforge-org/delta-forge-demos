-- ============================================================================
-- Iceberg UniForm Complex Types — Setup
-- ============================================================================
-- Creates a Delta table with complex nested types (STRUCT, ARRAY, MAP) and
-- Iceberg UniForm enabled. The Iceberg metadata must correctly represent
-- nested type definitions for interoperability with Iceberg engines.
--
-- Dataset: 18 products across 3 categories (Electronics, Home, Outdoor).
-- Each product has:
--   - dimensions: STRUCT(length, width, height, unit)
--   - tags: ARRAY of VARCHAR labels
--   - localized_names: MAP of language code → translated name
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with complex types and UniForm
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.product_catalog_nested (
    product_id       INT,
    product_name     VARCHAR,
    category         VARCHAR,
    price            DOUBLE,
    dimensions       STRUCT(length DOUBLE, width DOUBLE, height DOUBLE, unit VARCHAR),
    tags             ARRAY(VARCHAR),
    localized_names  MAP(VARCHAR, VARCHAR),
    in_stock         BOOLEAN
) LOCATION '{{data_path}}/product_catalog_nested'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.product_catalog_nested TO USER {{current_user}};

-- STEP 3: Seed 18 products — 6 per category (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.product_catalog_nested VALUES
    -- Electronics: 6 products
    (1,  'Wireless Mouse',      'Electronics', 29.99,
        STRUCT(10.5, 6.2, 3.8, 'cm'),
        ARRAY('peripheral', 'wireless', 'office'),
        MAP('en', 'Wireless Mouse', 'fr', 'Souris Sans Fil', 'de', 'Kabellose Maus'),
        true),
    (2,  'Mechanical Keyboard', 'Electronics', 89.99,
        STRUCT(44.0, 13.5, 3.5, 'cm'),
        ARRAY('peripheral', 'mechanical', 'gaming'),
        MAP('en', 'Mechanical Keyboard', 'fr', 'Clavier Mecanique', 'de', 'Mechanische Tastatur'),
        true),
    (3,  'USB-C Hub',           'Electronics', 45.50,
        STRUCT(12.0, 5.5, 1.8, 'cm'),
        ARRAY('accessory', 'usb-c', 'portable'),
        MAP('en', 'USB-C Hub', 'fr', 'Hub USB-C', 'de', 'USB-C Hub'),
        true),
    (4,  'Webcam HD',           'Electronics', 64.99,
        STRUCT(8.0, 5.0, 5.0, 'cm'),
        ARRAY('peripheral', 'video', 'streaming'),
        MAP('en', 'Webcam HD', 'fr', 'Webcam HD', 'de', 'Webcam HD'),
        true),
    (5,  'Bluetooth Speaker',   'Electronics', 34.95,
        STRUCT(15.0, 7.0, 7.0, 'cm'),
        ARRAY('audio', 'bluetooth', 'portable'),
        MAP('en', 'Bluetooth Speaker', 'fr', 'Haut-Parleur Bluetooth', 'de', 'Bluetooth Lautsprecher'),
        true),
    (6,  'LED Monitor',         'Electronics', 249.00,
        STRUCT(61.0, 36.0, 18.0, 'cm'),
        ARRAY('display', 'led', 'office'),
        MAP('en', 'LED Monitor', 'fr', 'Moniteur LED', 'de', 'LED Monitor'),
        false),
    -- Home: 6 products
    (7,  'Ceramic Vase',        'Home',        22.50,
        STRUCT(12.0, 12.0, 25.0, 'cm'),
        ARRAY('decor', 'ceramic', 'handmade'),
        MAP('en', 'Ceramic Vase', 'fr', 'Vase en Ceramique', 'de', 'Keramikvase'),
        true),
    (8,  'Table Lamp',          'Home',        38.00,
        STRUCT(15.0, 15.0, 40.0, 'cm'),
        ARRAY('lighting', 'modern', 'bedroom'),
        MAP('en', 'Table Lamp', 'fr', 'Lampe de Table', 'de', 'Tischlampe'),
        true),
    (9,  'Cotton Throw',        'Home',        55.00,
        STRUCT(150.0, 200.0, 2.0, 'cm'),
        ARRAY('textile', 'cotton', 'cozy'),
        MAP('en', 'Cotton Throw', 'fr', 'Couverture en Coton', 'de', 'Baumwolldecke'),
        true),
    (10, 'Wall Clock',          'Home',        29.99,
        STRUCT(30.0, 30.0, 4.5, 'cm'),
        ARRAY('decor', 'clock', 'minimalist'),
        MAP('en', 'Wall Clock', 'fr', 'Horloge Murale', 'de', 'Wanduhr'),
        true),
    (11, 'Scented Candle',      'Home',        18.75,
        STRUCT(8.0, 8.0, 10.0, 'cm'),
        ARRAY('candle', 'scented', 'relaxation'),
        MAP('en', 'Scented Candle', 'fr', 'Bougie Parfumee', 'de', 'Duftkerze'),
        true),
    (12, 'Picture Frame',       'Home',        15.99,
        STRUCT(25.0, 20.0, 2.5, 'cm'),
        ARRAY('decor', 'frame', 'wooden'),
        MAP('en', 'Picture Frame', 'fr', 'Cadre Photo', 'de', 'Bilderrahmen'),
        false),
    -- Outdoor: 6 products
    (13, 'Camping Tent',        'Outdoor',     129.99,
        STRUCT(220.0, 150.0, 120.0, 'cm'),
        ARRAY('camping', 'waterproof', 'family'),
        MAP('en', 'Camping Tent', 'fr', 'Tente de Camping', 'de', 'Campingzelt'),
        true),
    (14, 'Hiking Backpack',     'Outdoor',     79.50,
        STRUCT(55.0, 35.0, 25.0, 'cm'),
        ARRAY('hiking', 'backpack', 'durable'),
        MAP('en', 'Hiking Backpack', 'fr', 'Sac a Dos de Randonnee', 'de', 'Wanderrucksack'),
        true),
    (15, 'Water Bottle',        'Outdoor',     24.99,
        STRUCT(7.5, 7.5, 26.0, 'cm'),
        ARRAY('hydration', 'insulated', 'eco-friendly'),
        MAP('en', 'Water Bottle', 'fr', 'Gourde', 'de', 'Wasserflasche'),
        true),
    (16, 'Folding Chair',       'Outdoor',     42.00,
        STRUCT(50.0, 50.0, 80.0, 'cm'),
        ARRAY('furniture', 'portable', 'camping'),
        MAP('en', 'Folding Chair', 'fr', 'Chaise Pliante', 'de', 'Klappstuhl'),
        true),
    (17, 'Solar Lantern',       'Outdoor',     19.95,
        STRUCT(10.0, 10.0, 18.0, 'cm'),
        ARRAY('lighting', 'solar', 'eco-friendly'),
        MAP('en', 'Solar Lantern', 'fr', 'Lanterne Solaire', 'de', 'Solarlaterne'),
        true),
    (18, 'Hammock',             'Outdoor',     65.00,
        STRUCT(300.0, 150.0, 3.0, 'cm'),
        ARRAY('relaxation', 'outdoor', 'portable'),
        MAP('en', 'Hammock', 'fr', 'Hamac', 'de', 'Hangematte'),
        false);
