-- ============================================================================
-- String Statistics — Truncation & Bloom Filter Bridge — Setup Script
-- ============================================================================
-- IT product catalog with short SKU codes, medium product names, and long URLs.
-- Delta only tracks the first 32 characters for string statistics. Short strings
-- get full min/max ranges; long strings with common prefixes get truncated stats
-- that defeat data skipping.
--
-- Tables created:
--   1. product_catalog — 20 IT products with varied string lengths
--
-- Operations performed:
--   1. CREATE DELTA TABLE
--   2. INSERT all 20 products
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.string_demos
    COMMENT 'String statistics and truncation tutorial demos';


-- ============================================================================
-- TABLE: product_catalog — IT products with varied string column lengths
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.string_demos.product_catalog (
    id          INT,
    sku         VARCHAR,
    name        VARCHAR,
    description VARCHAR,
    category    VARCHAR,
    price       DOUBLE,
    url         VARCHAR
) LOCATION '{{data_path}}/product_catalog';

GRANT ADMIN ON TABLE {{zone_name}}.string_demos.product_catalog TO USER {{current_user}};


-- ============================================================================
-- STEP 2: Insert 20 products — SKUs (8 chars), names (8-24), descriptions
-- (43-77 chars), URLs (71-83 chars, all share 35-char common prefix)
-- ============================================================================
INSERT INTO {{zone_name}}.string_demos.product_catalog VALUES
    (1,  'SKU-A001', 'Wireless Mouse',          'Ergonomic wireless mouse with adjustable DPI settings',                              'peripherals', 29.99, 'https://store.example.com/products/peripherals/wireless-mouse-ergonomic-a001'),
    (2,  'SKU-A002', 'USB-C Hub',               'Multi-port USB-C hub with HDMI and ethernet',                                        'peripherals', 49.99, 'https://store.example.com/products/peripherals/usb-c-hub-multiport-a002'),
    (3,  'SKU-A003', 'Webcam HD',               'High-definition webcam with built-in microphone and auto-focus',                     'peripherals', 69.99, 'https://store.example.com/products/peripherals/webcam-hd-autofocus-a003'),
    (4,  'SKU-B001', 'Laptop Stand',            'Adjustable aluminum laptop stand for improved ergonomics',                           'furniture',   39.99, 'https://store.example.com/products/furniture/laptop-stand-aluminum-b001'),
    (5,  'SKU-B002', 'Monitor Arm',             'Dual monitor arm with full range of motion clamp mount',                             'furniture',   89.99, 'https://store.example.com/products/furniture/monitor-arm-dual-clamp-b002'),
    (6,  'SKU-B003', 'Desk Pad',               'Premium leather desk pad with anti-slip base',                                       'furniture',   24.99, 'https://store.example.com/products/furniture/desk-pad-leather-premium-b003'),
    (7,  'SKU-C001', 'Mechanical Keyboard',     'Cherry MX Blue mechanical keyboard with RGB backlighting',                           'peripherals', 129.99,'https://store.example.com/products/peripherals/mechanical-keyboard-cherry-c001'),
    (8,  'SKU-C002', 'Noise Cancelling Headset','Active noise cancelling wireless headset with 30-hour battery',                      'audio',       199.99,'https://store.example.com/products/audio/noise-cancelling-headset-wireless-c002'),
    (9,  'SKU-C003', 'Portable Speaker',        'Waterproof Bluetooth portable speaker with 360-degree sound',                        'audio',       79.99, 'https://store.example.com/products/audio/portable-speaker-bluetooth-360-c003'),
    (10, 'SKU-D001', 'Docking Station',         'Thunderbolt 4 docking station with triple display support and 96W charging',         'peripherals', 249.99,'https://store.example.com/products/peripherals/docking-station-thunderbolt4-d001'),
    (11, 'SKU-D002', 'Ergonomic Chair',         'Full mesh ergonomic office chair with lumbar support and adjustable armrests',        'furniture',   449.99,'https://store.example.com/products/furniture/ergonomic-chair-mesh-lumbar-d002'),
    (12, 'SKU-D003', 'Standing Desk',           'Electric standing desk with programmable height presets and cable management',        'furniture',   599.99,'https://store.example.com/products/furniture/standing-desk-electric-presets-d003'),
    (13, 'SKU-E001', 'Wireless Charger',        'Fast wireless charging pad compatible with Qi-enabled devices',                      'accessories', 19.99, 'https://store.example.com/products/accessories/wireless-charger-qi-fast-e001'),
    (14, 'SKU-E002', 'Cable Management Kit',    'Complete cable management kit with clips, sleeves, and velcro ties for desk',        'accessories', 14.99, 'https://store.example.com/products/accessories/cable-management-kit-complete-e002'),
    (15, 'SKU-E003', 'Screen Protector',        'Anti-glare matte screen protector for 27-inch monitors with easy installation',      'accessories', 12.99, 'https://store.example.com/products/accessories/screen-protector-antiglare-27in-e003'),
    (16, 'SKU-F001', 'Thunderbolt Cable',       'Certified Thunderbolt 4 cable with 40Gbps data transfer rate',                       'accessories', 34.99, 'https://store.example.com/products/accessories/thunderbolt-cable-40gbps-f001'),
    (17, 'SKU-F002', 'USB Microphone',          'Condenser USB microphone with cardioid pattern for streaming and podcasts',          'audio',       119.99,'https://store.example.com/products/audio/usb-microphone-condenser-cardioid-f002'),
    (18, 'SKU-F003', 'Desk Lamp',              'LED desk lamp with adjustable color temperature and brightness',                     'furniture',   44.99, 'https://store.example.com/products/furniture/desk-lamp-led-adjustable-temp-f003'),
    (19, 'SKU-G001', 'Webcam Light',           'Ring light for webcam with three color modes and dimmer',                            'accessories', 22.99, 'https://store.example.com/products/accessories/webcam-light-ring-3mode-g001'),
    (20, 'SKU-G002', 'Power Strip',            'Smart power strip with USB ports and surge protection',                              'accessories', 29.99, 'https://store.example.com/products/accessories/power-strip-smart-usb-surge-g002');
