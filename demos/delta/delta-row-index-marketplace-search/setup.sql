-- ============================================================================
-- Multi-Vendor Marketplace — Multiple Indexes on the Same Table — Setup
-- ============================================================================
-- A marketplace listing table with three concurrent search workloads:
--   1. SKU lookup (warehouse fulfillment)
--   2. Brand filter (storefront search)
--   3. Category + price faceting (browse pages)
--
-- The three CREATE INDEX statements live in queries.sql so each one
-- is taught next to the query shape it serves.
--
-- Tables created:
--   1. marketplace_listings — 70 listings across two batches
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.marketplace_listings (
    listing_id    BIGINT,
    sku           VARCHAR,
    brand         VARCHAR,
    category      VARCHAR,
    title         VARCHAR,
    price         DOUBLE,
    stock         INT,
    seller_id     VARCHAR,
    listed_at     VARCHAR
) LOCATION 'marketplace_listings';


-- Batch 1 — 40 listings
INSERT INTO {{zone_name}}.delta_demos.marketplace_listings VALUES
    (70001, 'SKU-AC-1001', 'AcmeAudio',  'electronics', 'Wireless Earbuds Pro',         89.00, 142, 'sel-001', '2026-03-01'),
    (70002, 'SKU-AC-1002', 'AcmeAudio',  'electronics', 'Soundbar 320',                249.00,  18, 'sel-001', '2026-03-01'),
    (70003, 'SKU-AC-1003', 'AcmeAudio',  'electronics', 'Studio Headphones',           179.00,  46, 'sel-001', '2026-03-02'),
    (70004, 'SKU-BL-2001', 'Bellweather','home',        'Cotton Throw Blanket',         42.50,  98, 'sel-002', '2026-03-02'),
    (70005, 'SKU-BL-2002', 'Bellweather','home',        'Linen Curtain Pair',           74.00,  31, 'sel-002', '2026-03-02'),
    (70006, 'SKU-BL-2003', 'Bellweather','home',        'Velvet Cushion Cover',         28.00, 157, 'sel-002', '2026-03-03'),
    (70007, 'SKU-CR-3001', 'Crestwood',  'outdoor',     '2-Person Backpacking Tent',   189.00,  22, 'sel-003', '2026-03-03'),
    (70008, 'SKU-CR-3002', 'Crestwood',  'outdoor',     'Insulated Sleeping Pad',       95.00,  64, 'sel-003', '2026-03-03'),
    (70009, 'SKU-CR-3003', 'Crestwood',  'outdoor',     'Trekking Poles Carbon',        72.00,  82, 'sel-003', '2026-03-04'),
    (70010, 'SKU-CR-3004', 'Crestwood',  'outdoor',     'Down Quilt 30°',              215.00,  19, 'sel-003', '2026-03-04'),
    (70011, 'SKU-DV-4001', 'Driftvale',  'apparel',     'Merino Crew Sweater',         128.00,  53, 'sel-004', '2026-03-04'),
    (70012, 'SKU-DV-4002', 'Driftvale',  'apparel',     'Linen Trouser',                 84.00,  77, 'sel-004', '2026-03-05'),
    (70013, 'SKU-DV-4003', 'Driftvale',  'apparel',     'Lambswool Beanie',             32.00, 134, 'sel-004', '2026-03-05'),
    (70014, 'SKU-DV-4004', 'Driftvale',  'apparel',     'Waxed Cotton Jacket',         245.00,  15, 'sel-004', '2026-03-05'),
    (70015, 'SKU-EM-5001', 'Emberforge', 'kitchen',     'Cast Iron Skillet 12in',       69.00,  88, 'sel-005', '2026-03-06'),
    (70016, 'SKU-EM-5002', 'Emberforge', 'kitchen',     'Enameled Dutch Oven',         149.00,  41, 'sel-005', '2026-03-06'),
    (70017, 'SKU-EM-5003', 'Emberforge', 'kitchen',     'Carbon Steel Wok 14in',        58.00, 102, 'sel-005', '2026-03-06'),
    (70018, 'SKU-FK-6001', 'Foxkin',     'apparel',     'Recycled Tote Bag',            24.00, 198, 'sel-006', '2026-03-07'),
    (70019, 'SKU-FK-6002', 'Foxkin',     'apparel',     'Organic Cotton Tee',           29.00, 245, 'sel-006', '2026-03-07'),
    (70020, 'SKU-FK-6003', 'Foxkin',     'apparel',     'Hemp Backpack',                88.00,  59, 'sel-006', '2026-03-07'),
    (70021, 'SKU-AC-1004', 'AcmeAudio',  'electronics', 'Bookshelf Speakers Pair',     320.00,  12, 'sel-001', '2026-03-08'),
    (70022, 'SKU-AC-1005', 'AcmeAudio',  'electronics', 'USB-C DAC',                    149.00,  38, 'sel-001', '2026-03-08'),
    (70023, 'SKU-BL-2004', 'Bellweather','home',        'Wool Area Rug 5x7',           295.00,  16, 'sel-002', '2026-03-08'),
    (70024, 'SKU-BL-2005', 'Bellweather','home',        'Ceramic Table Lamp',           62.00,  47, 'sel-002', '2026-03-09'),
    (70025, 'SKU-CR-3005', 'Crestwood',  'outdoor',     'Stainless Mess Kit',           48.00,  91, 'sel-003', '2026-03-09'),
    (70026, 'SKU-CR-3006', 'Crestwood',  'outdoor',     'Headlamp Rechargeable',        39.00, 124, 'sel-003', '2026-03-09'),
    (70027, 'SKU-DV-4005', 'Driftvale',  'apparel',     'Cashmere Scarf',              108.00,  28, 'sel-004', '2026-03-10'),
    (70028, 'SKU-DV-4006', 'Driftvale',  'apparel',     'Selvage Denim Jeans',         168.00,  44, 'sel-004', '2026-03-10'),
    (70029, 'SKU-EM-5004', 'Emberforge', 'kitchen',     'Damascus Chef Knife',         215.00,  21, 'sel-005', '2026-03-10'),
    (70030, 'SKU-EM-5005', 'Emberforge', 'kitchen',     'Olivewood Cutting Board',      78.00,  67, 'sel-005', '2026-03-11'),
    (70031, 'SKU-FK-6004', 'Foxkin',     'apparel',     'Canvas Sneakers',              72.00, 113, 'sel-006', '2026-03-11'),
    (70032, 'SKU-FK-6005', 'Foxkin',     'apparel',     'Linen Shirt',                  64.00,  86, 'sel-006', '2026-03-11'),
    (70033, 'SKU-AC-1006', 'AcmeAudio',  'electronics', 'Portable Speaker XL',          195.00,  27, 'sel-001', '2026-03-12'),
    (70034, 'SKU-BL-2006', 'Bellweather','home',        'Cedar Wardrobe Sachet 3pk',    18.00, 220, 'sel-002', '2026-03-12'),
    (70035, 'SKU-CR-3007', 'Crestwood',  'outdoor',     'Camp Stove Compact',           95.00,  55, 'sel-003', '2026-03-12'),
    (70036, 'SKU-DV-4007', 'Driftvale',  'apparel',     'Chambray Workshirt',          112.00,  33, 'sel-004', '2026-03-13'),
    (70037, 'SKU-EM-5006', 'Emberforge', 'kitchen',     'Copper Saucepan 2qt',         128.00,  29, 'sel-005', '2026-03-13'),
    (70038, 'SKU-FK-6006', 'Foxkin',     'apparel',     'Wool Felt Slippers',           54.00,  72, 'sel-006', '2026-03-13'),
    (70039, 'SKU-AC-1007', 'AcmeAudio',  'electronics', 'Subwoofer 10in',              399.00,   9, 'sel-001', '2026-03-14'),
    (70040, 'SKU-BL-2007', 'Bellweather','home',        'Glass Hurricane Lantern',      45.00,  61, 'sel-002', '2026-03-14');

-- Batch 2 — 30 more listings
INSERT INTO {{zone_name}}.delta_demos.marketplace_listings VALUES
    (70041, 'SKU-CR-3008', 'Crestwood',  'outdoor',     'Bivy Sack Lightweight',       139.00,  37, 'sel-003', '2026-03-15'),
    (70042, 'SKU-DV-4008', 'Driftvale',  'apparel',     'Tweed Flat Cap',                48.00,  91, 'sel-004', '2026-03-15'),
    (70043, 'SKU-EM-5007', 'Emberforge', 'kitchen',     'Ceramic Pestle and Mortar',    34.00, 108, 'sel-005', '2026-03-15'),
    (70044, 'SKU-FK-6007', 'Foxkin',     'apparel',     'Bandana 4-Pack',               19.00, 264, 'sel-006', '2026-03-16'),
    (70045, 'SKU-AC-1008', 'AcmeAudio',  'electronics', 'Vinyl Turntable Belt-Drive',  279.00,  16, 'sel-001', '2026-03-16'),
    (70046, 'SKU-BL-2008', 'Bellweather','home',        'Bamboo Bath Mat',              26.00, 144, 'sel-002', '2026-03-16'),
    (70047, 'SKU-CR-3009', 'Crestwood',  'outdoor',     'Folding Camp Chair',           58.00,  73, 'sel-003', '2026-03-17'),
    (70048, 'SKU-DV-4009', 'Driftvale',  'apparel',     'Heavyweight Hoodie',           94.00,  62, 'sel-004', '2026-03-17'),
    (70049, 'SKU-EM-5008', 'Emberforge', 'kitchen',     'French Press 32oz',            44.00,  85, 'sel-005', '2026-03-17'),
    (70050, 'SKU-FK-6008', 'Foxkin',     'apparel',     'Knit Crew Socks 3-Pack',       28.00, 198, 'sel-006', '2026-03-18'),
    (70051, 'SKU-AC-1009', 'AcmeAudio',  'electronics', 'Streaming Amplifier',         479.00,   8, 'sel-001', '2026-03-18'),
    (70052, 'SKU-BL-2009', 'Bellweather','home',        'Linen Pillowcase Pair',        58.00,  54, 'sel-002', '2026-03-18'),
    (70053, 'SKU-CR-3010', 'Crestwood',  'outdoor',     'Hydration Reservoir 3L',       45.00,  96, 'sel-003', '2026-03-19'),
    (70054, 'SKU-DV-4010', 'Driftvale',  'apparel',     'Field Coat Waxed',            295.00,  14, 'sel-004', '2026-03-19'),
    (70055, 'SKU-EM-5009', 'Emberforge', 'kitchen',     'Pepper Mill Walnut',           62.00,  43, 'sel-005', '2026-03-19'),
    (70056, 'SKU-FK-6009', 'Foxkin',     'apparel',     'Recycled Polyester Anorak',   118.00,  37, 'sel-006', '2026-03-20'),
    (70057, 'SKU-AC-1010', 'AcmeAudio',  'electronics', 'Studio Monitor Pair',         640.00,   6, 'sel-001', '2026-03-20'),
    (70058, 'SKU-BL-2010', 'Bellweather','home',        'Stoneware Vase Tall',          82.00,  19, 'sel-002', '2026-03-20'),
    (70059, 'SKU-CR-3011', 'Crestwood',  'outdoor',     'Fire Starter Flint',           14.00, 312, 'sel-003', '2026-03-21'),
    (70060, 'SKU-DV-4011', 'Driftvale',  'apparel',     'Cashmere Wool Cardigan',      178.00,  21, 'sel-004', '2026-03-21'),
    (70061, 'SKU-EM-5010', 'Emberforge', 'kitchen',     'Sourdough Banneton',           38.00,  74, 'sel-005', '2026-03-21'),
    (70062, 'SKU-FK-6010', 'Foxkin',     'apparel',     'Recycled Wool Throw',          88.00,  46, 'sel-006', '2026-03-22'),
    (70063, 'SKU-AC-1011', 'AcmeAudio',  'electronics', 'Bluetooth Receiver',           69.00, 132, 'sel-001', '2026-03-22'),
    (70064, 'SKU-BL-2011', 'Bellweather','home',        'Marble Coaster 4-Set',         32.00,  88, 'sel-002', '2026-03-22'),
    (70065, 'SKU-CR-3012', 'Crestwood',  'outdoor',     'Compact Camp Pillow',          22.00, 167, 'sel-003', '2026-03-23'),
    (70066, 'SKU-DV-4012', 'Driftvale',  'apparel',     'Brimmed Felt Hat',             92.00,  39, 'sel-004', '2026-03-23'),
    (70067, 'SKU-EM-5011', 'Emberforge', 'kitchen',     'Hand-Forged Cleaver',         165.00,  18, 'sel-005', '2026-03-23'),
    (70068, 'SKU-FK-6011', 'Foxkin',     'apparel',     'Linen Apron',                  46.00,  93, 'sel-006', '2026-03-24'),
    (70069, 'SKU-AC-1012', 'AcmeAudio',  'electronics', 'Headphone Amplifier',         215.00,  24, 'sel-001', '2026-03-24'),
    (70070, 'SKU-BL-2012', 'Bellweather','home',        'Sheepskin Floor Cushion',     145.00,  31, 'sel-002', '2026-03-24');
