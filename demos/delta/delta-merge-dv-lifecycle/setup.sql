-- ============================================================================
-- Delta MERGE — Generating & Materializing Deletion Vectors — Setup Script
-- ============================================================================
-- Creates two tables for the MERGE DV lifecycle demo:
--   1. product_catalog — 40 retail products (10 per category)
--   2. supplier_feed   — 20 ERP feed rows (10 updates, 5 deletes, 5 inserts)
--
-- The queries.sql file demonstrates the full lifecycle:
--   MERGE (generates DVs) → DESCRIBE DETAIL → OPTIMIZE (materializes DVs)
--   → DESCRIBE HISTORY → VACUUM
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: product_catalog — Retail product inventory (40 baseline products)
-- ============================================================================
-- 4 categories × 10 products: electronics, clothing, home, food
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.product_catalog (
    sku             VARCHAR,
    name            VARCHAR,
    category        VARCHAR,
    price           DECIMAL(10,2),
    stock           INT,
    supplier        VARCHAR,
    last_updated    VARCHAR
) LOCATION 'product_catalog';


INSERT INTO {{zone_name}}.delta_demos.product_catalog VALUES
    ('ELEC-1001', 'Wireless Bluetooth Earbuds', 'electronics', 49.99, 120, 'AudioWave', '2025-03-01'),
    ('ELEC-1002', '27-Inch 4K Monitor', 'electronics', 349.99, 35, 'DisplayPro', '2025-03-01'),
    ('ELEC-1003', 'Mechanical Gaming Keyboard', 'electronics', 89.99, 75, 'KeyTech', '2025-03-01'),
    ('ELEC-1004', 'USB-C Docking Station', 'electronics', 129.99, 50, 'HubConnect', '2025-03-01'),
    ('ELEC-1005', 'Portable Power Bank 20000mAh', 'electronics', 39.99, 200, 'ChargePlus', '2025-03-01'),
    ('ELEC-1006', 'Noise-Cancelling Headphones', 'electronics', 199.99, 40, 'AudioWave', '2025-03-01'),
    ('ELEC-1007', 'Wireless Ergonomic Mouse', 'electronics', 34.99, 150, 'ClickGear', '2025-03-01'),
    ('ELEC-1008', 'Smart LED Desk Lamp', 'electronics', 59.99, 80, 'LumiTech', '2025-03-01'),
    ('ELEC-1009', 'Webcam 1080p Autofocus', 'electronics', 69.99, 65, 'VisionCam', '2025-03-01'),
    ('ELEC-1010', 'Surge Protector 8-Outlet', 'electronics', 24.99, 300, 'PowerGuard', '2025-03-01'),
    ('CLTH-2001', 'Cotton Crew-Neck T-Shirt', 'clothing', 19.99, 500, 'ThreadCo', '2025-03-01'),
    ('CLTH-2002', 'Slim-Fit Chino Pants', 'clothing', 44.99, 200, 'ThreadCo', '2025-03-01'),
    ('CLTH-2003', 'Waterproof Rain Jacket', 'clothing', 79.99, 90, 'OutdoorEdge', '2025-03-01'),
    ('CLTH-2004', 'Merino Wool Sweater', 'clothing', 64.99, 110, 'WoolCraft', '2025-03-01'),
    ('CLTH-2005', 'Running Shoes Lightweight', 'clothing', 109.99, 70, 'StridePro', '2025-03-01'),
    ('CLTH-2006', 'Denim Jacket Classic', 'clothing', 59.99, 130, 'ThreadCo', '2025-03-01'),
    ('CLTH-2007', 'Athletic Shorts Mesh', 'clothing', 24.99, 350, 'StridePro', '2025-03-01'),
    ('CLTH-2008', 'Flannel Button-Down Shirt', 'clothing', 34.99, 180, 'ThreadCo', '2025-03-01'),
    ('CLTH-2009', 'Insulated Winter Boots', 'clothing', 89.99, 55, 'OutdoorEdge', '2025-03-01'),
    ('CLTH-2010', 'Leather Belt Reversible', 'clothing', 29.99, 250, 'LeatherKing', '2025-03-01'),
    ('HOME-3001', 'Stainless Steel Water Bottle', 'home', 22.99, 400, 'HomeEssentials', '2025-03-01'),
    ('HOME-3002', 'Bamboo Cutting Board Set', 'home', 29.99, 180, 'KitchenCraft', '2025-03-01'),
    ('HOME-3003', 'Memory Foam Pillow', 'home', 39.99, 150, 'SleepWell', '2025-03-01'),
    ('HOME-3004', 'Ceramic Coffee Mug Set', 'home', 18.99, 300, 'KitchenCraft', '2025-03-01'),
    ('HOME-3005', 'Aromatherapy Diffuser', 'home', 34.99, 95, 'ZenHome', '2025-03-01'),
    ('HOME-3006', 'Cotton Bath Towel Set', 'home', 44.99, 120, 'HomeEssentials', '2025-03-01'),
    ('HOME-3007', 'Wall-Mounted Shelf Set', 'home', 54.99, 60, 'WoodWorks', '2025-03-01'),
    ('HOME-3008', 'Non-Stick Cookware Set', 'home', 89.99, 45, 'KitchenCraft', '2025-03-01'),
    ('HOME-3009', 'LED String Lights 50ft', 'home', 16.99, 250, 'LumiTech', '2025-03-01'),
    ('HOME-3010', 'Vacuum Storage Bags 10-Pack', 'home', 14.99, 200, 'HomeEssentials', '2025-03-01'),
    ('FOOD-4001', 'Organic Coffee Beans 2lb', 'food', 24.99, 350, 'BeanOrigin', '2025-03-01'),
    ('FOOD-4002', 'Extra Virgin Olive Oil 1L', 'food', 16.99, 280, 'MedHarvest', '2025-03-01'),
    ('FOOD-4003', 'Raw Honey Wildflower 32oz', 'food', 14.99, 220, 'NaturePure', '2025-03-01'),
    ('FOOD-4004', 'Dried Mango Slices 1lb', 'food', 9.99, 400, 'TropiFruit', '2025-03-01'),
    ('FOOD-4005', 'Quinoa Grain Organic 5lb', 'food', 18.99, 160, 'GrainWorks', '2025-03-01'),
    ('FOOD-4006', 'Dark Chocolate Bar 85%', 'food', 5.99, 600, 'CocoaCraft', '2025-03-01'),
    ('FOOD-4007', 'Matcha Green Tea Powder', 'food', 29.99, 140, 'TeaLeaf', '2025-03-01'),
    ('FOOD-4008', 'Mixed Nut Butter 16oz', 'food', 12.99, 250, 'NutHouse', '2025-03-01'),
    ('FOOD-4009', 'Sparkling Water Variety 24pk', 'food', 11.99, 180, 'FizzCo', '2025-03-01'),
    ('FOOD-4010', 'Protein Bar Sampler 12-Pack', 'food', 22.99, 300, 'FitFuel', '2025-03-01');


-- ============================================================================
-- TABLE: supplier_feed — Daily ERP product feed (20 rows)
-- ============================================================================
-- 10 rows: existing SKUs with updated price/stock (MERGE → UPDATE)
-- 5 rows:  existing SKUs with stock=0 (MERGE → DELETE, discontinued)
-- 5 rows:  new SKUs not in catalog (MERGE → INSERT)
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.supplier_feed (
    sku             VARCHAR,
    name            VARCHAR,
    category        VARCHAR,
    price           DECIMAL(10,2),
    stock           INT,
    supplier        VARCHAR,
    last_updated    VARCHAR
) LOCATION 'supplier_feed';


INSERT INTO {{zone_name}}.delta_demos.supplier_feed VALUES
    -- Updates: existing SKUs with new price/stock (stock > 0 → UPDATE)
    ('ELEC-1001', 'Wireless Bluetooth Earbuds', 'electronics', 44.99, 140, 'AudioWave', '2025-03-15'),
    ('ELEC-1005', 'Portable Power Bank 20000mAh', 'electronics', 34.99, 250, 'ChargePlus', '2025-03-15'),
    ('ELEC-1008', 'Smart LED Desk Lamp', 'electronics', 64.99, 60, 'LumiTech', '2025-03-15'),
    ('CLTH-2002', 'Slim-Fit Chino Pants', 'clothing', 49.99, 175, 'ThreadCo', '2025-03-15'),
    ('CLTH-2005', 'Running Shoes Lightweight', 'clothing', 99.99, 85, 'StridePro', '2025-03-15'),
    ('HOME-3003', 'Memory Foam Pillow', 'home', 39.99, 180, 'SleepWell', '2025-03-15'),
    ('HOME-3006', 'Cotton Bath Towel Set', 'home', 49.99, 100, 'HomeEssentials', '2025-03-15'),
    ('FOOD-4001', 'Organic Coffee Beans 2lb', 'food', 27.99, 320, 'BeanOrigin', '2025-03-15'),
    ('FOOD-4006', 'Dark Chocolate Bar 85%', 'food', 5.99, 700, 'CocoaCraft', '2025-03-15'),
    ('FOOD-4009', 'Sparkling Water Variety 24pk', 'food', 10.99, 220, 'FizzCo', '2025-03-15'),
    -- Deletes: existing SKUs discontinued (stock = 0 → DELETE)
    ('ELEC-1009', 'Webcam 1080p Autofocus', 'electronics', 69.99, 0, 'VisionCam', '2025-03-15'),
    ('CLTH-2008', 'Flannel Button-Down Shirt', 'clothing', 34.99, 0, 'ThreadCo', '2025-03-15'),
    ('HOME-3009', 'LED String Lights 50ft', 'home', 16.99, 0, 'LumiTech', '2025-03-15'),
    ('FOOD-4003', 'Raw Honey Wildflower 32oz', 'food', 14.99, 0, 'NaturePure', '2025-03-15'),
    ('FOOD-4008', 'Mixed Nut Butter 16oz', 'food', 12.99, 0, 'NutHouse', '2025-03-15'),
    -- Inserts: new SKUs not in catalog (NOT MATCHED → INSERT)
    ('ELEC-1011', 'Wireless Charging Pad', 'electronics', 29.99, 100, 'ChargePlus', '2025-03-15'),
    ('CLTH-2011', 'UV-Protection Sunglasses', 'clothing', 39.99, 160, 'OutdoorEdge', '2025-03-15'),
    ('HOME-3011', 'Cast Iron Skillet 12-Inch', 'home', 44.99, 80, 'KitchenCraft', '2025-03-15'),
    ('FOOD-4011', 'Organic Maple Syrup 16oz', 'food', 13.99, 200, 'NaturePure', '2025-03-15'),
    ('FOOD-4012', 'Cold Brew Coffee Concentrate', 'food', 19.99, 150, 'BeanOrigin', '2025-03-15');
