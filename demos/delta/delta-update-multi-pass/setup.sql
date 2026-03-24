-- ============================================================================
-- Delta UPDATE Multi-Pass — ETL Pipeline Stages — Setup Script
-- ============================================================================
-- Demonstrates sequential UPDATE passes as ETL pipeline stages, each creating
-- a new Delta version that can be inspected with VERSION AS OF.
--
-- Tables created:
--   1. order_pipeline — 30 rows of e-commerce orders
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE
--   3. INSERT — 30 raw orders (messy status, empty derived fields)
--   4. UPDATE Pass 1 — Normalize: UPPER(TRIM(status))
--   5. UPDATE Pass 2 — Classify: CASE WHEN assigns priority + shipping_method
--   6. UPDATE Pass 3 — Enrich: compute total_with_tax, estimated_profit
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.pipeline_demos
    COMMENT 'Multi-pass UPDATE pipeline demos';


-- ============================================================================
-- TABLE: order_pipeline — E-commerce orders for ETL processing
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.pipeline_demos.order_pipeline (
    id                INT,
    customer_name     VARCHAR,
    status            VARCHAR,
    item_count        INT,
    unit_price        DECIMAL(10,2),
    subtotal          DECIMAL(12,2),
    tax_rate          DECIMAL(5,4),
    total_with_tax    DECIMAL(12,2),
    priority          VARCHAR,
    shipping_method   VARCHAR,
    estimated_profit  DECIMAL(10,2),
    region            VARCHAR,
    processed_at      VARCHAR
) LOCATION '{{data_path}}/order_pipeline';

GRANT ADMIN ON TABLE {{zone_name}}.pipeline_demos.order_pipeline TO USER {{current_user}};


-- ============================================================================
-- VERSION 1: Raw orders — 30 rows with messy status codes and empty deriveds
-- ============================================================================
-- Status values are deliberately inconsistent: mixed case, leading/trailing
-- whitespace. Derived fields (total_with_tax, estimated_profit, priority,
-- shipping_method, processed_at) are empty/zero — they will be populated
-- by subsequent UPDATE passes.
INSERT INTO {{zone_name}}.pipeline_demos.order_pipeline VALUES
    (1,  'Alice Johnson',   'pending',      3,  29.99,   89.97,  0.0800, 0.00, '', '', 0.00, 'WEST',    ''),
    (2,  'Bob Smith',       'PENDING',      1,  549.00,  549.00, 0.1000, 0.00, '', '', 0.00, 'EAST',    ''),
    (3,  'Carol Williams',  ' confirmed',   5,  12.50,   62.50,  0.0725, 0.00, '', '', 0.00, 'CENTRAL', ''),
    (4,  'David Brown',     'shipped ',     2,  199.99,  399.98, 0.0800, 0.00, '', '', 0.00, 'SOUTH',   ''),
    (5,  'Eva Martinez',    'Delivered',    10, 5.99,    59.90,  0.1000, 0.00, '', '', 0.00, 'WEST',    ''),
    (6,  'Frank Davis',     'pending',      1,  899.00,  899.00, 0.0725, 0.00, '', '', 0.00, 'EAST',    ''),
    (7,  'Grace Wilson',    'PENDING',      4,  45.00,   180.00, 0.0800, 0.00, '', '', 0.00, 'CENTRAL', ''),
    (8,  'Henry Taylor',    ' confirmed',   2,  75.00,   150.00, 0.1000, 0.00, '', '', 0.00, 'SOUTH',   ''),
    (9,  'Ivy Anderson',    'shipped ',     1,  1250.00, 1250.00,0.0725, 0.00, '', '', 0.00, 'WEST',    ''),
    (10, 'Jack Thomas',     'Delivered',    3,  89.99,   269.97, 0.0800, 0.00, '', '', 0.00, 'EAST',    ''),
    (11, 'Karen Jackson',   'pending',      7,  15.99,   111.93, 0.1000, 0.00, '', '', 0.00, 'CENTRAL', ''),
    (12, 'Leo White',       'PENDING',      1,  2100.00, 2100.00,0.0725, 0.00, '', '', 0.00, 'SOUTH',   ''),
    (13, 'Mia Harris',      ' confirmed',   6,  22.50,   135.00, 0.0800, 0.00, '', '', 0.00, 'WEST',    ''),
    (14, 'Noah Clark',      'shipped ',     2,  310.00,  620.00, 0.1000, 0.00, '', '', 0.00, 'EAST',    ''),
    (15, 'Olivia Lewis',    'Delivered',    1,  49.99,   49.99,  0.0725, 0.00, '', '', 0.00, 'CENTRAL', ''),
    (16, 'Paul Robinson',   'pending',      8,  9.99,    79.92,  0.0800, 0.00, '', '', 0.00, 'SOUTH',   ''),
    (17, 'Quinn Walker',    'PENDING',      2,  425.00,  850.00, 0.1000, 0.00, '', '', 0.00, 'WEST',    ''),
    (18, 'Rachel Hall',     ' confirmed',   3,  67.50,   202.50, 0.0725, 0.00, '', '', 0.00, 'EAST',    ''),
    (19, 'Sam Young',       'shipped ',     1,  159.99,  159.99, 0.0800, 0.00, '', '', 0.00, 'CENTRAL', ''),
    (20, 'Tina King',       'Delivered',    4,  35.00,   140.00, 0.1000, 0.00, '', '', 0.00, 'SOUTH',   ''),
    (21, 'Uma Wright',      'pending',      2,  750.00,  1500.00,0.0725, 0.00, '', '', 0.00, 'WEST',    ''),
    (22, 'Victor Lopez',    'PENDING',      5,  18.99,   94.95,  0.0800, 0.00, '', '', 0.00, 'EAST',    ''),
    (23, 'Wendy Hill',      ' confirmed',   1,  1100.00, 1100.00,0.1000, 0.00, '', '', 0.00, 'CENTRAL', ''),
    (24, 'Xavier Scott',    'shipped ',     3,  55.00,   165.00, 0.0725, 0.00, '', '', 0.00, 'SOUTH',   ''),
    (25, 'Yara Green',      'Delivered',    2,  299.99,  599.98, 0.0800, 0.00, '', '', 0.00, 'WEST',    ''),
    (26, 'Zach Adams',      'pending',      1,  79.99,   79.99,  0.1000, 0.00, '', '', 0.00, 'EAST',    ''),
    (27, 'Amy Nelson',      'PENDING',      6,  42.00,   252.00, 0.0725, 0.00, '', '', 0.00, 'CENTRAL', ''),
    (28, 'Brian Carter',    ' confirmed',   2,  185.00,  370.00, 0.0800, 0.00, '', '', 0.00, 'SOUTH',   ''),
    (29, 'Clara Mitchell',  'shipped ',     4,  95.00,   380.00, 0.1000, 0.00, '', '', 0.00, 'WEST',    ''),
    (30, 'Derek Perez',     'Delivered',    1,  449.99,  449.99, 0.0725, 0.00, '', '', 0.00, 'SOUTH',   '');


-- ============================================================================
-- VERSION 2: Pass 1 — Normalize status codes
-- ============================================================================
-- Standardize all status values: trim whitespace and convert to uppercase.
-- Before: 'pending', 'PENDING', ' confirmed', 'shipped ', 'Delivered'
-- After:  'PENDING', 'CONFIRMED', 'SHIPPED', 'DELIVERED'
UPDATE {{zone_name}}.pipeline_demos.order_pipeline
SET status = UPPER(TRIM(status));


-- ============================================================================
-- VERSION 3: Pass 2 — Classify orders by priority and shipping method
-- ============================================================================
-- Business rules based on subtotal thresholds:
--   subtotal > 500  → HIGH priority, EXPRESS shipping
--   subtotal > 100  → MEDIUM priority, STANDARD shipping
--   subtotal <= 100 → LOW priority, ECONOMY shipping
UPDATE {{zone_name}}.pipeline_demos.order_pipeline
SET priority = CASE
        WHEN subtotal > 500 THEN 'HIGH'
        WHEN subtotal > 100 THEN 'MEDIUM'
        ELSE 'LOW'
    END,
    shipping_method = CASE
        WHEN subtotal > 500 THEN 'EXPRESS'
        WHEN subtotal > 100 THEN 'STANDARD'
        ELSE 'ECONOMY'
    END;


-- ============================================================================
-- VERSION 4: Pass 3 — Enrich with computed financial fields
-- ============================================================================
-- Calculate total_with_tax and estimated_profit from existing columns.
-- Mark all rows as processed with a timestamp.
UPDATE {{zone_name}}.pipeline_demos.order_pipeline
SET total_with_tax = ROUND(subtotal * (1 + tax_rate), 2),
    estimated_profit = ROUND(subtotal * 0.15, 2),
    processed_at = '2024-06-15 14:30:00';
