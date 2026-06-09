-- ============================================================================
-- Delta DESCRIBE HISTORY — Root-Cause Investigation — Setup Script
-- ============================================================================
-- A SaaS platform tracks product revenue metrics across regions. This setup
-- simulates a realistic incident: correct data arrives, a region label is
-- fixed, more data arrives, then someone accidentally runs a script that
-- doubles Americas revenue. The queries.sql walks through the investigation.
--
-- Version History:
--   V0: CREATE TABLE                                      → 0 rows
--   V1: INSERT 20 baseline metric records                 → 20 rows
--   V2: UPDATE 5 records (region label fix: APAC → Asia-Pacific) → 20 rows
--   V3: INSERT 10 next-day records                        → 30 rows
--   V4: UPDATE — BAD: doubled revenue for Americas region → 30 rows (10 damaged)
--   V5: INSERT 5 following-day records                    → 35 rows
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- VERSION 0: CREATE TABLE
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.product_metrics (
    id              INT,
    product         VARCHAR,
    region          VARCHAR,
    metric_date     VARCHAR,
    revenue         DOUBLE,
    active_users    INT,
    conversion_rate DOUBLE
) LOCATION 'delta-history-investigation/product_metrics';


-- ============================================================================
-- VERSION 1: INSERT 20 baseline metric records (correct data)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.product_metrics VALUES
    (1,  'CloudSync',   'Americas',  '2025-04-01', 12500.00, 340, 0.042),
    (2,  'CloudSync',   'EMEA',      '2025-04-01', 9800.00,  280, 0.035),
    (3,  'CloudSync',   'Americas',  '2025-04-02', 13100.00, 355, 0.044),
    (4,  'CloudSync',   'APAC',      '2025-04-01', 7200.00,  190, 0.031),
    (5,  'DataVault',   'EMEA',      '2025-04-01', 18500.00, 520, 0.048),
    (6,  'DataVault',   'Americas',  '2025-04-01', 22000.00, 610, 0.055),
    (7,  'DataVault',   'EMEA',      '2025-04-02', 19200.00, 540, 0.050),
    (8,  'DataVault',   'Americas',  '2025-04-02', 23500.00, 630, 0.058),
    (9,  'PipelineX',   'APAC',      '2025-04-01', 5600.00,  150, 0.028),
    (10, 'PipelineX',   'EMEA',      '2025-04-01', 8900.00,  245, 0.038),
    (11, 'PipelineX',   'Americas',  '2025-04-01', 11200.00, 310, 0.041),
    (12, 'PipelineX',   'EMEA',      '2025-04-02', 9100.00,  250, 0.039),
    (13, 'FlowEngine',  'EMEA',      '2025-04-01', 6700.00,  180, 0.029),
    (14, 'FlowEngine',  'APAC',      '2025-04-01', 4300.00,  120, 0.025),
    (15, 'FlowEngine',  'EMEA',      '2025-04-02', 7100.00,  195, 0.032),
    (16, 'FlowEngine',  'Americas',  '2025-04-01', 8800.00,  240, 0.037),
    (17, 'CloudSync',   'APAC',      '2025-04-02', 7500.00,  200, 0.033),
    (18, 'PipelineX',   'EMEA',      '2025-04-03', 9400.00,  260, 0.040),
    (19, 'FlowEngine',  'EMEA',      '2025-04-03', 7300.00,  200, 0.031),
    (20, 'DataVault',   'APAC',      '2025-04-01', 6800.00,  185, 0.030);


-- ============================================================================
-- VERSION 2: UPDATE — region label correction (APAC → Asia-Pacific)
-- ============================================================================
-- Product team standardized region names. This is a legitimate metadata fix.
UPDATE {{zone_name}}.delta_demos.product_metrics
SET region = 'Asia-Pacific'
WHERE id IN (4, 9, 14, 17, 20);


-- ============================================================================
-- VERSION 3: INSERT 10 next-day records (correct data)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.product_metrics VALUES
    (21, 'CloudSync',   'Americas',      '2025-04-03', 13400.00, 365, 0.045),
    (22, 'CloudSync',   'EMEA',          '2025-04-03', 10100.00, 290, 0.036),
    (23, 'DataVault',   'Asia-Pacific',  '2025-04-02', 7100.00,  195, 0.032),
    (24, 'DataVault',   'EMEA',          '2025-04-03', 19800.00, 555, 0.051),
    (25, 'PipelineX',   'Americas',      '2025-04-03', 11800.00, 325, 0.043),
    (26, 'FlowEngine',  'Americas',      '2025-04-02', 9200.00,  250, 0.038),
    (27, 'FlowEngine',  'Asia-Pacific',  '2025-04-02', 4500.00,  125, 0.026),
    (28, 'PipelineX',   'Asia-Pacific',  '2025-04-02', 5900.00,  160, 0.029),
    (29, 'CloudSync',   'Asia-Pacific',  '2025-04-03', 7800.00,  210, 0.034),
    (30, 'DataVault',   'Americas',      '2025-04-03', 24000.00, 650, 0.060);


-- ============================================================================
-- VERSION 4: BAD UPDATE — someone doubled Americas revenue by mistake
-- ============================================================================
-- An engineer ran a revenue-adjustment script with the wrong multiplier.
-- Instead of a 1.02x inflation correction, they applied 2.0x to all Americas
-- records. This is the bug the investigation will uncover.
UPDATE {{zone_name}}.delta_demos.product_metrics
SET revenue = revenue * 2
WHERE region = 'Americas';


-- ============================================================================
-- VERSION 5: INSERT 5 following-day records (correct data)
-- ============================================================================
-- Normal data ingestion continues. These records are correct and arrive
-- after the bad update, making the incident harder to spot at first glance.
INSERT INTO {{zone_name}}.delta_demos.product_metrics VALUES
    (31, 'CloudSync',   'EMEA',          '2025-04-04', 10300.00, 295, 0.037),
    (32, 'DataVault',   'Americas',      '2025-04-04', 25000.00, 670, 0.062),
    (33, 'PipelineX',   'Asia-Pacific',  '2025-04-03', 6100.00,  165, 0.030),
    (34, 'FlowEngine',  'EMEA',          '2025-04-04', 7500.00,  205, 0.033),
    (35, 'DataVault',   'Asia-Pacific',  '2025-04-03', 7300.00,  200, 0.031);
