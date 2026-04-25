-- ============================================================================
-- E-Commerce Order Tracking — Indexed UPDATE / DELETE / MERGE — Setup
-- ============================================================================
-- Order fulfillment table for an e-commerce platform. Tracking numbers
-- are high-cardinality (each unique) and arrive in batches as carriers
-- pick up shipments. The index on tracking_number is what turns each
-- carrier event from "scan every file" into "open one slice".
--
-- Tables created:
--   1. shipment_orders — 50 active orders across 2 batches
--
-- Operations performed:
--   1. CREATE DELTA TABLE (with deletion vectors for cheaper UPDATE/DELETE)
--   2. INSERT batch 1 — 30 morning orders
--   3. INSERT batch 2 — 20 afternoon orders
--
-- The CREATE INDEX statement lives in queries.sql so the learner sees
-- it next to the UPDATE/DELETE/MERGE operations it accelerates.
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: shipment_orders
-- ============================================================================
-- Deletion vectors keep UPDATE/DELETE cheap: rather than rewriting the
-- entire file containing the matching row, the engine writes a tiny
-- bitmap marking which rows are logically removed. The index identifies
-- the row to mark; the DV avoids the full-file rewrite.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.shipment_orders (
    order_id        BIGINT,
    tracking_number VARCHAR,
    customer_id     BIGINT,
    carrier         VARCHAR,
    status          VARCHAR,
    weight_kg       DOUBLE,
    destination     VARCHAR,
    placed_at       VARCHAR,
    eta_date        VARCHAR
) LOCATION 'shipment_orders'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');


-- Batch 1 — 30 morning orders
INSERT INTO {{zone_name}}.delta_demos.shipment_orders VALUES
    (5001, '1Z9X7K0001A', 4012, 'BlueSky',  'in_transit',     2.30,  'Berlin',     '2026-04-01', '2026-04-05'),
    (5002, '1Z9X7K0002B', 4015, 'BlueSky',  'in_transit',     0.85,  'Munich',     '2026-04-01', '2026-04-04'),
    (5003, '1Z9X7K0003C', 4023, 'BlueSky',  'out_for_delivery', 5.10, 'Hamburg',    '2026-04-01', '2026-04-03'),
    (5004, '1Z9X7K0004D', 4031, 'NorthStar','in_transit',     3.40,  'Vienna',     '2026-04-01', '2026-04-06'),
    (5005, '1Z9X7K0005E', 4044, 'BlueSky',  'in_transit',     1.75,  'Paris',      '2026-04-01', '2026-04-04'),
    (5006, '1Z9X7K0006F', 4055, 'NorthStar','in_transit',    10.20,  'Madrid',     '2026-04-02', '2026-04-07'),
    (5007, '1Z9X7K0007G', 4067, 'BlueSky',  'preparing',      0.60,  'Rome',       '2026-04-02', '2026-04-08'),
    (5008, '1Z9X7K0008H', 4072, 'BlueSky',  'in_transit',     4.95,  'Lisbon',     '2026-04-02', '2026-04-06'),
    (5009, '1Z9X7K0009J', 4081, 'NorthStar','in_transit',     2.10,  'Amsterdam',  '2026-04-02', '2026-04-05'),
    (5010, '1Z9X7K0010K', 4093, 'BlueSky',  'preparing',      6.50,  'Copenhagen', '2026-04-02', '2026-04-07'),
    (5011, '1Z9X7K0011L', 4104, 'BlueSky',  'in_transit',     1.20,  'Dublin',     '2026-04-02', '2026-04-06'),
    (5012, '1Z9X7K0012M', 4112, 'NorthStar','in_transit',     7.85,  'Helsinki',   '2026-04-02', '2026-04-08'),
    (5013, '1Z9X7K0013N', 4123, 'BlueSky',  'preparing',      0.95,  'Oslo',       '2026-04-02', '2026-04-08'),
    (5014, '1Z9X7K0014P', 4131, 'NorthStar','in_transit',     3.15,  'Stockholm',  '2026-04-02', '2026-04-07'),
    (5015, '1Z9X7K0015Q', 4144, 'BlueSky',  'out_for_delivery', 2.60, 'Warsaw',    '2026-04-02', '2026-04-04'),
    (5016, '1Z9X7K0016R', 4156, 'NorthStar','in_transit',     8.40,  'Prague',     '2026-04-02', '2026-04-06'),
    (5017, '1Z9X7K0017S', 4163, 'BlueSky',  'in_transit',     1.05,  'Budapest',   '2026-04-02', '2026-04-07'),
    (5018, '1Z9X7K0018T', 4175, 'BlueSky',  'preparing',      4.30,  'Athens',     '2026-04-02', '2026-04-09'),
    (5019, '1Z9X7K0019U', 4188, 'NorthStar','in_transit',     2.45,  'Bucharest',  '2026-04-02', '2026-04-08'),
    (5020, '1Z9X7K0020V', 4199, 'BlueSky',  'in_transit',     0.75,  'Sofia',      '2026-04-02', '2026-04-08'),
    (5021, '1Z9X7K0021W', 4202, 'NorthStar','preparing',      9.10,  'Zurich',     '2026-04-02', '2026-04-07'),
    (5022, '1Z9X7K0022X', 4214, 'BlueSky',  'in_transit',     1.85,  'Geneva',     '2026-04-02', '2026-04-06'),
    (5023, '1Z9X7K0023Y', 4223, 'BlueSky',  'in_transit',     5.50,  'Brussels',   '2026-04-02', '2026-04-05'),
    (5024, '1Z9X7K0024Z', 4231, 'NorthStar','out_for_delivery', 0.40, 'Luxembourg','2026-04-02', '2026-04-03'),
    (5025, '1Z9X7K0025A', 4244, 'BlueSky',  'preparing',      3.75,  'Reykjavik',  '2026-04-02', '2026-04-10'),
    (5026, '1Z9X7K0026B', 4256, 'NorthStar','in_transit',     2.95,  'Tallinn',    '2026-04-02', '2026-04-08'),
    (5027, '1Z9X7K0027C', 4263, 'BlueSky',  'in_transit',    11.30,  'Riga',       '2026-04-02', '2026-04-07'),
    (5028, '1Z9X7K0028D', 4275, 'BlueSky',  'in_transit',     1.50,  'Vilnius',    '2026-04-02', '2026-04-08'),
    (5029, '1Z9X7K0029E', 4288, 'NorthStar','preparing',      6.80,  'Ljubljana',  '2026-04-02', '2026-04-09'),
    (5030, '1Z9X7K0030F', 4299, 'BlueSky',  'in_transit',     2.20,  'Zagreb',     '2026-04-02', '2026-04-08');


-- Batch 2 — 20 afternoon orders
INSERT INTO {{zone_name}}.delta_demos.shipment_orders VALUES
    (5031, '1Z9X7K0031G', 4302, 'BlueSky',  'preparing',      0.55,  'Bratislava', '2026-04-02', '2026-04-09'),
    (5032, '1Z9X7K0032H', 4314, 'NorthStar','in_transit',     4.10,  'Belgrade',   '2026-04-02', '2026-04-08'),
    (5033, '1Z9X7K0033J', 4323, 'BlueSky',  'in_transit',     1.95,  'Sarajevo',   '2026-04-02', '2026-04-09'),
    (5034, '1Z9X7K0034K', 4331, 'BlueSky',  'preparing',      7.20,  'Skopje',     '2026-04-02', '2026-04-10'),
    (5035, '1Z9X7K0035L', 4344, 'NorthStar','in_transit',     0.85,  'Tirana',     '2026-04-02', '2026-04-09'),
    (5036, '1Z9X7K0036M', 4356, 'BlueSky',  'in_transit',     3.60,  'Podgorica',  '2026-04-02', '2026-04-09'),
    (5037, '1Z9X7K0037N', 4363, 'BlueSky',  'preparing',      2.05,  'Pristina',   '2026-04-02', '2026-04-10'),
    (5038, '1Z9X7K0038P', 4375, 'NorthStar','in_transit',     5.85,  'Valletta',   '2026-04-02', '2026-04-08'),
    (5039, '1Z9X7K0039Q', 4388, 'BlueSky',  'in_transit',     1.40,  'Nicosia',    '2026-04-02', '2026-04-09'),
    (5040, '1Z9X7K0040R', 4399, 'BlueSky',  'in_transit',     8.95,  'Edinburgh',  '2026-04-02', '2026-04-07'),
    (5041, '1Z9X7K0041S', 4402, 'NorthStar','out_for_delivery', 0.70, 'London',    '2026-04-02', '2026-04-03'),
    (5042, '1Z9X7K0042T', 4414, 'BlueSky',  'preparing',      3.30,  'Cardiff',    '2026-04-02', '2026-04-10'),
    (5043, '1Z9X7K0043U', 4423, 'NorthStar','in_transit',     2.65,  'Glasgow',    '2026-04-02', '2026-04-09'),
    (5044, '1Z9X7K0044V', 4431, 'BlueSky',  'in_transit',    12.40,  'Belfast',    '2026-04-02', '2026-04-09'),
    (5045, '1Z9X7K0045W', 4444, 'BlueSky',  'in_transit',     1.10,  'Manchester', '2026-04-02', '2026-04-09'),
    (5046, '1Z9X7K0046X', 4456, 'NorthStar','preparing',      4.55,  'Liverpool',  '2026-04-02', '2026-04-10'),
    (5047, '1Z9X7K0047Y', 4463, 'BlueSky',  'in_transit',     0.90,  'Leeds',      '2026-04-02', '2026-04-09'),
    (5048, '1Z9X7K0048Z', 4475, 'NorthStar','in_transit',     6.15,  'Bristol',    '2026-04-02', '2026-04-09'),
    (5049, '1Z9X7K0049A', 4488, 'BlueSky',  'in_transit',     2.40,  'Sheffield',  '2026-04-02', '2026-04-09'),
    (5050, '1Z9X7K0050B', 4499, 'BlueSky',  'preparing',      9.80,  'Newcastle',  '2026-04-02', '2026-04-10');
