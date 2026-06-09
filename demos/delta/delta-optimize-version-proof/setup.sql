-- ============================================================================
-- Delta OPTIMIZE — Cross-Version Data Integrity Proof — Setup Script
-- ============================================================================
-- A logistics company tracks shipment manifests across three carriers.
-- Daily merchant uploads create file fragmentation. This setup builds a
-- 4-version history to simulate weeks of operational activity.
--
-- Version History:
--   V0: CREATE TABLE                              → 0 rows
--   V1: INSERT 20 shipments                       → 20 rows
--   V2: UPDATE 5 shipments (weight recalibration) → 20 rows (5 changed)
--   V3: DELETE 3 cancelled shipments              → 17 rows
--   V4: INSERT 8 new shipments                    → 25 rows
--
-- After setup, the table has 4+ small Parquet files. The queries.sql
-- script runs OPTIMIZE and proves data integrity across the version boundary.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- VERSION 0: CREATE TABLE
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.shipments (
    id             INT,
    tracking_code  VARCHAR,
    carrier        VARCHAR,
    origin         VARCHAR,
    destination    VARCHAR,
    weight_kg      DOUBLE,
    status         VARCHAR,
    ship_date      VARCHAR
) LOCATION 'delta-optimize-version-proof/shipments';


-- ============================================================================
-- VERSION 1: INSERT 20 shipments across 3 carriers
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.shipments VALUES
    (1,  'TRK-10001', 'FastFreight',  'Chicago',     'New York',      12.5,  'delivered',  '2025-03-01'),
    (2,  'TRK-10002', 'SwiftShip',    'Los Angeles', 'Seattle',       8.3,   'delivered',  '2025-03-01'),
    (3,  'TRK-10003', 'FastFreight',  'Houston',     'Miami',         22.0,  'in_transit', '2025-03-02'),
    (4,  'TRK-10004', 'CargoLine',    'Boston',      'Philadelphia',  5.7,   'delivered',  '2025-03-02'),
    (5,  'TRK-10005', 'SwiftShip',    'Denver',      'Phoenix',       15.1,  'delivered',  '2025-03-03'),
    (6,  'TRK-10006', 'FastFreight',  'Atlanta',     'Dallas',        9.8,   'cancelled',  '2025-03-03'),
    (7,  'TRK-10007', 'CargoLine',    'Portland',    'San Francisco', 18.2,  'delivered',  '2025-03-04'),
    (8,  'TRK-10008', 'SwiftShip',    'Detroit',     'Cleveland',     3.4,   'in_transit', '2025-03-04'),
    (9,  'TRK-10009', 'FastFreight',  'Nashville',   'Memphis',       27.6,  'delivered',  '2025-03-05'),
    (10, 'TRK-10010', 'CargoLine',    'Minneapolis', 'Milwaukee',     11.0,  'delivered',  '2025-03-05'),
    (11, 'TRK-10011', 'SwiftShip',    'Orlando',     'Tampa',         6.2,   'delivered',  '2025-03-06'),
    (12, 'TRK-10012', 'FastFreight',  'Salt Lake',   'Las Vegas',     14.8,  'in_transit', '2025-03-06'),
    (13, 'TRK-10013', 'CargoLine',    'San Diego',   'Sacramento',    20.3,  'delivered',  '2025-03-07'),
    (14, 'TRK-10014', 'SwiftShip',    'Charlotte',   'Raleigh',       7.9,   'cancelled',  '2025-03-07'),
    (15, 'TRK-10015', 'FastFreight',  'Austin',      'San Antonio',   16.5,  'delivered',  '2025-03-08'),
    (16, 'TRK-10016', 'CargoLine',    'Pittsburgh',  'Baltimore',     9.1,   'in_transit', '2025-03-08'),
    (17, 'TRK-10017', 'SwiftShip',    'Columbus',    'Indianapolis',  25.0,  'delivered',  '2025-03-09'),
    (18, 'TRK-10018', 'FastFreight',  'Kansas City', 'St. Louis',     13.7,  'delivered',  '2025-03-09'),
    (19, 'TRK-10019', 'CargoLine',    'Richmond',    'Norfolk',       4.6,   'delivered',  '2025-03-10'),
    (20, 'TRK-10020', 'SwiftShip',    'Tucson',      'Albuquerque',   19.4,  'cancelled',  '2025-03-10');


-- ============================================================================
-- VERSION 2: UPDATE — carrier weight recalibration (+0.5 kg on 5 shipments)
-- ============================================================================
-- The FastFreight scale was off by 0.5 kg. Correct shipments that went
-- through recalibration: ids 3, 8, 12, 16, 19.
UPDATE {{zone_name}}.delta_demos.shipments
SET weight_kg = weight_kg + 0.5
WHERE id IN (3, 8, 12, 16, 19);


-- ============================================================================
-- VERSION 3: DELETE — remove 3 cancelled shipments
-- ============================================================================
-- Cancelled shipments (ids 6, 14, 20) are purged from the active manifest
-- after the 7-day cancellation window expires.
DELETE FROM {{zone_name}}.delta_demos.shipments
WHERE id IN (6, 14, 20);


-- ============================================================================
-- VERSION 4: INSERT — 8 new shipments from the next batch
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.shipments VALUES
    (21, 'TRK-10021', 'FastFreight',  'Newark',       'Hartford',    10.2,  'in_transit', '2025-03-11'),
    (22, 'TRK-10022', 'SwiftShip',    'Louisville',   'Cincinnati',  7.8,   'delivered',  '2025-03-11'),
    (23, 'TRK-10023', 'CargoLine',    'Jacksonville', 'Savannah',    14.5,  'delivered',  '2025-03-12'),
    (24, 'TRK-10024', 'FastFreight',  'Birmingham',   'Montgomery',  21.3,  'in_transit', '2025-03-12'),
    (25, 'TRK-10025', 'SwiftShip',    'Omaha',        'Des Moines',  6.0,   'delivered',  '2025-03-13'),
    (26, 'TRK-10026', 'CargoLine',    'Boise',        'Spokane',     17.9,  'delivered',  '2025-03-13'),
    (27, 'TRK-10027', 'FastFreight',  'Little Rock',  'Tulsa',       11.6,  'delivered',  '2025-03-14'),
    (28, 'TRK-10028', 'SwiftShip',    'Reno',         'Sacramento',  8.4,   'in_transit', '2025-03-14');
