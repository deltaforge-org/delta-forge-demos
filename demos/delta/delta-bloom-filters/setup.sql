-- ============================================================================
-- Delta Bloom Filters — Probabilistic Index for Fast Lookups — Setup Script
-- ============================================================================
-- Demonstrates how bloom filter indexes on transaction IDs accelerate
-- point lookups in a payment processing system:
--   - Bloom filters tell the engine which files can't contain a given txn_id
--   - Skips irrelevant data files entirely for exact-match queries
--   - TBLPROPERTIES configure data skipping across all indexed columns
--
-- Tables created:
--   1. transaction_log — 60 transactions across 3 batches
--
-- Operations performed:
--   1. CREATE DELTA TABLE with TBLPROPERTIES
--   2. INSERT batch 1 — 30 rows (online purchases)
--   3. INSERT batch 2 — 15 rows (in-store purchases)
--   4. INSERT batch 3 — 15 rows (refunds)
--   5. UPDATE — set 5 transaction statuses to 'disputed'
--   6. ANALYZE — build bloom filter indexes on txn_id
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: transaction_log — payment transactions with bloom filter on txn_id
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.transaction_log (
    id          INT,
    txn_id      VARCHAR,
    user_id     VARCHAR,
    merchant    VARCHAR,
    amount      DOUBLE,
    category    VARCHAR,
    status      VARCHAR,
    txn_date    VARCHAR
) LOCATION 'delta-bloom-filters/transaction_log'
TBLPROPERTIES ('delta.dataSkippingNumIndexedCols'='8');


-- STEP 2: Batch 1 — 30 online purchases
INSERT INTO {{zone_name}}.delta_demos.transaction_log VALUES
    (1,  'TXN-0001', 'USR-101', 'TechMart Online',      149.99, 'electronics',    'completed', '2025-03-01'),
    (2,  'TXN-0002', 'USR-102', 'FreshGrocer',           45.80,  'groceries',      'completed', '2025-03-01'),
    (3,  'TXN-0003', 'USR-103', 'CloudKitchen',          22.50,  'dining',         'completed', '2025-03-01'),
    (4,  'TXN-0004', 'USR-104', 'SkyWings Travel',      380.00,  'travel',         'completed', '2025-03-01'),
    (5,  'TXN-0005', 'USR-105', 'StreamFlix',            15.99,  'entertainment',  'completed', '2025-03-01'),
    (6,  'TXN-0006', 'USR-106', 'GadgetWorld',          299.95,  'electronics',    'completed', '2025-03-02'),
    (7,  'TXN-0007', 'USR-107', 'OrganicBasket',         67.30,  'groceries',      'completed', '2025-03-02'),
    (8,  'TXN-0008', 'USR-108', 'BurgerBarn',            18.75,  'dining',         'completed', '2025-03-02'),
    (9,  'TXN-0009', 'USR-109', 'JetSet Holidays',      950.00,  'travel',         'completed', '2025-03-02'),
    (10, 'TXN-0010', 'USR-110', 'GameVault',             59.99,  'entertainment',  'completed', '2025-03-02'),
    (11, 'TXN-0011', 'USR-111', 'MegaElectro',          124.50,  'electronics',    'completed', '2025-03-03'),
    (12, 'TXN-0012', 'USR-112', 'DailyMart',             33.20,  'groceries',      'completed', '2025-03-03'),
    (13, 'TXN-0013', 'USR-113', 'PastaPlace',            28.00,  'dining',         'completed', '2025-03-03'),
    (14, 'TXN-0014', 'USR-114', 'TrainTravel Co',       175.00,  'travel',         'completed', '2025-03-03'),
    (15, 'TXN-0015', 'USR-115', 'MusicStream',            9.99,  'entertainment',  'completed', '2025-03-03'),
    (16, 'TXN-0016', 'USR-116', 'PhoneHub',             799.00,  'electronics',    'completed', '2025-03-04'),
    (17, 'TXN-0017', 'USR-117', 'GreenLeaf Grocer',      52.45,  'groceries',      'completed', '2025-03-04'),
    (18, 'TXN-0018', 'USR-118', 'SushiExpress',          42.00,  'dining',         'completed', '2025-03-04'),
    (19, 'TXN-0019', 'USR-119', 'CruiseLine Intl',     1200.00,  'travel',         'completed', '2025-03-04'),
    (20, 'TXN-0020', 'USR-120', 'CinemaPlus',            14.50,  'entertainment',  'completed', '2025-03-04'),
    (21, 'TXN-0021', 'USR-121', 'LaptopDirect',         549.99,  'electronics',    'completed', '2025-03-05'),
    (22, 'TXN-0022', 'USR-122', 'FarmFresh Market',      78.90,  'groceries',      'completed', '2025-03-05'),
    (23, 'TXN-0023', 'USR-123', 'TacoFiesta',            16.25,  'dining',         'completed', '2025-03-05'),
    (24, 'TXN-0024', 'USR-124', 'BusTravel Express',     45.00,  'travel',         'completed', '2025-03-05'),
    (25, 'TXN-0025', 'USR-125', 'BookNook Digital',      12.99,  'entertainment',  'completed', '2025-03-05'),
    (26, 'TXN-0026', 'USR-126', 'SmartHome Devices',    189.00,  'electronics',    'completed', '2025-03-06'),
    (27, 'TXN-0027', 'USR-127', 'QuickBite Snacks',      8.50,   'groceries',      'completed', '2025-03-06'),
    (28, 'TXN-0028', 'USR-128', 'WokMaster',             31.00,  'dining',         'completed', '2025-03-06'),
    (29, 'TXN-0029', 'USR-129', 'AirHopper Flights',    425.00,  'travel',         'completed', '2025-03-06'),
    (30, 'TXN-0030', 'USR-130', 'PodcastPro',             5.99,  'entertainment',  'completed', '2025-03-06');


-- ============================================================================
-- STEP 3: Batch 2 — 15 in-store purchases
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.transaction_log
SELECT * FROM (VALUES
    (31, 'TXN-0031', 'USR-131', 'MegaMall Electronics',  349.99, 'electronics',    'completed', '2025-03-07'),
    (32, 'TXN-0032', 'USR-132', 'SuperStore Groceries',   92.15, 'groceries',      'completed', '2025-03-07'),
    (33, 'TXN-0033', 'USR-133', 'Downtown Bistro',        55.00, 'dining',         'completed', '2025-03-07'),
    (34, 'TXN-0034', 'USR-134', 'CityTravel Agency',     280.00, 'travel',         'completed', '2025-03-08'),
    (35, 'TXN-0035', 'USR-135', 'Arcade Palace',          25.00, 'entertainment',  'completed', '2025-03-08'),
    (36, 'TXN-0036', 'USR-136', 'CircuitCity Store',     199.50, 'electronics',    'completed', '2025-03-08'),
    (37, 'TXN-0037', 'USR-137', 'Wholesome Foods',        64.80, 'groceries',      'completed', '2025-03-09'),
    (38, 'TXN-0038', 'USR-138', 'Golden Dragon',          38.50, 'dining',         'completed', '2025-03-09'),
    (39, 'TXN-0039', 'USR-139', 'Harbor Cruises',        150.00, 'travel',         'completed', '2025-03-09'),
    (40, 'TXN-0040', 'USR-140', 'FunZone Bowling',        18.00, 'entertainment',  'completed', '2025-03-10'),
    (41, 'TXN-0041', 'USR-141', 'PowerTools Depot',      275.00, 'electronics',    'completed', '2025-03-10'),
    (42, 'TXN-0042', 'USR-142', 'Corner Deli',            11.50, 'groceries',      'completed', '2025-03-10'),
    (43, 'TXN-0043', 'USR-143', 'Seafood Shack',          47.25, 'dining',         'completed', '2025-03-11'),
    (44, 'TXN-0044', 'USR-144', 'Metro Bus Pass',         85.00, 'travel',         'completed', '2025-03-11'),
    (45, 'TXN-0045', 'USR-145', 'LiveShow Tickets',       65.00, 'entertainment',  'completed', '2025-03-11')
) AS t(id, txn_id, user_id, merchant, amount, category, status, txn_date);


-- ============================================================================
-- STEP 4: Batch 3 — 15 refunds
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.transaction_log
SELECT * FROM (VALUES
    (46, 'TXN-0046', 'USR-101', 'TechMart Online',      -149.99, 'electronics',    'refunded', '2025-03-12'),
    (47, 'TXN-0047', 'USR-106', 'GadgetWorld',          -299.95, 'electronics',    'refunded', '2025-03-12'),
    (48, 'TXN-0048', 'USR-109', 'JetSet Holidays',      -950.00, 'travel',         'refunded', '2025-03-12'),
    (49, 'TXN-0049', 'USR-112', 'DailyMart',             -33.20, 'groceries',      'refunded', '2025-03-13'),
    (50, 'TXN-0050', 'USR-116', 'PhoneHub',             -799.00, 'electronics',    'refunded', '2025-03-13'),
    (51, 'TXN-0051', 'USR-119', 'CruiseLine Intl',     -1200.00, 'travel',         'refunded', '2025-03-13'),
    (52, 'TXN-0052', 'USR-121', 'LaptopDirect',         -549.99, 'electronics',    'refunded', '2025-03-14'),
    (53, 'TXN-0053', 'USR-103', 'CloudKitchen',          -22.50, 'dining',         'refunded', '2025-03-14'),
    (54, 'TXN-0054', 'USR-108', 'BurgerBarn',            -18.75, 'dining',         'refunded', '2025-03-14'),
    (55, 'TXN-0055', 'USR-115', 'MusicStream',            -9.99, 'entertainment',  'refunded', '2025-03-15'),
    (56, 'TXN-0056', 'USR-120', 'CinemaPlus',            -14.50, 'entertainment',  'refunded', '2025-03-15'),
    (57, 'TXN-0057', 'USR-125', 'BookNook Digital',      -12.99, 'entertainment',  'refunded', '2025-03-15'),
    (58, 'TXN-0058', 'USR-127', 'QuickBite Snacks',       -8.50, 'groceries',      'refunded', '2025-03-16'),
    (59, 'TXN-0059', 'USR-132', 'SuperStore Groceries',  -92.15, 'groceries',      'refunded', '2025-03-16'),
    (60, 'TXN-0060', 'USR-137', 'Wholesome Foods',       -64.80, 'groceries',      'refunded', '2025-03-16')
) AS t(id, txn_id, user_id, merchant, amount, category, status, txn_date);


-- ============================================================================
-- STEP 5: UPDATE — set 5 transaction statuses to 'disputed'
-- ============================================================================
-- Disputes on a mix of online and in-store transactions
UPDATE {{zone_name}}.delta_demos.transaction_log
SET status = 'disputed'
WHERE txn_id IN ('TXN-0004', 'TXN-0016', 'TXN-0024', 'TXN-0034', 'TXN-0041');


-- ============================================================================
-- STEP 6: ANALYZE — build bloom filter indexes on txn_id
-- ============================================================================
-- This rewrites data files to embed Parquet-native bloom filters on txn_id.
-- Point lookups (WHERE txn_id = 'X') can then skip files that definitely
-- do not contain the value, dramatically reducing I/O.
ANALYZE TABLE {{zone_name}}.delta_demos.transaction_log
    BLOOM FILTER COLUMNS (txn_id);
