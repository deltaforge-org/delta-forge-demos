-- ============================================================================
-- Card Payment Ledger - Row-Index Cost / Benefit - Setup
-- ============================================================================
-- A fintech card-payment platform keeps two Delta tables:
--
--   1. payments    - the live authorization ledger. Card authorizations
--                    stream in all day. txn_id is a unique, high-cardinality
--                    key, and the rows arrive INTERLEAVED (each daily file
--                    carries txn_ids spread across the whole id range), so
--                    every file's min/max on txn_id overlaps every other
--                    file's. Delta's built-in data skipping therefore CANNOT
--                    prune on txn_id - a point lookup has to open every file.
--                    This is the table where a row-level index earns its keep.
--
--   2. settlements - the end-of-day batch. Each run writes a contiguous block
--                    of settlement_ids in sorted order, so each file holds a
--                    non-overlapping id range. Delta's min/max stats already
--                    prune a settlement_id lookup to one file with no index at
--                    all. This is the table where an index would be redundant.
--
-- The queries.sql then MEASURES the difference with SHOW STATS ACTUAL and
-- DESCRIBE INDEXES instead of just asserting it in prose.
--
-- payments uses deletion vectors so keyed UPDATE/DELETE mark rows in a bitmap
-- instead of rewriting whole files: index + deletion vectors is the standard
-- recipe for cheap single-row mutations on a large Delta table.
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables - demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: payments - streaming authorization ledger (shuffled txn_id)
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.payments WITH FILES;

CREATE DELTA TABLE {{zone_name}}.delta_demos.payments (
    txn_id        BIGINT,
    card_id       BIGINT,
    region        VARCHAR,
    amount        DOUBLE,
    status        VARCHAR,
    authorized_at VARCHAR
) LOCATION 'delta-row-index-payment-ledger/payments'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- 12 daily files, 5 authorizations each = 60 rows. txn_ids are interleaved
-- (stride 12) so every file spans almost the whole id range -> Delta data
-- skipping cannot prune a txn_id lookup.
-- File 1: authorizations for 2026-06-01 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000001, 4007, 'EU', 37.50, 'authorized', '2026-06-01'),
    (90000013, 4011, 'EU', 62.50, 'authorized', '2026-06-01'),
    (90000025, 4015, 'EU', 87.50, 'authorized', '2026-06-01'),
    (90000037, 4019, 'EU', 112.50, 'authorized', '2026-06-01'),
    (90000049, 4003, 'EU', 137.50, 'authorized', '2026-06-01');

-- File 2: authorizations for 2026-06-02 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000002, 4014, 'NA', 50.00, 'authorized', '2026-06-02'),
    (90000014, 4018, 'NA', 75.00, 'authorized', '2026-06-02'),
    (90000026, 4002, 'NA', 100.00, 'authorized', '2026-06-02'),
    (90000038, 4006, 'NA', 125.00, 'authorized', '2026-06-02'),
    (90000050, 4010, 'NA', 25.00, 'declined', '2026-06-02');

-- File 3: authorizations for 2026-06-03 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000003, 4001, 'AP', 62.50, 'authorized', '2026-06-03'),
    (90000015, 4005, 'AP', 87.50, 'authorized', '2026-06-03'),
    (90000027, 4009, 'AP', 112.50, 'authorized', '2026-06-03'),
    (90000039, 4013, 'AP', 137.50, 'authorized', '2026-06-03'),
    (90000051, 4017, 'AP', 37.50, 'authorized', '2026-06-03');

-- File 4: authorizations for 2026-06-04 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000004, 4008, 'EU', 75.00, 'authorized', '2026-06-04'),
    (90000016, 4012, 'EU', 100.00, 'authorized', '2026-06-04'),
    (90000028, 4016, 'EU', 125.00, 'authorized', '2026-06-04'),
    (90000040, 4000, 'EU', 25.00, 'declined', '2026-06-04'),
    (90000052, 4004, 'EU', 50.00, 'authorized', '2026-06-04');

-- File 5: authorizations for 2026-06-05 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000005, 4015, 'NA', 87.50, 'authorized', '2026-06-05'),
    (90000017, 4019, 'NA', 112.50, 'authorized', '2026-06-05'),
    (90000029, 4003, 'NA', 137.50, 'authorized', '2026-06-05'),
    (90000041, 4007, 'NA', 37.50, 'authorized', '2026-06-05'),
    (90000053, 4011, 'NA', 62.50, 'authorized', '2026-06-05');

-- File 6: authorizations for 2026-06-06 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000006, 4002, 'AP', 100.00, 'authorized', '2026-06-06'),
    (90000018, 4006, 'AP', 125.00, 'authorized', '2026-06-06'),
    (90000030, 4010, 'AP', 25.00, 'declined', '2026-06-06'),
    (90000042, 4014, 'AP', 50.00, 'authorized', '2026-06-06'),
    (90000054, 4018, 'AP', 75.00, 'authorized', '2026-06-06');

-- File 7: authorizations for 2026-06-07 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000007, 4009, 'EU', 112.50, 'authorized', '2026-06-07'),
    (90000019, 4013, 'EU', 137.50, 'authorized', '2026-06-07'),
    (90000031, 4017, 'EU', 37.50, 'authorized', '2026-06-07'),
    (90000043, 4001, 'EU', 62.50, 'authorized', '2026-06-07'),
    (90000055, 4005, 'EU', 87.50, 'authorized', '2026-06-07');

-- File 8: authorizations for 2026-06-08 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000008, 4016, 'NA', 125.00, 'authorized', '2026-06-08'),
    (90000020, 4000, 'NA', 25.00, 'declined', '2026-06-08'),
    (90000032, 4004, 'NA', 50.00, 'authorized', '2026-06-08'),
    (90000044, 4008, 'NA', 75.00, 'authorized', '2026-06-08'),
    (90000056, 4012, 'NA', 100.00, 'authorized', '2026-06-08');

-- File 9: authorizations for 2026-06-09 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000009, 4003, 'AP', 137.50, 'authorized', '2026-06-09'),
    (90000021, 4007, 'AP', 37.50, 'authorized', '2026-06-09'),
    (90000033, 4011, 'AP', 62.50, 'authorized', '2026-06-09'),
    (90000045, 4015, 'AP', 87.50, 'authorized', '2026-06-09'),
    (90000057, 4019, 'AP', 112.50, 'authorized', '2026-06-09');

-- File 10: authorizations for 2026-06-10 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000010, 4010, 'EU', 25.00, 'declined', '2026-06-10'),
    (90000022, 4014, 'EU', 50.00, 'authorized', '2026-06-10'),
    (90000034, 4018, 'EU', 75.00, 'authorized', '2026-06-10'),
    (90000046, 4002, 'EU', 100.00, 'authorized', '2026-06-10'),
    (90000058, 4006, 'EU', 125.00, 'authorized', '2026-06-10');

-- File 11: authorizations for 2026-06-11 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000011, 4017, 'NA', 37.50, 'authorized', '2026-06-11'),
    (90000023, 4001, 'NA', 62.50, 'authorized', '2026-06-11'),
    (90000035, 4005, 'NA', 87.50, 'authorized', '2026-06-11'),
    (90000047, 4009, 'NA', 112.50, 'authorized', '2026-06-11'),
    (90000059, 4013, 'NA', 137.50, 'authorized', '2026-06-11');

-- File 12: authorizations for 2026-06-12 (txn_ids interleaved across the day)
INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000012, 4004, 'AP', 50.00, 'authorized', '2026-06-12'),
    (90000024, 4008, 'AP', 75.00, 'authorized', '2026-06-12'),
    (90000036, 4012, 'AP', 100.00, 'authorized', '2026-06-12'),
    (90000048, 4016, 'AP', 125.00, 'authorized', '2026-06-12'),
    (90000060, 4000, 'AP', 25.00, 'declined', '2026-06-12');


-- ============================================================================
-- TABLE: settlements - end-of-day batch (clustered settlement_id)
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.settlements WITH FILES;

CREATE DELTA TABLE {{zone_name}}.delta_demos.settlements (
    settlement_id BIGINT,
    batch_date    VARCHAR,
    region        VARCHAR,
    gross_amount  DOUBLE,
    txn_count     BIGINT
) LOCATION 'delta-row-index-payment-ledger/settlements';

-- 6 settlement runs, 5 rows each = 30 rows. settlement_ids are written in
-- sorted order so each file holds a contiguous, non-overlapping id range ->
-- Delta data skipping prunes a settlement_id lookup to one file by itself.
-- File 1: settlement run 2026-05-01 (settlement_ids contiguous, written in order)
INSERT INTO {{zone_name}}.delta_demos.settlements VALUES
    (800001, '2026-05-01', 'EU', 1050.00, 101),
    (800002, '2026-05-01', 'NA', 1100.00, 102),
    (800003, '2026-05-01', 'AP', 1150.00, 103),
    (800004, '2026-05-01', 'EU', 1200.00, 104),
    (800005, '2026-05-01', 'NA', 1250.00, 105);

-- File 2: settlement run 2026-05-02 (settlement_ids contiguous, written in order)
INSERT INTO {{zone_name}}.delta_demos.settlements VALUES
    (800006, '2026-05-02', 'AP', 1300.00, 106),
    (800007, '2026-05-02', 'EU', 1350.00, 107),
    (800008, '2026-05-02', 'NA', 1400.00, 108),
    (800009, '2026-05-02', 'AP', 1450.00, 109),
    (800010, '2026-05-02', 'EU', 1500.00, 110);

-- File 3: settlement run 2026-05-03 (settlement_ids contiguous, written in order)
INSERT INTO {{zone_name}}.delta_demos.settlements VALUES
    (800011, '2026-05-03', 'NA', 1550.00, 111),
    (800012, '2026-05-03', 'AP', 1600.00, 112),
    (800013, '2026-05-03', 'EU', 1650.00, 113),
    (800014, '2026-05-03', 'NA', 1700.00, 114),
    (800015, '2026-05-03', 'AP', 1750.00, 115);

-- File 4: settlement run 2026-05-04 (settlement_ids contiguous, written in order)
INSERT INTO {{zone_name}}.delta_demos.settlements VALUES
    (800016, '2026-05-04', 'EU', 1800.00, 116),
    (800017, '2026-05-04', 'NA', 1850.00, 117),
    (800018, '2026-05-04', 'AP', 1900.00, 118),
    (800019, '2026-05-04', 'EU', 1950.00, 119),
    (800020, '2026-05-04', 'NA', 2000.00, 120);

-- File 5: settlement run 2026-05-05 (settlement_ids contiguous, written in order)
INSERT INTO {{zone_name}}.delta_demos.settlements VALUES
    (800021, '2026-05-05', 'AP', 2050.00, 121),
    (800022, '2026-05-05', 'EU', 2100.00, 122),
    (800023, '2026-05-05', 'NA', 2150.00, 123),
    (800024, '2026-05-05', 'AP', 2200.00, 124),
    (800025, '2026-05-05', 'EU', 2250.00, 125);

-- File 6: settlement run 2026-05-06 (settlement_ids contiguous, written in order)
INSERT INTO {{zone_name}}.delta_demos.settlements VALUES
    (800026, '2026-05-06', 'NA', 2300.00, 126),
    (800027, '2026-05-06', 'AP', 2350.00, 127),
    (800028, '2026-05-06', 'EU', 2400.00, 128),
    (800029, '2026-05-06', 'NA', 2450.00, 129),
    (800030, '2026-05-06', 'AP', 2500.00, 130);
