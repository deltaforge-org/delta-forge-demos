-- ==========================================================================
-- Delta VACUUM with Deletion Vectors: Why Space Is Not Reclaimed: Setup
-- ==========================================================================
-- A warehouse order-fulfilment table where order statuses change constantly
-- (received -> picked -> packed -> shipped) and a few orders get cancelled.
--
-- This table ENABLES deletion vectors (delta.enableDeletionVectors = 'true').
-- That is the whole point of the demo: with deletion vectors, an UPDATE or
-- DELETE does NOT rewrite the whole Parquet file. Instead the engine keeps the
-- original file ACTIVE and records the affected row positions in a tiny .bin
-- deletion-vector sidecar (updated rows are appended to a small new file). No
-- whole data file is ever dereferenced, so nothing becomes an orphan and a
-- later VACUUM has nothing to reclaim: the dead rows still sit INSIDE the live
-- Parquet files. queries.sql proves this (VACUUM reclaims 0 files), explains
-- why, and then shows OPTIMIZE as the lever that finally frees the space.
--
-- Contrast with the delta-vacuum-storage-savings demo, which disables deletion
-- vectors so the same mutations rewrite whole files and VACUUM reclaims them.
--
-- Operations:
--   1. CREATE DELTA TABLE (deletion vectors ON) + INSERT 30 fulfilment orders
--   2. UPDATE  - advance 10 orders to 'shipped'   (deletion-vector rewrite)
--   3. UPDATE  - advance 6 orders to 'packed'      (deletion-vector rewrite)
--   4. DELETE  - cancel 5 orders                   (deletion-vector delete)
--
-- After setup the table has 25 live rows but several deletion vectors and the
-- original data files are all still on disk and still referenced.
--
-- Tables created:
--   1. fulfillment_orders - 25 live rows after the deletion-vector mutations
-- ==========================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables: demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ==========================================================================
-- TABLE: fulfillment_orders: warehouse order fulfilment (deletion vectors ON)
-- ==========================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.fulfillment_orders (
    order_id    INT,
    customer    VARCHAR,
    warehouse   VARCHAR,
    item        VARCHAR,
    quantity    INT,
    status      VARCHAR,
    updated_at  VARCHAR
) LOCATION 'delta-vacuum-deletion-vectors/fulfillment_orders'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');


-- STEP 2: Insert 30 fulfilment orders across 3 warehouses (all 'received')
INSERT INTO {{zone_name}}.delta_demos.fulfillment_orders VALUES
    (1,  'Acme Corp',    'WH-East',    'Widget A',   4, 'received', '2025-03-01'),
    (2,  'Beta LLC',     'WH-East',    'Widget B',   2, 'received', '2025-03-01'),
    (3,  'Coral Inc',    'WH-East',    'Gadget C',   7, 'received', '2025-03-01'),
    (4,  'Delta Co',     'WH-East',    'Gadget D',   1, 'received', '2025-03-01'),
    (5,  'Echo Ltd',     'WH-East',    'Widget A',   3, 'received', '2025-03-01'),
    (6,  'Foxtrot SA',   'WH-East',    'Gadget C',   5, 'received', '2025-03-01'),
    (7,  'Gamma GmbH',   'WH-East',    'Widget B',   2, 'received', '2025-03-01'),
    (8,  'Hotel Pty',    'WH-East',    'Gadget D',   6, 'received', '2025-03-01'),
    (9,  'Indigo BV',    'WH-East',    'Widget A',   1, 'received', '2025-03-01'),
    (10, 'Juliet Corp',  'WH-East',    'Gadget C',   8, 'received', '2025-03-01'),
    (11, 'Kilo Systems', 'WH-West',    'Widget B',   4, 'received', '2025-03-02'),
    (12, 'Lima Digital', 'WH-West',    'Gadget D',   3, 'received', '2025-03-02'),
    (13, 'Mike Labs',    'WH-West',    'Widget A',   2, 'received', '2025-03-02'),
    (14, 'November AI',  'WH-West',    'Gadget C',   9, 'received', '2025-03-02'),
    (15, 'Oscar Cloud',  'WH-West',    'Widget B',   1, 'received', '2025-03-02'),
    (16, 'Papa Retail',  'WH-West',    'Gadget D',   5, 'received', '2025-03-02'),
    (17, 'Quebec Foods', 'WH-West',    'Widget A',   3, 'received', '2025-03-02'),
    (18, 'Romeo Parts',  'WH-West',    'Gadget C',   2, 'received', '2025-03-02'),
    (19, 'Sierra Mfg',   'WH-West',    'Widget B',   7, 'received', '2025-03-02'),
    (20, 'Tango Tools',  'WH-West',    'Gadget D',   4, 'received', '2025-03-02'),
    (21, 'Uniform Co',   'WH-Central', 'Widget A',   6, 'received', '2025-03-03'),
    (22, 'Victor Ltd',   'WH-Central', 'Gadget C',   1, 'received', '2025-03-03'),
    (23, 'Whiskey Inc',  'WH-Central', 'Widget B',   3, 'received', '2025-03-03'),
    (24, 'Xray Corp',    'WH-Central', 'Gadget D',   2, 'received', '2025-03-03'),
    (25, 'Yankee SA',    'WH-Central', 'Widget A',   5, 'received', '2025-03-03'),
    (26, 'Zulu GmbH',    'WH-Central', 'Gadget C',   4, 'received', '2025-03-03'),
    (27, 'Alpha Pty',    'WH-Central', 'Widget B',   8, 'received', '2025-03-03'),
    (28, 'Bravo BV',     'WH-Central', 'Gadget D',   2, 'received', '2025-03-03'),
    (29, 'Charlie Co',   'WH-Central', 'Widget A',   1, 'received', '2025-03-03'),
    (30, 'Golf Systems', 'WH-Central', 'Gadget C',   3, 'received', '2025-03-03');


-- ==========================================================================
-- STEP 3: UPDATE: advance the first 10 orders to 'shipped'
-- ==========================================================================
-- Because deletion vectors are enabled, this does NOT rewrite the data file.
-- The 10 old rows are marked deleted in a .bin sidecar and the 10 new 'shipped'
-- rows are written to a small companion file. The original file stays ACTIVE.
UPDATE {{zone_name}}.delta_demos.fulfillment_orders
SET status = 'shipped', updated_at = '2025-03-05'
WHERE order_id <= 10;


-- ==========================================================================
-- STEP 4: UPDATE: advance orders 11-16 to 'packed'
-- ==========================================================================
-- Another deletion-vector rewrite: more .bin sidecars, still no orphaned file.
UPDATE {{zone_name}}.delta_demos.fulfillment_orders
SET status = 'packed', updated_at = '2025-03-05'
WHERE order_id BETWEEN 11 AND 16;


-- ==========================================================================
-- STEP 5: DELETE: cancel 5 orders (ids 25-29)
-- ==========================================================================
-- A deletion-vector delete: the 5 rows are recorded in a .bin sidecar. The
-- data file that holds them stays ACTIVE (it still holds the other rows), so
-- the physical file is never orphaned. The bytes for these 5 rows remain on
-- disk inside the live file until an OPTIMIZE rewrites it.
DELETE FROM {{zone_name}}.delta_demos.fulfillment_orders
WHERE order_id IN (25, 26, 27, 28, 29);
