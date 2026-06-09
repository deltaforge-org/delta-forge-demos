-- ============================================================================
-- Delta Duration Arithmetic — Maritime Canal Transit — Setup Script
-- ============================================================================
-- Demonstrates duration arithmetic on VARCHAR timestamps in a maritime context:
--   - 35 vessel transits across 3 canal locks (North, Central, South)
--   - Entry and exit times stored as VARCHAR (NTZ format)
--   - Queries compute transit durations via SUBSTRING + CAST math
--
-- Table created:
--   1. vessel_transit — 35 vessels with entry/exit timestamps and cargo data
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE with VARCHAR timestamp columns
--   3. INSERT 12 rows — North Lock vessels (ids 1-12)
--   4. INSERT 12 rows — Central Lock vessels (ids 13-24)
--   5. INSERT 11 rows — South Lock vessels (ids 25-35)
--   6. UPDATE — set 3 vessels to 'in_transit' status
--   7. UPDATE — set 2 vessels to 'delayed' status
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: vessel_transit — maritime canal lock transit tracking
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.vessel_transit (
    id                  INT,
    vessel_name         VARCHAR,
    vessel_type         VARCHAR,
    lock_name           VARCHAR,
    entry_time          VARCHAR,
    exit_time           VARCHAR,
    cargo_tons          INT,
    pilot_name          VARCHAR,
    direction           VARCHAR,
    status              VARCHAR
) LOCATION 'delta-duration-arithmetic/vessel_transit';


-- ============================================================================
-- STEP 3: INSERT batch 1 — 12 North Lock vessels (ids 1-12)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.vessel_transit VALUES
    (1,  'MV Pacific Star',     'tanker',       'North Lock',   '2025-08-20 01:15:00', '2025-08-20 05:20:00', 85000,  'Capt. R. Mercer',    'inbound',  'completed'),
    (2,  'SS Atlantic Voyager', 'container',    'North Lock',   '2025-08-20 02:30:00', '2025-08-20 05:35:00', 52000,  'Capt. J. Holm',      'outbound', 'completed'),
    (3,  'MV Iron Monarch',     'bulk_carrier', 'North Lock',   '2025-08-20 03:45:00', '2025-08-20 07:15:00', 67000,  'Capt. D. Santos',    'inbound',  'completed'),
    (4,  'MS Coral Princess',   'passenger',    'North Lock',   '2025-08-20 05:00:00', '2025-08-20 07:00:00', 8500,   'Capt. A. Petrov',    'outbound', 'completed'),
    (5,  'MV Golden Horizon',   'tanker',       'North Lock',   '2025-08-20 06:20:00', '2025-08-20 11:00:00', 110000, 'Capt. L. Nakamura',  'inbound',  'completed'),
    (6,  'SS Northern Crown',   'container',    'North Lock',   '2025-08-20 07:45:00', '2025-08-20 10:20:00', 45000,  'Capt. F. Dubois',    'outbound', 'completed'),
    (7,  'MV Caspian Breeze',   'bulk_carrier', 'North Lock',   '2025-08-20 09:10:00', '2025-08-20 12:25:00', 72000,  'Capt. S. Okafor',    'inbound',  'completed'),
    (8,  'MS Silver Wave',      'passenger',    'North Lock',   '2025-08-20 10:30:00', '2025-08-20 12:05:00', 6200,   'Capt. M. Reyes',     'outbound', 'completed'),
    (9,  'MV Titan Express',    'tanker',       'North Lock',   '2025-08-20 12:00:00', '2025-08-20 16:25:00', 95000,  'Capt. T. Lindgren',  'inbound',  'completed'),
    (10, 'SS Orient Pearl',     'container',    'North Lock',   '2025-08-20 14:15:00', '2025-08-20 17:35:00', 61000,  'Capt. K. Andersen',  'outbound', 'completed'),
    (11, 'MV Cape Thunder',     'bulk_carrier', 'North Lock',   '2025-08-20 16:30:00', '2025-08-20 20:40:00', 78000,  'Capt. R. Mercer',    'inbound',  'completed'),
    (12, 'MS Azure Spirit',     'passenger',    'North Lock',   '2025-08-20 18:45:00', '2025-08-20 21:05:00', 12000,  'Capt. J. Holm',      'outbound', 'completed');


-- ============================================================================
-- STEP 4: INSERT batch 2 — 12 Central Lock vessels (ids 13-24)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.vessel_transit VALUES
    (13, 'MV Emerald Tide',     'tanker',       'Central Lock', '2025-08-20 00:30:00', '2025-08-20 05:20:00', 102000, 'Capt. D. Santos',    'outbound', 'completed'),
    (14, 'SS Pacific Meridian', 'container',    'Central Lock', '2025-08-20 02:00:00', '2025-08-20 04:55:00', 48000,  'Capt. A. Petrov',    'inbound',  'completed'),
    (15, 'MV Coastal Ranger',   'bulk_carrier', 'Central Lock', '2025-08-20 03:15:00', '2025-08-20 07:05:00', 58000,  'Capt. L. Nakamura',  'outbound', 'completed'),
    (16, 'MS Sapphire Dawn',    'passenger',    'Central Lock', '2025-08-20 04:50:00', '2025-08-20 06:35:00', 9800,   'Capt. F. Dubois',    'inbound',  'completed'),
    (17, 'MV Red Falcon',       'tanker',       'Central Lock', '2025-08-20 06:15:00', '2025-08-20 10:35:00', 88000,  'Capt. S. Okafor',    'outbound', 'completed'),
    (18, 'SS Global Venture',   'container',    'Central Lock', '2025-08-20 08:20:00', '2025-08-20 12:00:00', 71000,  'Capt. M. Reyes',     'inbound',  'completed'),
    (19, 'MV Horizon Bulk',     'bulk_carrier', 'Central Lock', '2025-08-20 10:00:00', '2025-08-20 13:00:00', 54000,  'Capt. T. Lindgren',  'outbound', 'completed'),
    (20, 'MS Ocean Jewel',      'passenger',    'Central Lock', '2025-08-20 11:40:00', '2025-08-20 13:50:00', 11500,  'Capt. K. Andersen',  'inbound',  'completed'),
    (21, 'MV Nordic Carrier',   'tanker',       'Central Lock', '2025-08-20 13:30:00', '2025-08-20 18:30:00', 118000, 'Capt. R. Mercer',    'outbound', 'completed'),
    (22, 'SS Magellan Route',   'container',    'Central Lock', '2025-08-20 15:45:00', '2025-08-20 18:55:00', 63000,  'Capt. J. Holm',      'inbound',  'completed'),
    (23, 'MV Granite Peak',     'bulk_carrier', 'Central Lock', '2025-08-20 17:20:00', '2025-08-20 21:20:00', 81000,  'Capt. D. Santos',    'outbound', 'completed'),
    (24, 'MS Windward Isle',    'passenger',    'Central Lock', '2025-08-20 19:30:00', '2025-08-20 21:55:00', 13500,  'Capt. A. Petrov',    'inbound',  'completed');


-- ============================================================================
-- STEP 5: INSERT batch 3 — 11 South Lock vessels (ids 25-35)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.vessel_transit VALUES
    (25, 'MV Strait Runner',    'tanker',       'South Lock',   '2025-08-20 01:45:00', '2025-08-20 06:15:00', 92000,  'Capt. L. Nakamura',  'inbound',  'completed'),
    (26, 'SS Delta Clipper',    'container',    'South Lock',   '2025-08-20 04:10:00', '2025-08-20 06:55:00', 41000,  'Capt. F. Dubois',    'outbound', 'completed'),
    (27, 'MV Harbour Sentinel', 'bulk_carrier', 'South Lock',   '2025-08-20 05:50:00', '2025-08-20 09:25:00', 63000,  'Capt. S. Okafor',    'inbound',  'completed'),
    (28, 'MS Riviera Sun',      'passenger',    'South Lock',   '2025-08-20 07:30:00', '2025-08-20 09:00:00', 5500,   'Capt. M. Reyes',     'outbound', 'completed'),
    (29, 'MV Petro Vanguard',   'tanker',       'South Lock',   '2025-08-20 09:45:00', '2025-08-20 14:30:00', 105000, 'Capt. T. Lindgren',  'inbound',  'completed'),
    (30, 'SS Trade Compass',    'container',    'South Lock',   '2025-08-20 11:55:00', '2025-08-20 15:20:00', 57000,  'Capt. K. Andersen',  'outbound', 'completed'),
    (31, 'MV Coral Bulk',       'bulk_carrier', 'South Lock',   '2025-08-20 14:00:00', '2025-08-20 17:10:00', 69000,  'Capt. R. Mercer',    'inbound',  'completed'),
    (32, 'MS Adriatic Star',    'passenger',    'South Lock',   '2025-08-20 16:15:00', '2025-08-20 18:05:00', 7800,   'Capt. J. Holm',      'outbound', 'completed'),
    (33, 'MV Bering Trader',    'tanker',       'South Lock',   '2025-08-20 18:30:00', '2025-08-20 22:45:00', 98000,  'Capt. D. Santos',    'inbound',  'completed'),
    (34, 'SS Longitude One',    'container',    'South Lock',   '2025-08-20 20:40:00', '2025-08-21 00:30:00', 66000,  'Capt. A. Petrov',    'outbound', 'completed'),
    (35, 'MV Fjord Viking',     'bulk_carrier', 'South Lock',   '2025-08-20 22:15:00', '2025-08-21 02:45:00', 75000,  'Capt. L. Nakamura',  'inbound',  'completed');


-- ============================================================================
-- STEP 6: UPDATE — set 3 vessels to 'in_transit' status
-- ============================================================================
UPDATE {{zone_name}}.delta_demos.vessel_transit SET status = 'in_transit' WHERE id = 10;

UPDATE {{zone_name}}.delta_demos.vessel_transit SET status = 'in_transit' WHERE id = 22;

UPDATE {{zone_name}}.delta_demos.vessel_transit SET status = 'in_transit' WHERE id = 34;


-- ============================================================================
-- STEP 7: UPDATE — set 2 vessels to 'delayed' status
-- ============================================================================
UPDATE {{zone_name}}.delta_demos.vessel_transit SET status = 'delayed' WHERE id = 7;

UPDATE {{zone_name}}.delta_demos.vessel_transit SET status = 'delayed' WHERE id = 28;
