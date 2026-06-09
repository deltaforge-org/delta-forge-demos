-- ============================================================================
-- Delta Type Widening — IoT Fleet Counter Overflow — Setup Script
-- ============================================================================
-- Creates a device telemetry table with INT counters and inserts 25 baseline
-- readings. The type widening (ALTER TABLE ALTER COLUMN TYPE), counter
-- accumulation (UPDATE), and BIGINT-range inserts happen in queries.sql.
--
-- Real-world scenario: An IoT platform monitors network gateways, sensors,
-- cameras, and routers. Early on, INT (max 2.1 billion) is sufficient for
-- event counters. As the fleet scales over months, cumulative counters
-- approach and exceed INT range — requiring type widening to BIGINT.
--
-- Tables created:
--   1. device_telemetry — 25 device readings with INT-range counters
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: device_telemetry — IoT fleet monitoring readings
-- ============================================================================
-- event_count and bytes_sent start as INT — sufficient for daily readings
-- but will overflow as cumulative counters grow over months.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.device_telemetry (
    id              INT,
    device_id       VARCHAR,
    region          VARCHAR,
    event_count     INT,
    bytes_sent      INT,
    avg_latency     DOUBLE,
    status          VARCHAR,
    reported_date   VARCHAR
) LOCATION 'delta-type-widening/device_telemetry';


-- STEP 2: Insert 25 baseline device readings (all values fit comfortably in INT)
INSERT INTO {{zone_name}}.delta_demos.device_telemetry VALUES
    (1,  'GW-NYC-001',   'us-east',  45000,   1200000,  12.45, 'active', '2025-01-15'),
    (2,  'GW-NYC-002',   'us-east',  62000,   1800000,  15.23, 'active', '2025-01-15'),
    (3,  'GW-NYC-003',   'us-east',  38000,   980000,   8.91,  'active', '2025-01-15'),
    (4,  'GW-LON-001',   'eu-west',  51000,   1500000,  22.67, 'active', '2025-01-15'),
    (5,  'GW-LON-002',   'eu-west',  47000,   1350000,  19.80, 'active', '2025-01-15'),
    (6,  'GW-LON-003',   'eu-west',  33000,   870000,   25.12, 'active', '2025-01-15'),
    (7,  'GW-TKY-001',   'ap-east',  72000,   2100000,  31.55, 'active', '2025-01-15'),
    (8,  'GW-TKY-002',   'ap-east',  58000,   1650000,  28.90, 'active', '2025-01-15'),
    (9,  'GW-TKY-003',   'ap-east',  41000,   1100000,  35.44, 'active', '2025-01-15'),
    (10, 'GW-SYD-001',   'ap-south', 29000,   750000,   42.18, 'active', '2025-01-15'),
    (11, 'GW-SYD-002',   'ap-south', 35000,   920000,   38.75, 'active', '2025-01-15'),
    (12, 'GW-SYD-003',   'ap-south', 22000,   580000,   45.60, 'active', '2025-01-15'),
    (13, 'SNS-TEMP-001', 'us-east',  150000,  450000,   5.20,  'active', '2025-01-15'),
    (14, 'SNS-TEMP-002', 'us-east',  142000,  426000,   5.80,  'active', '2025-01-15'),
    (15, 'SNS-TEMP-003', 'eu-west',  138000,  414000,   6.10,  'active', '2025-01-15'),
    (16, 'SNS-HUM-001',  'eu-west',  125000,  375000,   4.90,  'active', '2025-01-15'),
    (17, 'SNS-HUM-002',  'ap-east',  131000,  393000,   5.50,  'active', '2025-01-15'),
    (18, 'SNS-HUM-003',  'ap-east',  119000,  357000,   6.30,  'active', '2025-01-15'),
    (19, 'CAM-001',      'us-east',  88000,   2400000,  18.20, 'active', '2025-01-15'),
    (20, 'CAM-002',      'eu-west',  95000,   2650000,  20.15, 'active', '2025-01-15'),
    (21, 'CAM-003',      'ap-east',  76000,   2050000,  24.80, 'active', '2025-01-15'),
    (22, 'RTR-001',      'us-east',  210000,  5200000,  3.20,  'active', '2025-01-15'),
    (23, 'RTR-002',      'eu-west',  195000,  4800000,  4.10,  'active', '2025-01-15'),
    (24, 'RTR-003',      'ap-east',  225000,  5500000,  2.90,  'active', '2025-01-15'),
    (25, 'RTR-004',      'ap-south', 180000,  4300000,  3.80,  'active', '2025-01-15');
