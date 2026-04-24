-- ============================================================================
-- Delta RESTORE + VACUUM — Incident Response Lifecycle — Setup Script
-- ============================================================================
-- Simulates a cold-storage facility IoT monitoring system.
-- Sensors across three rooms report temperature and humidity readings.
--
-- Version history after setup:
--   V0: CREATE TABLE sensor_readings
--   V1: INSERT 30 baseline readings (6 sensors, 3 rooms, 5 time slots)
--
-- Rooms:
--   room_a — Cold Storage (target: -18°C)
--   room_b — Chilled Storage (target: 4°C)
--   room_c — Ambient Storage (target: 22°C)
--
-- Tables created:
--   1. sensor_readings — 30 rows of normal IoT sensor data
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: sensor_readings — Cold-storage facility IoT monitoring
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sensor_readings (
    id              INT,
    sensor_id       VARCHAR,
    room            VARCHAR,
    temperature     DOUBLE,
    humidity        DOUBLE,
    reading_time    VARCHAR,
    status          VARCHAR
) LOCATION 'sensor_readings';


-- ============================================================================
-- V1: INSERT 30 baseline readings — all normal, across 3 rooms
-- ============================================================================
-- Room A: Cold Storage (target: -18°C), sensors S01, S02
-- Room B: Chilled Storage (target: 4°C), sensors S03, S04
-- Room C: Ambient Storage (target: 22°C), sensors S05, S06
INSERT INTO {{zone_name}}.delta_demos.sensor_readings VALUES
    (1,  'S01', 'room_a', -18.2, 45.0, '2025-06-01 08:00', 'normal'),
    (2,  'S01', 'room_a', -17.8, 44.5, '2025-06-01 08:15', 'normal'),
    (3,  'S01', 'room_a', -18.5, 45.2, '2025-06-01 08:30', 'normal'),
    (4,  'S01', 'room_a', -18.0, 44.8, '2025-06-01 08:45', 'normal'),
    (5,  'S01', 'room_a', -17.6, 45.1, '2025-06-01 09:00', 'normal'),
    (6,  'S02', 'room_a', -18.3, 45.3, '2025-06-01 08:00', 'normal'),
    (7,  'S02', 'room_a', -17.9, 44.7, '2025-06-01 08:15', 'normal'),
    (8,  'S02', 'room_a', -18.6, 45.5, '2025-06-01 08:30', 'normal'),
    (9,  'S02', 'room_a', -18.1, 44.9, '2025-06-01 08:45', 'normal'),
    (10, 'S02', 'room_a', -17.7, 45.0, '2025-06-01 09:00', 'normal'),
    (11, 'S03', 'room_b',   3.8, 60.0, '2025-06-01 08:00', 'normal'),
    (12, 'S03', 'room_b',   4.1, 59.5, '2025-06-01 08:15', 'normal'),
    (13, 'S03', 'room_b',   3.9, 60.2, '2025-06-01 08:30', 'normal'),
    (14, 'S03', 'room_b',   4.2, 59.8, '2025-06-01 08:45', 'normal'),
    (15, 'S03', 'room_b',   4.0, 60.1, '2025-06-01 09:00', 'normal'),
    (16, 'S04', 'room_b',   3.7, 60.3, '2025-06-01 08:00', 'normal'),
    (17, 'S04', 'room_b',   4.3, 59.7, '2025-06-01 08:15', 'normal'),
    (18, 'S04', 'room_b',   3.6, 60.4, '2025-06-01 08:30', 'normal'),
    (19, 'S04', 'room_b',   4.4, 59.6, '2025-06-01 08:45', 'normal'),
    (20, 'S04', 'room_b',   3.5, 60.5, '2025-06-01 09:00', 'normal'),
    (21, 'S05', 'room_c',  21.8, 35.0, '2025-06-01 08:00', 'normal'),
    (22, 'S05', 'room_c',  22.1, 34.5, '2025-06-01 08:15', 'normal'),
    (23, 'S05', 'room_c',  21.5, 35.2, '2025-06-01 08:30', 'normal'),
    (24, 'S05', 'room_c',  22.3, 34.8, '2025-06-01 08:45', 'normal'),
    (25, 'S05', 'room_c',  21.9, 35.1, '2025-06-01 09:00', 'normal'),
    (26, 'S06', 'room_c',  22.0, 35.3, '2025-06-01 08:00', 'normal'),
    (27, 'S06', 'room_c',  21.7, 34.7, '2025-06-01 08:15', 'normal'),
    (28, 'S06', 'room_c',  22.4, 35.5, '2025-06-01 08:30', 'normal'),
    (29, 'S06', 'room_c',  21.6, 34.9, '2025-06-01 08:45', 'normal'),
    (30, 'S06', 'room_c',  22.2, 35.0, '2025-06-01 09:00', 'normal');
