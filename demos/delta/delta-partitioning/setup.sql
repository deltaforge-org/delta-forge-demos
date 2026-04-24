-- ============================================================================
-- Delta Partitioning — Setup Script
-- ============================================================================
-- Creates the orders table PARTITIONED BY (region) with 80 baseline rows
-- (20 per region: North, South, East, West).
--
-- Table: orders — 80 rows, partitioned by region
--
-- Known values per region:
--   North: ids 1-20,  products rotate through 5 items, amounts 25-500
--   South: ids 21-40, same pattern
--   East:  ids 41-60, same pattern
--   West:  ids 61-80, same pattern
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: orders — 80 orders across 4 regions
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.orders (
    id         INT,
    customer   VARCHAR,
    product    VARCHAR,
    amount     DOUBLE,
    order_date VARCHAR,
    region     VARCHAR
) LOCATION 'orders'
PARTITIONED BY (region);


-- North region: ids 1-20
INSERT INTO {{zone_name}}.delta_demos.orders VALUES
    (1,  'Customer_01', 'Widget A',    120.00, '2024-01-05', 'North'),
    (2,  'Customer_02', 'Widget B',    250.00, '2024-01-06', 'North'),
    (3,  'Customer_03', 'Gadget X',    75.50,  '2024-01-07', 'North'),
    (4,  'Customer_04', 'Gadget Y',    180.00, '2024-01-08', 'North'),
    (5,  'Customer_05', 'Tool Z',      45.00,  '2024-01-09', 'North'),
    (6,  'Customer_06', 'Widget A',    120.00, '2024-01-10', 'North'),
    (7,  'Customer_07', 'Widget B',    310.00, '2024-01-11', 'North'),
    (8,  'Customer_08', 'Gadget X',    95.00,  '2024-01-12', 'North'),
    (9,  'Customer_09', 'Gadget Y',    200.00, '2024-01-13', 'North'),
    (10, 'Customer_10', 'Tool Z',      30.00,  '2024-01-14', 'North'),
    (11, 'Customer_11', 'Widget A',    155.00, '2024-01-15', 'North'),
    (12, 'Customer_12', 'Widget B',    275.00, '2024-01-16', 'North'),
    (13, 'Customer_13', 'Gadget X',    88.00,  '2024-01-17', 'North'),
    (14, 'Customer_14', 'Gadget Y',    420.00, '2024-01-18', 'North'),
    (15, 'Customer_15', 'Tool Z',      35.00,  '2024-01-19', 'North'),
    (16, 'Customer_16', 'Widget A',    140.00, '2024-01-20', 'North'),
    (17, 'Customer_17', 'Widget B',    500.00, '2024-01-21', 'North'),
    (18, 'Customer_18', 'Gadget X',    67.00,  '2024-01-22', 'North'),
    (19, 'Customer_19', 'Gadget Y',    225.00, '2024-01-23', 'North'),
    (20, 'Customer_20', 'Tool Z',      25.00,  '2024-01-24', 'North');

-- South region: ids 21-40
INSERT INTO {{zone_name}}.delta_demos.orders VALUES
    (21, 'Customer_21', 'Widget A',    130.00, '2024-02-01', 'South'),
    (22, 'Customer_22', 'Widget B',    260.00, '2024-02-02', 'South'),
    (23, 'Customer_23', 'Gadget X',    80.00,  '2024-02-03', 'South'),
    (24, 'Customer_24', 'Gadget Y',    190.00, '2024-02-04', 'South'),
    (25, 'Customer_25', 'Tool Z',      40.00,  '2024-02-05', 'South'),
    (26, 'Customer_26', 'Widget A',    135.00, '2024-02-06', 'South'),
    (27, 'Customer_27', 'Widget B',    320.00, '2024-02-07', 'South'),
    (28, 'Customer_28', 'Gadget X',    99.00,  '2024-02-08', 'South'),
    (29, 'Customer_29', 'Gadget Y',    210.00, '2024-02-09', 'South'),
    (30, 'Customer_30', 'Tool Z',      28.00,  '2024-02-10', 'South'),
    (31, 'Customer_31', 'Widget A',    160.00, '2024-02-11', 'South'),
    (32, 'Customer_32', 'Widget B',    290.00, '2024-02-12', 'South'),
    (33, 'Customer_33', 'Gadget X',    92.00,  '2024-02-13', 'South'),
    (34, 'Customer_34', 'Gadget Y',    440.00, '2024-02-14', 'South'),
    (35, 'Customer_35', 'Tool Z',      33.00,  '2024-02-15', 'South'),
    (36, 'Customer_36', 'Widget A',    145.00, '2024-02-16', 'South'),
    (37, 'Customer_37', 'Widget B',    480.00, '2024-02-17', 'South'),
    (38, 'Customer_38', 'Gadget X',    70.00,  '2024-02-18', 'South'),
    (39, 'Customer_39', 'Gadget Y',    235.00, '2024-02-19', 'South'),
    (40, 'Customer_40', 'Tool Z',      22.00,  '2024-02-20', 'South');

-- East region: ids 41-60
INSERT INTO {{zone_name}}.delta_demos.orders VALUES
    (41, 'Customer_41', 'Widget A',    115.00, '2024-03-01', 'East'),
    (42, 'Customer_42', 'Widget B',    240.00, '2024-03-02', 'East'),
    (43, 'Customer_43', 'Gadget X',    72.00,  '2024-03-03', 'East'),
    (44, 'Customer_44', 'Gadget Y',    175.00, '2024-03-04', 'East'),
    (45, 'Customer_45', 'Tool Z',      48.00,  '2024-03-05', 'East'),
    (46, 'Customer_46', 'Widget A',    125.00, '2024-03-06', 'East'),
    (47, 'Customer_47', 'Widget B',    305.00, '2024-03-07', 'East'),
    (48, 'Customer_48', 'Gadget X',    90.00,  '2024-03-08', 'East'),
    (49, 'Customer_49', 'Gadget Y',    195.00, '2024-03-09', 'East'),
    (50, 'Customer_50', 'Tool Z',      38.00,  '2024-03-10', 'East'),
    (51, 'Customer_51', 'Widget A',    150.00, '2024-03-11', 'East'),
    (52, 'Customer_52', 'Widget B',    270.00, '2024-03-12', 'East'),
    (53, 'Customer_53', 'Gadget X',    85.00,  '2024-03-13', 'East'),
    (54, 'Customer_54', 'Gadget Y',    410.00, '2024-03-14', 'East'),
    (55, 'Customer_55', 'Tool Z',      32.00,  '2024-03-15', 'East'),
    (56, 'Customer_56', 'Widget A',    138.00, '2024-03-16', 'East'),
    (57, 'Customer_57', 'Widget B',    490.00, '2024-03-17', 'East'),
    (58, 'Customer_58', 'Gadget X',    65.00,  '2024-03-18', 'East'),
    (59, 'Customer_59', 'Gadget Y',    220.00, '2024-03-19', 'East'),
    (60, 'Customer_60', 'Tool Z',      27.00,  '2024-03-20', 'East');

-- West region: ids 61-80
INSERT INTO {{zone_name}}.delta_demos.orders VALUES
    (61, 'Customer_61', 'Widget A',    110.00, '2024-04-01', 'West'),
    (62, 'Customer_62', 'Widget B',    230.00, '2024-04-02', 'West'),
    (63, 'Customer_63', 'Gadget X',    68.00,  '2024-04-03', 'West'),
    (64, 'Customer_64', 'Gadget Y',    170.00, '2024-04-04', 'West'),
    (65, 'Customer_65', 'Tool Z',      42.00,  '2024-04-05', 'West'),
    (66, 'Customer_66', 'Widget A',    118.00, '2024-04-06', 'West'),
    (67, 'Customer_67', 'Widget B',    295.00, '2024-04-07', 'West'),
    (68, 'Customer_68', 'Gadget X',    82.00,  '2024-04-08', 'West'),
    (69, 'Customer_69', 'Gadget Y',    205.00, '2024-04-09', 'West'),
    (70, 'Customer_70', 'Tool Z',      29.00,  '2024-04-10', 'West'),
    (71, 'Customer_71', 'Widget A',    148.00, '2024-04-11', 'West'),
    (72, 'Customer_72', 'Widget B',    280.00, '2024-04-12', 'West'),
    (73, 'Customer_73', 'Gadget X',    78.00,  '2024-04-13', 'West'),
    (74, 'Customer_74', 'Gadget Y',    430.00, '2024-04-14', 'West'),
    (75, 'Customer_75', 'Tool Z',      36.00,  '2024-04-15', 'West'),
    (76, 'Customer_76', 'Widget A',    142.00, '2024-04-16', 'West'),
    (77, 'Customer_77', 'Widget B',    510.00, '2024-04-17', 'West'),
    (78, 'Customer_78', 'Gadget X',    71.00,  '2024-04-18', 'West'),
    (79, 'Customer_79', 'Gadget Y',    215.00, '2024-04-19', 'West'),
    (80, 'Customer_80', 'Tool Z',      24.00,  '2024-04-20', 'West');

