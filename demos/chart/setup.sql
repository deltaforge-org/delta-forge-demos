-- ==========================================================================
-- Mira's Mercantile — Retail Analytics Chart Gallery (SETUP)
-- ==========================================================================
-- Feature: End-to-end showcase of every CREATE CHART visualization type.
--
-- Data model:
--   sales_daily   — 80 rows (4 stores x 4 categories x 5 weekdays)
--   stock_prices  — 10 rows (weekly OHLC for parent ticker MIRA)
--
-- All values are deterministic. Proofs in queries.sql are computed by
-- generate.py against the exact INSERT VALUES below.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables — retail chart gallery demo';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.retail
    COMMENT 'Mira''s Mercantile retail chain — daily sales and parent stock';

-- --------------------------------------------------------------------------
-- Table: sales_daily — one row per (store, category, weekday)
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.sales_daily (
    txn_id        INT,
    txn_date      DATE,
    store_name    VARCHAR,
    category      VARCHAR,
    units_sold    INT,
    revenue       DOUBLE,
    discount_pct  DOUBLE,
    customers     INT
) LOCATION '{{data_path}}/sales_daily';

INSERT INTO {{zone_name}}.retail.sales_daily (txn_id, txn_date, store_name, category, units_sold, revenue, discount_pct, customers) VALUES
    (1, '2026-03-02', 'Downtown', 'Apparel', 30, 1080.00, 0.10, 15),
    (2, '2026-03-02', 'Downtown', 'Electronics', 10, 1900.00, 0.05, 5),
    (3, '2026-03-02', 'Downtown', 'Home', 25, 1275.00, 0.15, 12),
    (4, '2026-03-02', 'Downtown', 'Beauty', 32, 640.00, 0.20, 16),
    (5, '2026-03-02', 'Mall', 'Apparel', 38, 1350.00, 0.10, 19),
    (6, '2026-03-02', 'Mall', 'Electronics', 12, 2280.00, 0.05, 6),
    (7, '2026-03-02', 'Mall', 'Home', 29, 1445.00, 0.15, 14),
    (8, '2026-03-02', 'Mall', 'Beauty', 44, 880.00, 0.20, 22),
    (9, '2026-03-02', 'Airport', 'Apparel', 23, 810.00, 0.10, 11),
    (10, '2026-03-02', 'Airport', 'Electronics', 8, 1520.00, 0.05, 4),
    (11, '2026-03-02', 'Airport', 'Home', 17, 850.00, 0.15, 8),
    (12, '2026-03-02', 'Airport', 'Beauty', 24, 480.00, 0.20, 12),
    (13, '2026-03-02', 'Beach', 'Apparel', 25, 900.00, 0.10, 12),
    (14, '2026-03-02', 'Beach', 'Electronics', 7, 1235.00, 0.05, 3),
    (15, '2026-03-02', 'Beach', 'Home', 20, 1020.00, 0.15, 10),
    (16, '2026-03-02', 'Beach', 'Beauty', 28, 560.00, 0.20, 14),
    (17, '2026-03-03', 'Downtown', 'Apparel', 29, 1026.00, 0.10, 14),
    (18, '2026-03-03', 'Downtown', 'Electronics', 10, 1805.00, 0.05, 5),
    (19, '2026-03-03', 'Downtown', 'Home', 24, 1211.25, 0.15, 12),
    (20, '2026-03-03', 'Downtown', 'Beauty', 31, 608.00, 0.20, 15),
    (21, '2026-03-03', 'Mall', 'Apparel', 36, 1282.50, 0.10, 18),
    (22, '2026-03-03', 'Mall', 'Electronics', 12, 2166.00, 0.05, 6),
    (23, '2026-03-03', 'Mall', 'Home', 27, 1372.75, 0.15, 13),
    (24, '2026-03-03', 'Mall', 'Beauty', 42, 836.00, 0.20, 21),
    (25, '2026-03-03', 'Airport', 'Apparel', 22, 769.50, 0.10, 11),
    (26, '2026-03-03', 'Airport', 'Electronics', 8, 1444.00, 0.05, 4),
    (27, '2026-03-03', 'Airport', 'Home', 16, 807.50, 0.15, 8),
    (28, '2026-03-03', 'Airport', 'Beauty', 23, 456.00, 0.20, 11),
    (29, '2026-03-03', 'Beach', 'Apparel', 24, 855.00, 0.10, 12),
    (30, '2026-03-03', 'Beach', 'Electronics', 7, 1173.25, 0.05, 3),
    (31, '2026-03-03', 'Beach', 'Home', 19, 969.00, 0.15, 9),
    (32, '2026-03-03', 'Beach', 'Beauty', 27, 532.00, 0.20, 13),
    (33, '2026-03-04', 'Downtown', 'Apparel', 32, 1134.00, 0.10, 16),
    (34, '2026-03-04', 'Downtown', 'Electronics', 11, 1995.00, 0.05, 5),
    (35, '2026-03-04', 'Downtown', 'Home', 27, 1338.75, 0.15, 13),
    (36, '2026-03-04', 'Downtown', 'Beauty', 34, 672.00, 0.20, 17),
    (37, '2026-03-04', 'Mall', 'Apparel', 40, 1417.50, 0.10, 20),
    (38, '2026-03-04', 'Mall', 'Electronics', 13, 2394.00, 0.05, 6),
    (39, '2026-03-04', 'Mall', 'Home', 30, 1517.25, 0.15, 15),
    (40, '2026-03-04', 'Mall', 'Beauty', 47, 924.00, 0.20, 23),
    (41, '2026-03-04', 'Airport', 'Apparel', 24, 850.50, 0.10, 12),
    (42, '2026-03-04', 'Airport', 'Electronics', 9, 1596.00, 0.05, 4),
    (43, '2026-03-04', 'Airport', 'Home', 18, 892.50, 0.15, 9),
    (44, '2026-03-04', 'Airport', 'Beauty', 26, 504.00, 0.20, 13),
    (45, '2026-03-04', 'Beach', 'Apparel', 27, 945.00, 0.10, 13),
    (46, '2026-03-04', 'Beach', 'Electronics', 7, 1296.75, 0.05, 3),
    (47, '2026-03-04', 'Beach', 'Home', 21, 1071.00, 0.15, 10),
    (48, '2026-03-04', 'Beach', 'Beauty', 30, 588.00, 0.20, 15),
    (49, '2026-03-05', 'Downtown', 'Apparel', 33, 1161.60, 0.12, 16),
    (50, '2026-03-05', 'Downtown', 'Electronics', 11, 2046.00, 0.07, 5),
    (51, '2026-03-05', 'Downtown', 'Home', 28, 1369.50, 0.17, 14),
    (52, '2026-03-05', 'Downtown', 'Beauty', 36, 686.40, 0.22, 18),
    (53, '2026-03-05', 'Mall', 'Apparel', 42, 1452.00, 0.12, 21),
    (54, '2026-03-05', 'Mall', 'Electronics', 14, 2455.20, 0.07, 7),
    (55, '2026-03-05', 'Mall', 'Home', 32, 1552.10, 0.17, 16),
    (56, '2026-03-05', 'Mall', 'Beauty', 49, 943.80, 0.22, 24),
    (57, '2026-03-05', 'Airport', 'Apparel', 25, 871.20, 0.12, 12),
    (58, '2026-03-05', 'Airport', 'Electronics', 9, 1636.80, 0.07, 4),
    (59, '2026-03-05', 'Airport', 'Home', 19, 913.00, 0.17, 9),
    (60, '2026-03-05', 'Airport', 'Beauty', 27, 514.80, 0.22, 13),
    (61, '2026-03-05', 'Beach', 'Apparel', 28, 968.00, 0.12, 14),
    (62, '2026-03-05', 'Beach', 'Electronics', 8, 1329.90, 0.07, 4),
    (63, '2026-03-05', 'Beach', 'Home', 22, 1095.60, 0.17, 11),
    (64, '2026-03-05', 'Beach', 'Beauty', 31, 600.60, 0.22, 15),
    (65, '2026-03-06', 'Downtown', 'Apparel', 36, 1267.20, 0.12, 18),
    (66, '2026-03-06', 'Downtown', 'Electronics', 12, 2232.00, 0.07, 6),
    (67, '2026-03-06', 'Downtown', 'Home', 30, 1494.00, 0.17, 15),
    (68, '2026-03-06', 'Downtown', 'Beauty', 39, 748.80, 0.22, 19),
    (69, '2026-03-06', 'Mall', 'Apparel', 45, 1584.00, 0.12, 22),
    (70, '2026-03-06', 'Mall', 'Electronics', 15, 2678.40, 0.07, 7),
    (71, '2026-03-06', 'Mall', 'Home', 34, 1693.20, 0.17, 17),
    (72, '2026-03-06', 'Mall', 'Beauty', 53, 1029.60, 0.22, 26),
    (73, '2026-03-06', 'Airport', 'Apparel', 27, 950.40, 0.12, 13),
    (74, '2026-03-06', 'Airport', 'Electronics', 10, 1785.60, 0.07, 5),
    (75, '2026-03-06', 'Airport', 'Home', 20, 996.00, 0.17, 10),
    (76, '2026-03-06', 'Airport', 'Beauty', 29, 561.60, 0.22, 14),
    (77, '2026-03-06', 'Beach', 'Apparel', 30, 1056.00, 0.12, 15),
    (78, '2026-03-06', 'Beach', 'Electronics', 8, 1450.80, 0.07, 4),
    (79, '2026-03-06', 'Beach', 'Home', 24, 1195.20, 0.17, 12),
    (80, '2026-03-06', 'Beach', 'Beauty', 34, 655.20, 0.22, 17);

-- --------------------------------------------------------------------------
-- Table: stock_prices — weekly OHLC for parent ticker MIRA
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.stock_prices (
    week_start   DATE,
    open_price   DOUBLE,
    high_price   DOUBLE,
    low_price    DOUBLE,
    close_price  DOUBLE
) LOCATION '{{data_path}}/stock_prices';

INSERT INTO {{zone_name}}.retail.stock_prices (week_start, open_price, high_price, low_price, close_price) VALUES
    ('2026-01-05', 52.00, 54.50, 51.20, 53.80),
    ('2026-01-12', 53.80, 55.40, 52.90, 54.60),
    ('2026-01-19', 54.60, 55.10, 52.30, 52.70),
    ('2026-01-26', 52.70, 53.20, 49.80, 50.10),
    ('2026-02-02', 50.10, 51.00, 48.50, 50.70),
    ('2026-02-09', 50.70, 53.60, 50.40, 53.20),
    ('2026-02-16', 53.20, 56.80, 53.00, 56.40),
    ('2026-02-23', 56.40, 58.20, 55.90, 57.90),
    ('2026-03-02', 57.90, 59.70, 57.50, 59.10),
    ('2026-03-09', 59.10, 61.40, 58.80, 60.80);

-- --------------------------------------------------------------------------
-- Schema Detection & Permissions
-- --------------------------------------------------------------------------

DETECT SCHEMA FOR TABLE {{zone_name}}.retail.sales_daily;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.stock_prices;

GRANT ADMIN ON TABLE {{zone_name}}.retail.sales_daily TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.retail.stock_prices TO USER {{current_user}};
