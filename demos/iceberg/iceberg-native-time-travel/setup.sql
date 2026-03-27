-- ============================================================================
-- Iceberg Native Time Travel (Stock Prices) — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg V2 table
-- containing stock price data that has undergone 4 mutations:
--   Snapshot 1: Initial 120 rows (20 tickers x 6 trading days)
--   Snapshot 2: UPDATE — tech sector prices +5% (earnings beat correction)
--   Snapshot 3: INSERT — 30 new IPO records (5 new tickers x 6 days)
--   Snapshot 4: DELETE — 12 delisted records (COP, SLB x 6 days)
--
-- The current snapshot (final state) contains 138 rows. Iceberg V2
-- merge-on-read mode uses position delete files for UPDATE and DELETE
-- operations, which Delta Forge resolves during query execution.
--
-- Schema: ticker, company_name, price, volume, market_cap, sector, trade_date
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg V2 table
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- Delta Forge parses metadata.json to discover schema, snapshots, and data files.
-- Position delete files are resolved automatically during merge-on-read.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg.stock_prices
USING ICEBERG
LOCATION '{{data_path}}/stock_prices';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg.stock_prices TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg.stock_prices;
