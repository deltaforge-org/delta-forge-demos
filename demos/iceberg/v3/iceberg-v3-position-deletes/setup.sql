-- ============================================================================
-- Iceberg V3 Position Delete Files — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg table that
-- uses Parquet position delete files to retract erroneous trades.
-- DeltaForge reads the Iceberg metadata chain directly:
-- metadata.json -> manifest list -> manifests -> Parquet data + delete files.
--
-- The table was originally created as Iceberg V2, then upgraded to V3 —
-- a common real-world migration scenario. The position delete file marks
-- 24 rows from a malfunctioning algorithm (ALGO-X99) for removal.
--
-- Dataset: 480 equity trades across 4 exchanges (NYSE, NASDAQ, LSE, TSE)
-- with 24 position-deleted erroneous trades, leaving 456 valid trades.
-- Columns: trade_id, exchange, trader, symbol, side, quantity, price,
-- notional, is_erroneous, trade_date.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg table with position deletes
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- DeltaForge parses metadata.json to discover schema, data files, and position
-- delete files automatically. The table was upgraded from V2 to V3 format.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.equity_trades
USING ICEBERG
LOCATION 'iceberg-v3-position-deletes/equity_trades';

