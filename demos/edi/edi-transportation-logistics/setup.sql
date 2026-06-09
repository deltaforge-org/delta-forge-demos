-- ============================================================================
-- EDI Transportation & Logistics -- Setup Script
-- ============================================================================
-- Ingests 12 real-world X12 EDI transactions spanning the full freight
-- lifecycle: load tendering (204/990), freight invoicing (210), shipment
-- tracking (214), rail transport (404), warehouse operations (940/945),
-- payment remittance (820), and price catalogs (832).
--
-- Transaction types covered:
--   204 -- Motor Carrier Load Tender (1 message)
--   210 -- Freight Invoice           (2 messages)
--   214 -- Shipment Status           (3 messages)
--   404 -- Rail Carrier Shipment     (1 message)
--   820 -- Payment Order/Remittance  (1 message)
--   832 -- Price/Sales Catalog       (2 messages)
--   945 -- Warehouse Shipping Advice (1 message)
--   990 -- Load Tender Response      (1 message)
--
-- Two tables demonstrate different views of the same transaction feed:
--   1. logistics_messages      -- Compact: ISA/GS/ST headers + full JSON
--   2. logistics_materialized  -- Enriched: headers + key logistics fields
--
-- Variables (auto-injected by DeltaForge):
--   data_path     -- Local or cloud path where demo data files were downloaded
--   current_user  -- Username of the current logged-in user
--   zone_name     -- Target zone name (defaults to 'external')
--
-- Naming convention: zone_name.format.table
--   zone   = {{zone_name}}  (defaults to 'external')
--   schema = 'edi'          (the file format)
--   table  = object name
-- ============================================================================
-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables -- demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.edi_demos
    COMMENT 'EDI transaction-backed external tables';
-- ============================================================================
-- TABLE 1: logistics_messages -- Compact view (12 transactions)
-- ============================================================================
-- Default X12 output: ISA envelope fields (ISA_1 through ISA_16),
-- GS functional group fields (GS_1 through GS_8), ST transaction header
-- (ST_1 transaction set ID, ST_2 control number), df_transaction_json
-- (full transaction as JSON), and df_transaction_id (unique hash).
-- Use df_transaction_json with JSON functions for deep segment access.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.logistics_messages
USING EDI
LOCATION 'edi-transportation-logistics/*.edi'
OPTIONS (
    edi_config = '{"ediFormat": "x12"}',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: logistics_materialized -- Key logistics fields extracted
-- ============================================================================
-- Uses materialized_paths to extract commonly-queried transportation and
-- logistics fields as first-class columns alongside the default ISA/GS/ST
-- headers and JSON output.
--
-- Materialized columns:
--   B2_2  -- Standard Carrier Alpha Code (SCAC) from Load Tender 204
--   B2_4  -- Shipment identification number from Load Tender 204
--   B3_2  -- Invoice number from Freight Invoice 210
--   B3_3  -- Shipment identification number from Freight Invoice 210
--   B10_1 -- Reference identification (shipment ID) from Status 214
--   B10_2 -- Shipment identification number (BOL) from Status 214
--   N1_1  -- Entity identifier code (SH=shipper, CN=consignee, BT=bill-to)
--   N1_2  -- Party name (company or individual)
--   L3_1  -- Weight (total shipment weight)
--   L3_5  -- Total charges amount
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.logistics_materialized
USING EDI
LOCATION 'edi-transportation-logistics/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "b2_2", "b2_4",
            "b3_2", "b3_3",
            "b10_1", "b10_2",
            "n1_1", "n1_2",
            "l3_1", "l3_5"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
