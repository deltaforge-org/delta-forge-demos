-- ============================================================================
-- EDI 850 Purchase Orders — Aliased Columns + To-JSON Line Items
-- ============================================================================
-- Real-world scenario: Wholesale-distributor purchase-order inbox.
--
-- A wholesale distributor receives X12 850 Purchase Orders from a dozen
-- retail trading partners. Analysts want immediately-readable column names
-- (rather than opaque BEG_2 / PO1_7) and the FULL list of line items per PO
-- as a JSON array — no falling back to df_transaction_json walks.
--
-- Engine features exercised:
--   1. aliasFormat = 'friendly_column'
--      - Materialized columns are renamed to "<FriendlyName>_<numeric>"
--        using the per-segment X12 V5010 element dictionary.
--      - Example column names produced (verified against
--        delta-forge-edi/src/edi_metadata.rs V5010 dictionary):
--          beg_1 -> purchaseordertypecode_beg_1
--          beg_2 -> purchaseordernumber_beg_2
--          beg_3 -> releasenumber_beg_3
--          beg_5 -> contractnumber_beg_5
--          n1_1  -> memberreportingcategoryname_n1_1
--          n1_2  -> identificationcodequalifier_n1_2
--          po1_1 -> po1_1               (empty dict entry, falls back numeric)
--          po1_2 -> unitorbasisformeasurementcode_po1_2
--          po1_3 -> unitprice_po1_3
--          po1_4 -> basisofunitpricecode_po1_4
--          po1_6 -> productserviceid_po1_6
--          po1_7 -> productserviceidqualifier_po1_7
--      - The element catalog is fixed-position; some friendly names sit one
--        position off vs the X12 spec's BEG01/BEG02 numbering (handler comment
--        in edi_handler.rs::lookup_friendly_name acknowledges this). The
--        numeric suffix preserves stable addressing regardless.
--
--   2. repeating_segment_mode = 'to_json'
--      - All occurrences of repeating segments (N1 parties: BY/VN/ST,
--        and PO1 line items) collapse into JSON-array values per column.
--      - max_repeating_segments=50 covers our largest PO (6 PO1s).
--      - Analysts use json_array_length(...) for counts and json_extract /
--        unnest for per-line analysis.
--
-- Data: 4 synthetic 850 transactions, one per .edi file, with 3-6 PO1 line
-- items each (18 total line items across the corpus).
--
-- Variables (auto-injected by DeltaForge):
--   data_path     — Local or cloud path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--   zone_name     — Target zone name (defaults to 'external')
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.commerce
    COMMENT 'Wholesale-distributor commerce datasets (POs, invoices, ASNs)';


-- ============================================================================
-- STEP 2: Bronze external table — typed line-item ingest
-- ============================================================================
-- materialized_paths uses the engine's 1-based numeric element naming:
--   beg_1..beg_5 — Beginning Segment header fields
--   n1_1, n1_2   — Name segment entity code + party name
--   po1_1..po1_7 — Line-item element values
-- After alias rewrite, the actual stored column names look like
-- "purchaseordernumber_beg_2", "memberreportingcategoryname_n1_1",
-- "productserviceidqualifier_po1_7" — see queries.sql for examples.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.commerce.purchase_orders
USING EDI
LOCATION '850-purchase-orders-aliased/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "aliasFormat": "friendly_column",
        "repeating_segment_mode": "to_json",
        "max_repeating_segments": 50,
        "materialized_paths": [
            "beg_1", "beg_2", "beg_3", "beg_4", "beg_5",
            "n1_1", "n1_2",
            "po1_1", "po1_2", "po1_3", "po1_4", "po1_6", "po1_7"
        ]
    }',
    file_metadata = '{"columns":["df_file_name"]}'
);


-- ============================================================================
-- STEP 3: Schema detection
-- ============================================================================

DETECT SCHEMA FOR TABLE {{zone_name}}.commerce.purchase_orders;
