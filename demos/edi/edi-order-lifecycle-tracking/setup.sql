-- ============================================================================
-- EDI Order Lifecycle Tracking — Setup Script
-- ============================================================================
-- Creates a single unified EDI table with materialized business fields from
-- every lifecycle-relevant transaction type. This enables cross-document
-- traceability queries that join/correlate different X12 transaction types
-- within one table — tracking orders from creation through fulfillment.
--
-- Transaction types covered (14 files total):
--   850 — Purchase Order            (3 files)  BEG segment
--   855 — PO Acknowledgment         (1 file)   BAK segment
--   856 — Ship Notice               (1 file)   BSN segment
--   857 — Shipment & Billing Notice (1 file)   BHT segment
--   810 — Invoice                   (5 files)  BIG segment
--   861 — Receiving Advice          (1 file)   BRA segment
--   824 — Application Advice        (1 file)   BGN segment
--   997 — Functional Acknowledgment (1 file)   (envelope only)
--
-- Materialized fields span all lifecycle stages:
--   BEG_1/3/5    — PO purpose, number, date (850)
--   BIG_1/2      — Invoice date, number (810)
--   BAK_1/3/4    — Ack status, PO ref, ack date (855)
--   BSN_2/3      — Shipment ID, date (856)
--   BRA_1        — Receipt ID (861)
--   BGN_2        — Advice reference (824)
--   N1_1/2       — First party code and name (all types)
--   CTT_1        — Line item count (all types)
--   REF_1/2      — First reference type and value (all types)
--
-- Variables (auto-injected by DeltaForge):
--   data_path     — Local or cloud path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--   zone_name     — Target zone name (defaults to 'external')
--
-- Naming convention: zone_name.format.table
--   zone   = {{zone_name}}  (defaults to 'external')
--   schema = 'edi'          (the file format)
--   table  = lifecycle_tracking
-- ============================================================================
-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.edi_demos
    COMMENT 'EDI transaction-backed external tables';
-- ============================================================================
-- TABLE: lifecycle_tracking — Cross-document lifecycle fields
-- ============================================================================
-- A single table that materializes key business fields from every lifecycle-
-- relevant segment type. Because all 14 files are read into one table, queries
-- can correlate across transaction types without joins — the ST_1 column
-- identifies the document type, and the materialized fields from each segment
-- are NULL for transaction types that don't contain that segment.
--
-- This is the key differentiator: instead of one table per transaction type,
-- a unified table enables lifecycle tracking with simple WHERE/GROUP BY/CASE.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.lifecycle_tracking
USING EDI
LOCATION 'edi-order-lifecycle-tracking/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "beg_1", "beg_3", "beg_5",
            "big_1", "big_2",
            "bak_1", "bak_3", "bak_4",
            "bsn_2", "bsn_3",
            "bra_1",
            "bgn_2",
            "n1_1", "n1_2",
            "ctt_1",
            "ref_1", "ref_2"
        ]
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
