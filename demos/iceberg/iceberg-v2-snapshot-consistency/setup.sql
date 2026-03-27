-- ============================================================================
-- Iceberg V2 Snapshot Consistency — Setup
-- ============================================================================
-- Creates an external table backed by an Iceberg format-version 2 table with
-- 4 snapshots representing a retail inventory ledger lifecycle:
--   Snapshot 1: Initial stock load (80 products)
--   Snapshot 2: Restocking INSERT (20 new products → 100 total)
--   Snapshot 3: Price correction UPDATE (Electronics +8%, merge-on-read)
--   Snapshot 4: Discontinued DELETE (10 products removed → 90 final)
--
-- The reader must process the full metadata chain including position delete
-- files from the UPDATE and DELETE operations to produce the correct 90-row
-- final state.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg V2 table
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg.inventory
USING ICEBERG
LOCATION '{{data_path}}/inventory';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg.inventory TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg.inventory;
