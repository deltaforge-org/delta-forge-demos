-- ============================================================================
-- Iceberg V3 — Clinical Lab NULL Edge Cases — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg V3 table.
-- Delta Forge reads the Iceberg metadata chain directly.
--
-- Dataset: 50 clinical lab results with intentional NULLs:
--   - result_value: 5 NULLs (pending tests)
--   - unit: 2 NULLs (non-standard tests)
--   - reference_low/high: 3 NULLs each (experimental tests)
--   - is_critical: 12 NULLs (not yet assessed or undeterminable)
--   - lab_technician: 7 NULLs (automated analyzer runs)
--   - notes: 37 NULLs (most tests have no notes)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg V3 table
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg.lab_results
USING ICEBERG
LOCATION '{{data_path}}/lab_results';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg.lab_results TO USER {{current_user}};
