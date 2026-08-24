-- ============================================================================
-- Cleanup: SEG-D Field Record QC
-- ============================================================================
-- record_1043 goes first because it points at one file inside the landing
-- folder that field_records covers as a whole.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.seismic_acquisition.record_1043 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.seismic_acquisition.field_records WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.seismic_acquisition.trace_inventory WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.seismic_acquisition;
