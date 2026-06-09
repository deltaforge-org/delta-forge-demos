-- ============================================================================
-- Iceberg V2 — Airline Loyalty Window Analytics — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg V2 table.
-- DeltaForge reads the Iceberg metadata chain directly:
-- metadata.json -> manifest list -> manifests -> Parquet data files.
--
-- Dataset: 60 airline frequent flyer members across 4 tiers (Bronze,
-- Silver, Gold, Platinum) and 5 home airports (JFK, LAX, ORD, ATL, DFW)
-- with 9 columns: member_id, member_name, tier, miles_ytd, flights_ytd,
-- spend_ytd, join_date, home_airport, last_flight_date.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg V2 table
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.loyalty_members
USING ICEBERG
LOCATION 'iceberg-v2-window-analytics/loyalty_members';

