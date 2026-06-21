-- ============================================================================
-- Demo: Order-Event Auto-Onboarding, DISCOVER over Avro
-- Feature: DISCOVER auto-detects Avro from its container magic header
--          (Obj\x01), reads the embedded Avro schema, and registers an
--          external table in one statement. No hand-written CREATE EXTERNAL
--          TABLE, no OPTIONS, no schema typed by hand.
-- ============================================================================
--
-- Real-world story: a retail platform sinks its order-event stream from Kafka
-- to Avro files in object storage. Each Avro object carries its own embedded
-- schema in the file header, so the column names and types travel with the
-- data. The order team wants those files queryable without anyone first
-- transcribing the schema into DDL.
--
-- This is the Avro entry in the DISCOVER category. Rather than hand-write a
-- CREATE EXTERNAL TABLE with column types, this demo points DISCOVER at the
-- folder and lets it do the work: it reads the actual bytes, recognises the
-- Avro container magic (Obj\x01), parses the embedded record schema, derives
-- the column names (order_id, customer_id, ... already lowercase snake_case
-- in the writer schema), and registers the table USING AVRO.
--
-- This file declares ONLY the zone and schema. The external table itself is
-- created by DISCOVER in queries.sql (both PRINT and EXECUTE modes are shown
-- there), mirroring the discover/json-clickstream demo.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables: demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.discover_demos
    COMMENT 'Tables auto-registered by the DISCOVER command';
