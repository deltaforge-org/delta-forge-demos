-- ============================================================================
-- Monthly Production Allocation - Setup Script
-- ============================================================================
-- An operator files monthly produced volumes per facility with the regulator
-- as PRODML, one document per facility per reporting year. The production
-- engineer loads them to track decline, watch the water cut, and total the
-- year for the annual statement.
--
--   11 March   SLEIPNER-A, GUDRUN-B    24 monthly periods
--   12 March   UTGARD-C                12 monthly periods
--
-- The volumes follow a hyperbolic decline with a rising water cut and a
-- rising gas/oil ratio, which is what a maturing field does. They are
-- internally consistent rather than decorative: the water volume follows from
-- the oil volume and the cut, so the crossover the demo finds is a real
-- feature of the data rather than a value planted to be found.
--
--   1. production_reports  external, DISCOVER over the landing folder
--   2. production_history  DELTA, one row per facility per month
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.production
    COMMENT 'PRODML monthly returns read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- PRODML is XML, read through the XML engine under a curated profile rather
-- than a parser of its own. The profile names //productVolume as the row and
-- explodes //period, so one row comes out per reporting month with the
-- facility's own fields repeated down it.
--
-- One facility per document keeps that explosion single-valued, which is also
-- how a regulator's monthly return is filed: a facility reports for itself.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.production.production_reports;

DISCOVER {{zone_name}}.production.production_reports
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta table
-- ----------------------------------------------------------------------------
-- The flattened names are the document's full element paths, so a month's oil
-- volume arrives as `product_volume_facility_period_volume`. Renaming them is
-- most of what this layer is for, along with pulling the month out of the
-- period's start timestamp so the year can be ordered and grouped.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.production.production_history (
    facility       VARCHAR,
    facility_uid   VARCHAR,
    product        VARCHAR,
    delivered_on   VARCHAR,
    source_file    VARCHAR,
    period_start   VARCHAR,
    month          INTEGER,
    oil_m3         DOUBLE,
    water_m3       DOUBLE,
    gas_m3         DOUBLE
) LOCATION '{{data_subdir}}/curated/production_history';
