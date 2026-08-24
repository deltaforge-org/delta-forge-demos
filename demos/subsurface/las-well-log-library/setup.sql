-- ============================================================================
-- Well Log Library Consolidation - Setup Script
-- ============================================================================
-- An operator takes a position in Dutch acreage and receives the regulator's
-- public log package with it. Seven wells logged between 1958 and 1990, in
-- LAS 2.0, arriving in two tranches.
--
-- The logs are real. They are NLOG composite logs from the Netherlands, as
-- the OSDU Forum redistributes them; see ATTRIBUTION.md in the parent folder.
-- Their curve sets differ by vintage because the tools did: the three
-- Wassenaar wells from 1958 to 1963 carry a gamma ray and a neutron and
-- nothing else, the wells from 1969 on carry the full triple combo, and one
-- 1990 well carries only a gamma ray.
--
--   11 March   MED-01 (1958), WAS-25 (1961), MED-05 (1963)   1585 depth steps
--   12 March   D15-01 (1969), K08-02 (1972), GRW-01 (1979),
--              L09-06 (1990)                                  2791 depth steps
--
-- One external table spans the whole library even though the curve sets
-- differ, because the reader merges the per-file schemas into a union: a well
-- whose tool never recorded a sonic gets NULL for it, which is the truthful
-- answer and not a zero.
--
--   1. log_files    external, DISCOVER over the landing folder
--   2. log_library  DELTA, the curated library
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.log_library
    COMMENT 'NLOG well log package read in place from LAS, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- LAS is sectioned text, identified by its `~V` and `~C` section markers
-- rather than by a magic number.
--
-- The well-information section rides on every row as constant columns:
-- well_well, well_fld, well_strt and the rest. That is the whole reason a
-- table over a log library is queryable by well or field with no join to a
-- header table, and it is why the curated table below takes the well name
-- from well_well rather than from the file name. A file renamed in transit
-- still says which well it holds.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.log_library.log_files;

DISCOVER {{zone_name}}.log_library.log_files
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta library
-- ----------------------------------------------------------------------------
-- Every curve is nullable because the library genuinely does not have them
-- all. Four of the seven wells were logged before a compensated density tool
-- was on the truck, and a curated table that filled those with zero would
-- make a 1958 well look like a rock with no density rather than a well that
-- was never measured for one.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.log_library.log_library (
    well          VARCHAR,
    field         VARCHAR,
    vintage       INTEGER,
    delivered_on  VARCHAR,
    source_file   VARCHAR,
    dept          DOUBLE,
    gr            DOUBLE,
    dt            DOUBLE,
    rhob          DOUBLE,
    drho          DOUBLE,
    nphi          DOUBLE
) LOCATION '{{data_subdir}}/curated/log_library';
