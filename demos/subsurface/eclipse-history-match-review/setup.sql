-- ============================================================================
-- Reservoir Simulation History Match Review - Setup Script
-- ============================================================================
-- A reservoir engineer is history matching a field before a development plan
-- goes in. Each iteration of the match is a full simulation run, and the
-- simulator writes a set of files sharing one base name:
--
--   .EGRID    grid geometry and which cells are active
--   .INIT     static properties, written once at initialisation
--   .UNRST    the restart file, one set of arrays per report step
--   .SMSPEC   the summary specification, naming the well vectors
--
-- Two runs land, a day apart. Run HM12 has the aquifer too weak and its
-- pressure falls away from the observed decline; HM13 strengthens it. Deciding
-- which is closer is the whole job, and it is a SQL question once the files
-- are readable at all.
--
--   11 March   HM12   four files, 18624 array elements
--   12 March   HM13   four files, 18624 array elements
--
--   1. sim_arrays     external, DISCOVER over the landing folder
--   2. cell_pressure  DELTA, pressure and saturation per cell per step
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.simulation
    COMMENT 'ECLIPSE simulator output read in place, pivoted into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- One reader serves the whole family. An EGRID, an INIT, a UNRST and a SMSPEC
-- hold completely different things, so the rows are LONG FORM: one row per
-- array element, carrying the keyword it came from, which occurrence of that
-- keyword, its index within the array, and its value or its text.
--
-- That is what lets a single external table span all eight files. A wide
-- table would need a column per keyword and there is no fixed set of them:
-- a restart file's keywords depend on what the engineer asked the simulator
-- to output.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.simulation.sim_arrays;

DISCOVER {{zone_name}}.simulation.sim_arrays
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta table
-- ----------------------------------------------------------------------------
-- Long form is right for reading the files and wrong for asking questions of
-- them, so the load pivots. One row per cell per report step, with the
-- dynamic properties from the restart file and the static porosity from the
-- initialisation file beside them.
--
-- Those come from two different files, and they line up because the element
-- index IS the cell index: array element 500 of PRESSURE and array element
-- 500 of PORO are the same cell. That is the contract the format guarantees
-- and the reason the join needs no geometry.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.simulation.cell_pressure (
    run           VARCHAR,
    delivered_on  VARCHAR,
    source_file   VARCHAR,
    report_step   INTEGER,
    cell_index    INTEGER,
    pressure      DOUBLE,
    swat          DOUBLE,
    poro          DOUBLE
) LOCATION '{{data_subdir}}/curated/cell_pressure';
