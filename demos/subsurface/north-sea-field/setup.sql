-- ============================================================================
-- Subsurface: North Sea Demo Field - Setup Script
-- ============================================================================
-- Six external tables over five upstream energy formats, all read in place:
--
--   1. seismic_traces    SEG-Y, one row per trace, samples kept as an array
--   2. seismic_headers   SEG-Y again, header columns only, no sample decoding
--   3. well_logs         LAS, one row per depth step, well header on every row
--   4. top_reservoir     ZMAP+, one row per grid node with real coordinates
--   5. reservoir_model   GRDECL, one row per cell, one column per property
--   6. survey_navigation UKOOA P1/90, one row per shot point
--
-- Every format keeps its own natural row shape. That is deliberate: a trace,
-- a depth step, a grid node and a cell are different things, and flattening
-- them into a common shape would lose what makes each queryable.
--
-- No format needs a USING clause it cannot infer, but each is written out
-- here so the script reads as documentation of which keyword goes with which
-- extension.
-- ============================================================================

-- STEP 1: Zone and schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.subsurface
    COMMENT 'Upstream energy formats read in place: seismic, well logs, grids, simulation decks and navigation';


-- ============================================================================
-- TABLE 1: seismic_traces - SEG-Y, one row per trace
-- ============================================================================
-- The trace header becomes ordinary scalar columns (inline, crossline,
-- source_x, source_y, offset, ...) and the samples become a single
-- FixedSizeList<Float32> column. Coordinates arrive already corrected: SEG-Y
-- stores them as integers with a separate scalar, and the reader applies it,
-- so source_x is a real UTM easting rather than a raw integer.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.subsurface.seismic_traces
USING SEGY
LOCATION '{{data_subdir}}/demo_survey.segy';


-- ============================================================================
-- TABLE 2: seismic_headers - the same volume, without the samples
-- ============================================================================
-- include_samples = 'false' skips sample decoding entirely. On a real survey
-- this is the difference between reading the geometry of a 40 GB volume and
-- reading the volume, so it is the table to build a map or a fold plot from.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.subsurface.seismic_headers
USING SEGY
LOCATION '{{data_subdir}}/demo_survey.segy'
OPTIONS (
    include_samples = 'false'
);


-- ============================================================================
-- TABLE 3: well_logs - LAS, both wells in one table
-- ============================================================================
-- The glob reads both files as one table. The ~W well-information section
-- becomes constant columns on every row (well_well, well_fld, well_uwi), so
-- the table is queryable by well or field with no join to a header table.
--
-- The RHOB curve has a washed-out interval written with the file's own NULL
-- sentinel (-999.25). It reads as a real SQL NULL, so AVG(rhob) is the average
-- of the readings that exist rather than being dragged to minus a thousand.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.subsurface.well_logs
USING LAS
LOCATION '{{data_subdir}}/*.las'
OPTIONS (
    file_metadata = '{"columns":["df_file_name"]}'
);


-- ============================================================================
-- TABLE 4: top_reservoir - ZMAP+ depth grid
-- ============================================================================
-- One row per grid node, with x and y already computed from the header's
-- extent, so the surface joins to well positions and seismic geometry without
-- the caller reconstructing the grid.
--
-- ZMAP+ values are laid out column by column, not row by row. The reader
-- honours that; a reader that assumed row-major would produce a transposed
-- map with no error to show for it.
--
-- Nodes outside the mapped polygon carry the file's 1e30 sentinel and are
-- dropped by default, because a blank node carries no information.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.subsurface.top_reservoir
USING ZMAP
LOCATION '{{data_subdir}}/top_reservoir.zmap';


-- ============================================================================
-- TABLE 5: reservoir_model - ECLIPSE GRDECL corner-point deck
-- ============================================================================
-- One row per cell, with i, j and k derived from SPECGRID so that several
-- properties of the same model line up on the same cell. ECLIPSE orders cells
-- with I fastest, then J, then K, and the reader follows that.
--
-- The deck uses run-length values (3*0.25 means three values of 0.25, not a
-- multiplication) and inline -- comments. Both are handled.
--
-- Cells that ACTNUM marks inactive are dropped by default: they carry no
-- reservoir property and inflate a model by a large factor.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.subsurface.reservoir_model
USING GRDECL
LOCATION '{{data_subdir}}/demo_model.grdecl';


-- ============================================================================
-- TABLE 6: survey_navigation - UKOOA P1/90
-- ============================================================================
-- Fixed-width positional text: the column a character sits in is its meaning.
-- Latitude and longitude are stored packed as DDMMSS.SS with a trailing
-- hemisphere letter, and arrive as decimal degrees, so the survey plots on a
-- map without a conversion step.
--
-- The file's H header records describe the survey and are not positions; they
-- do not become rows.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.subsurface.survey_navigation
USING UKOOA
LOCATION '{{data_subdir}}/demo_survey.p190';
