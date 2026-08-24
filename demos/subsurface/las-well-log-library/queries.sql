-- ============================================================================
-- Well Log Library Consolidation - Incremental Load and Verification
-- ============================================================================
-- Seven real NLOG wells, logged between 1958 and 1990, in two tranches:
--
--   2026-03-11  MED-01 1958, WAS-25 1961, MED-05 1963      1585 depth steps
--   2026-03-12  D15-01 1969, K08-02 1972, GRW-01 1979,
--               L09-06 1990                                 2791 depth steps
--
-- 4376 depth steps in total. Every count below was read from the files by a
-- second, independent LAS reader before the engine saw them.
--
-- What this demo is about that the other well-log demos are not: the LAS
-- well-information section rides on every row. A table over the whole library
-- is queryable by well, field and vintage with no join to a header table
-- anywhere, and the well name comes out of the file's own `~W` block rather
-- than out of its name.
--
-- The library is also honestly ragged, because the tools changed over thirty
-- years. Four of the seven wells have no sonic and no density at all, and one
-- has nothing but a gamma ray. The union of curve sets is what the table
-- carries and NULL is what a well that was never measured gets.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.log_library.log_files
    PATH '{{data_path}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE WHOLE PACKAGE, READ IN PLACE
-- ============================================================================
-- 1585 + 2791 = 4376 depth steps across seven wells and three different tool
-- suites, in one table.

ASSERT ROW_COUNT = 4376
SELECT *
FROM {{zone_name}}.log_library.log_files;


-- ============================================================================
-- 3. THE WELL HEADER IS ON EVERY ROW
-- ============================================================================
-- This is the query that needs no join. well_well and well_fld come from the
-- `~W` section of each file and are repeated down every depth step, so
-- grouping the library by well is grouping the rows themselves.

ASSERT ROW_COUNT = 7
ASSERT VALUE steps = 620 WHERE well_well = 'MED-01'
ASSERT VALUE steps = 407 WHERE well_well = 'WAS-25'
ASSERT VALUE steps = 558 WHERE well_well = 'MED-05'
ASSERT VALUE steps = 456 WHERE well_well = 'D15-01'
ASSERT VALUE steps = 465 WHERE well_well = 'K08-02'
ASSERT VALUE steps = 842 WHERE well_well = 'GRW-01'
ASSERT VALUE steps = 1028 WHERE well_well = 'L09-06'
SELECT well_well AS well,
       well_fld  AS field,
       COUNT(*)  AS steps
FROM {{zone_name}}.log_library.log_files
GROUP BY well_well, well_fld
ORDER BY well_well;


-- ============================================================================
-- 4. THE CURVE SETS DIFFER, AND THE SCHEMA IS THEIR UNION
-- ============================================================================
-- Gamma ray on all seven wells. Sonic and density on three. Neutron on six.
-- One well with nothing but gamma. A table that demanded one common schema
-- would have to drop four curves or reject four files.

ASSERT ROW_COUNT = 1
ASSERT VALUE steps = 4376
ASSERT VALUE live_gr = 4297
ASSERT VALUE live_dt = 1763
ASSERT VALUE live_rhob = 1763
ASSERT VALUE live_nphi = 3210
SELECT COUNT(*)                                    AS steps,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)      AS live_gr,
       COUNT(*) FILTER (WHERE dt IS NOT NULL)      AS live_dt,
       COUNT(*) FILTER (WHERE rhob IS NOT NULL)    AS live_rhob,
       COUNT(*) FILTER (WHERE drho IS NOT NULL)    AS live_drho,
       COUNT(*) FILTER (WHERE nphi IS NOT NULL)    AS live_nphi
FROM {{zone_name}}.log_library.log_files;


-- ============================================================================
-- 5. LOAD THE 11 MARCH TRANCHE
-- ============================================================================
-- The well name and field come from the file's own header. The vintage comes
-- from the delivery name, which is where the data manager put it, and the
-- literal string UNKNOWN that NLOG uses for an unattributed field becomes a
-- real NULL: a field called UNKNOWN groups as though it were a field.

INSERT INTO {{zone_name}}.log_library.log_library
SELECT l.well_well                                       AS well,
       CASE WHEN l.well_fld <> 'UNKNOWN' THEN l.well_fld END AS field,
       CAST(SUBSTR(l.df_file_name, 12, 4) AS INTEGER)    AS vintage,
       '2026-03-11'                                      AS delivered_on,
       l.df_file_name                                    AS source_file,
       l.dept,
       l.gr,
       l.dt,
       l.rhob,
       l.drho,
       l.nphi
FROM {{zone_name}}.log_library.log_files l
WHERE l.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.log_library.log_library c
      WHERE c.source_file = l.df_file_name
  );


-- ============================================================================
-- 6. THE FIRST TRANCHE LANDED
-- ============================================================================
-- Three wells from one field, logged across five years, none of them carrying
-- a sonic because in 1963 the tool was not on the truck.

ASSERT ROW_COUNT = 3
ASSERT VALUE steps = 620 WHERE well = 'MED-01'
ASSERT VALUE vintage = 1958 WHERE well = 'MED-01'
ASSERT VALUE field = 'WASSENAAR' WHERE well = 'MED-01'
ASSERT VALUE live_gr = 552 WHERE well = 'MED-01'
ASSERT VALUE live_dt = 0 WHERE well = 'MED-01'
ASSERT VALUE steps = 407 WHERE well = 'WAS-25'
ASSERT VALUE vintage = 1961 WHERE well = 'WAS-25'
ASSERT VALUE live_gr = 407 WHERE well = 'WAS-25'
ASSERT VALUE steps = 558 WHERE well = 'MED-05'
ASSERT VALUE vintage = 1963 WHERE well = 'MED-05'
ASSERT VALUE live_gr = 555 WHERE well = 'MED-05'
ASSERT VALUE live_nphi = 486 WHERE well = 'MED-05'
SELECT well,
       MIN(field)                                  AS field,
       MIN(vintage)                                AS vintage,
       COUNT(*)                                    AS steps,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)      AS live_gr,
       COUNT(*) FILTER (WHERE dt IS NOT NULL)      AS live_dt,
       COUNT(*) FILTER (WHERE nphi IS NOT NULL)    AS live_nphi
FROM {{zone_name}}.log_library.log_library
WHERE delivered_on = '2026-03-11'
GROUP BY well
ORDER BY well;


-- ============================================================================
-- 7. THE SAME TRANCHE AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.log_library.log_library
SELECT l.well_well                                       AS well,
       CASE WHEN l.well_fld <> 'UNKNOWN' THEN l.well_fld END AS field,
       CAST(SUBSTR(l.df_file_name, 12, 4) AS INTEGER)    AS vintage,
       '2026-03-11'                                      AS delivered_on,
       l.df_file_name                                    AS source_file,
       l.dept,
       l.gr,
       l.dt,
       l.rhob,
       l.drho,
       l.nphi
FROM {{zone_name}}.log_library.log_files l
WHERE l.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.log_library.log_library c
      WHERE c.source_file = l.df_file_name
  );


-- ============================================================================
-- 8. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE steps = 1585
ASSERT VALUE wells = 3
SELECT COUNT(*)                    AS steps,
       COUNT(DISTINCT source_file) AS wells
FROM {{zone_name}}.log_library.log_library
WHERE delivered_on = '2026-03-11';


-- ============================================================================
-- 9. LOAD THE 12 MARCH TRANCHE
-- ============================================================================

INSERT INTO {{zone_name}}.log_library.log_library
SELECT l.well_well                                       AS well,
       CASE WHEN l.well_fld <> 'UNKNOWN' THEN l.well_fld END AS field,
       CAST(SUBSTR(l.df_file_name, 12, 4) AS INTEGER)    AS vintage,
       '2026-03-12'                                      AS delivered_on,
       l.df_file_name                                    AS source_file,
       l.dept,
       l.gr,
       l.dt,
       l.rhob,
       l.drho,
       l.nphi
FROM {{zone_name}}.log_library.log_files l
WHERE l.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.log_library.log_library c
      WHERE c.source_file = l.df_file_name
  );


-- ============================================================================
-- 10. THE LIBRARY, WELL BY WELL
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE steps = 456 WHERE well = 'D15-01'
ASSERT VALUE vintage = 1969 WHERE well = 'D15-01'
ASSERT VALUE live_dt = 456 WHERE well = 'D15-01'
ASSERT VALUE steps = 465 WHERE well = 'K08-02'
ASSERT VALUE live_rhob = 465 WHERE well = 'K08-02'
ASSERT VALUE steps = 842 WHERE well = 'GRW-01'
ASSERT VALUE vintage = 1979 WHERE well = 'GRW-01'
ASSERT VALUE field = '101 ROTLIEGEND' WHERE well = 'GRW-01'
ASSERT VALUE steps = 1028 WHERE well = 'L09-06'
ASSERT VALUE vintage = 1990 WHERE well = 'L09-06'
ASSERT VALUE live_gr = 1020 WHERE well = 'L09-06'
ASSERT VALUE live_dt = 0 WHERE well = 'L09-06'
ASSERT VALUE live_nphi = 0 WHERE well = 'L09-06'
SELECT well,
       MIN(vintage)                                AS vintage,
       MIN(field)                                  AS field,
       COUNT(*)                                    AS steps,
       COUNT(*) FILTER (WHERE gr IS NOT NULL)      AS live_gr,
       COUNT(*) FILTER (WHERE dt IS NOT NULL)      AS live_dt,
       COUNT(*) FILTER (WHERE rhob IS NOT NULL)    AS live_rhob,
       COUNT(*) FILTER (WHERE nphi IS NOT NULL)    AS live_nphi,
       CAST(ROUND(MIN(dept)) AS BIGINT)            AS top_m,
       CAST(ROUND(MAX(dept)) AS BIGINT)            AS base_m
FROM {{zone_name}}.log_library.log_library
GROUP BY well
ORDER BY well;


-- ============================================================================
-- 11. THE FIELD THAT IS NOT A FIELD
-- ============================================================================
-- Three of the seven files name their field UNKNOWN, which is NLOG saying it
-- does not know. Left as a string it would group as though it were a field
-- and show up on a map. The load turned it into NULL, so the three wells with
-- a real field name group cleanly and the rest are visibly unattributed.

ASSERT ROW_COUNT = 1
ASSERT VALUE named_fields = 2
ASSERT VALUE wells_with_a_field = 4
ASSERT VALUE wells_without = 3
ASSERT VALUE literal_unknown = 0
SELECT COUNT(DISTINCT field)                                       AS named_fields,
       COUNT(DISTINCT well) FILTER (WHERE field IS NOT NULL)       AS wells_with_a_field,
       COUNT(DISTINCT well) FILTER (WHERE field IS NULL)           AS wells_without,
       COUNT(*) FILTER (WHERE field = 'UNKNOWN')                   AS literal_unknown
FROM {{zone_name}}.log_library.log_library;


-- ============================================================================
-- 12. THE WASSENAAR WELLS
-- ============================================================================
-- The query the library exists for: everything logged in one field, without a
-- join, because the field name is on every row.

ASSERT ROW_COUNT = 1
ASSERT VALUE wells = 3
ASSERT VALUE steps = 1585
ASSERT VALUE oldest = 1958
ASSERT VALUE newest = 1963
SELECT COUNT(DISTINCT well)      AS wells,
       COUNT(*)                  AS steps,
       MIN(vintage)              AS oldest,
       MAX(vintage)              AS newest,
       CAST(ROUND(MIN(dept)) AS BIGINT) AS shallowest_m,
       CAST(ROUND(MAX(dept)) AS BIGINT) AS deepest_m
FROM {{zone_name}}.log_library.log_library
WHERE field = 'WASSENAAR';


-- ============================================================================
-- 13. WHAT THE LIBRARY CAN AND CANNOT ANSWER
-- ============================================================================
-- A petrophysical evaluation needs a density and a neutron. Only three of the
-- seven wells have both, and knowing which three before starting is worth
-- more than an average over the four that do not.

ASSERT ROW_COUNT = 2
ASSERT VALUE wells = 3 WHERE has_density_and_neutron = true
ASSERT VALUE steps = 1763 WHERE has_density_and_neutron = true
ASSERT VALUE wells = 4 WHERE has_density_and_neutron = false
ASSERT VALUE steps = 2613 WHERE has_density_and_neutron = false
SELECT rhob IS NOT NULL AND nphi IS NOT NULL AS has_density_and_neutron,
       COUNT(DISTINCT well)                  AS wells,
       COUNT(*)                              AS steps
FROM {{zone_name}}.log_library.log_library
GROUP BY rhob IS NOT NULL AND nphi IS NOT NULL
ORDER BY has_density_and_neutron;


-- ============================================================================
-- 14. EVERY FILE LANDED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT c.source_file, c.curated, l.landed
FROM (
    SELECT source_file, COUNT(*) AS curated
    FROM {{zone_name}}.log_library.log_library
    GROUP BY source_file
) c
JOIN (
    SELECT df_file_name, COUNT(*) AS landed
    FROM {{zone_name}}.log_library.log_files
    GROUP BY df_file_name
) l
  ON l.df_file_name = c.source_file
WHERE c.curated <> l.landed;


-- ============================================================================
-- 15. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT l.df_file_name
FROM {{zone_name}}.log_library.log_files l
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.log_library.log_library c
    WHERE c.source_file = l.df_file_name
);


-- ============================================================================
-- 16. THE STATE AFTER THE FIRST TRANCHE, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 1585
SELECT *
FROM {{zone_name}}.log_library.log_library VERSION AS OF 1;


-- ============================================================================
-- 17. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.log_library.log_library;


-- ============================================================================
-- 18. NO NULL SENTINEL SURVIVED AS A VALUE
-- ============================================================================
-- Every one of these files declares NULL as -999.25 in its `~W` section, and
-- the reader honours the declaration rather than assuming it. A sentinel read
-- as a number would put a gamma ray of minus a thousand into a library
-- average.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.log_library.log_library
WHERE gr < -900 OR dt < -900 OR rhob < -900 OR drho < -900 OR nphi < -900;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The library as a data manager would sign it off: one row per well, what it
-- holds, and over what interval.

ASSERT ROW_COUNT = 7
ASSERT VALUE steps = 620 WHERE well = 'MED-01'
ASSERT VALUE delivered_on = '2026-03-11' WHERE well = 'MED-01'
ASSERT VALUE curves_present = 2 WHERE well = 'MED-01'
ASSERT VALUE steps = 407 WHERE well = 'WAS-25'
ASSERT VALUE steps = 558 WHERE well = 'MED-05'
ASSERT VALUE steps = 456 WHERE well = 'D15-01'
ASSERT VALUE delivered_on = '2026-03-12' WHERE well = 'D15-01'
ASSERT VALUE curves_present = 5 WHERE well = 'D15-01'
ASSERT VALUE steps = 465 WHERE well = 'K08-02'
ASSERT VALUE steps = 842 WHERE well = 'GRW-01'
ASSERT VALUE steps = 1028 WHERE well = 'L09-06'
ASSERT VALUE curves_present = 1 WHERE well = 'L09-06'
SELECT well,
       MIN(vintage)                     AS vintage,
       MIN(delivered_on)                AS delivered_on,
       COUNT(*)                         AS steps,
       CAST(ROUND(MIN(dept)) AS BIGINT) AS top_m,
       CAST(ROUND(MAX(dept)) AS BIGINT) AS base_m,
       (CASE WHEN COUNT(gr)   > 0 THEN 1 ELSE 0 END
      + CASE WHEN COUNT(dt)   > 0 THEN 1 ELSE 0 END
      + CASE WHEN COUNT(rhob) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COUNT(drho) > 0 THEN 1 ELSE 0 END
      + CASE WHEN COUNT(nphi) > 0 THEN 1 ELSE 0 END) AS curves_present
FROM {{zone_name}}.log_library.log_library
GROUP BY well
ORDER BY well;
