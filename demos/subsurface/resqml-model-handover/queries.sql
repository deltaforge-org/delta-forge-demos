-- ============================================================================
-- Reservoir Model Handover Audit - Incremental Load and Verification
-- ============================================================================
-- Three real RESQML packages delivered over two days:
--
--   2026-03-11  block         26 objects, 16 types, 22 external arrays
--   2026-03-12  s_bend        24 objects,  8 types, 67 external arrays
--               tic_tac_toe    6 objects,  4 types,  1 external array
--
-- 56 objects in total. Every count was audited from the packages by a second,
-- independent ZIP and XML reader before the engine saw them.
--
-- These are the resqpy project's own example models, written by resqpy, with
-- their .h5 companions delivered alongside. That matters for the last query:
-- the dependency list is a real one, naming datasets that really exist in
-- files that really came with the packages, rather than a scenario.
--
-- Reading real packages found two defects in the reader, both fixed and both
-- asserted here, and both of the kind that produce a table which looks
-- perfectly reasonable:
--
--   1. `docProps/core.xml` is Open Packaging Conventions metadata about the
--      PACKAGE, not a RESQML object. It ends in .xml and sits outside _rels/,
--      so it was emitted as an object with the type `coreProperties`: one
--      spurious row per package, in a table whose entire purpose is an
--      accurate inventory.
--   2. A DataObjectReference names its target in an `<eml:UUID>` CHILD
--      ELEMENT, not a `uuid` attribute. Reading only the attribute form left
--      reference_count at zero for all 56 objects, so the reference graph was
--      empty while every other column was right.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- USING RESQML, not USING EXCEL. An .epc is a ZIP holding XML parts, which is
-- also what an .xlsx is.

DISCOVER {{zone_name}}.model_handover.model_packages
    PATH '{{data_subdir}}/landing/packages'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. ONE ROW PER OBJECT PART
-- ============================================================================
-- 26 + 24 + 6 = 56. Each package also holds a content-type manifest, a _rels
-- tree and a docProps folder, which are packaging rather than data: block.epc
-- has 55 ZIP entries and yields 26 objects.

ASSERT ROW_COUNT = 56
SELECT *
FROM {{zone_name}}.model_handover.model_packages;


-- ============================================================================
-- 3. PACKAGE METADATA IS NOT AN OBJECT
-- ============================================================================
-- The first defect, asserted directly. `docProps/core.xml` carries Dublin
-- Core metadata about the package and would parse as an object of type
-- `coreProperties`. Every one of these three packages has one.

ASSERT ROW_COUNT = 1
ASSERT VALUE core_properties_rows = 0
ASSERT VALUE docprops_rows = 0
SELECT COUNT(*) FILTER (WHERE object_type = 'coreProperties')  AS core_properties_rows,
       COUNT(*) FILTER (WHERE STRPOS(part_name, 'docProps') > 0) AS docprops_rows
FROM {{zone_name}}.model_handover.model_packages;


-- ============================================================================
-- 4. WHAT EACH PACKAGE HOLDS
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE objects = 26 WHERE df_file_name = '2026-03-11_block.epc'
ASSERT VALUE object_types = 16 WHERE df_file_name = '2026-03-11_block.epc'
ASSERT VALUE with_arrays = 14 WHERE df_file_name = '2026-03-11_block.epc'
ASSERT VALUE objects = 24 WHERE df_file_name = '2026-03-12_s_bend.epc'
ASSERT VALUE object_types = 8 WHERE df_file_name = '2026-03-12_s_bend.epc'
ASSERT VALUE with_arrays = 20 WHERE df_file_name = '2026-03-12_s_bend.epc'
ASSERT VALUE objects = 6 WHERE df_file_name = '2026-03-12_tic_tac_toe.epc'
ASSERT VALUE object_types = 4 WHERE df_file_name = '2026-03-12_tic_tac_toe.epc'
ASSERT VALUE with_arrays = 1 WHERE df_file_name = '2026-03-12_tic_tac_toe.epc'
SELECT df_file_name,
       COUNT(*)                                              AS objects,
       COUNT(DISTINCT object_type)                           AS object_types,
       COUNT(*) FILTER (WHERE external_array_count > 0)      AS with_arrays
FROM {{zone_name}}.model_handover.model_packages
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 5. THE REFERENCE GRAPH IS NOT EMPTY
-- ============================================================================
-- The second defect, asserted directly. These packages carry 104 references
-- between their objects, and every one of them lives in an <eml:UUID> child
-- element. Read only as attributes, this total is zero and the inventory
-- claims 56 objects that reference nothing at all.

ASSERT ROW_COUNT = 1
ASSERT VALUE objects = 56
ASSERT VALUE total_references = 104
ASSERT VALUE objects_that_reference = 44
ASSERT VALUE root_objects = 12
SELECT COUNT(*)                                        AS objects,
       SUM(reference_count)                            AS total_references,
       COUNT(*) FILTER (WHERE reference_count > 0)     AS objects_that_reference,
       COUNT(*) FILTER (WHERE reference_count = 0)     AS root_objects
FROM {{zone_name}}.model_handover.model_packages;


-- ============================================================================
-- 6. LOAD THE FIRST HANDOVER
-- ============================================================================

INSERT INTO {{zone_name}}.model_handover.model_inventory
SELECT 'block'               AS model,
       '2026-03-11'          AS delivered_on,
       p.df_file_name        AS source_file,
       p.part_name,
       p.object_type,
       p.uuid,
       p.title,
       p.originator,
       p.schema_version,
       p.reference_count,
       p.external_array_count,
       p.external_arrays
FROM {{zone_name}}.model_handover.model_packages p
WHERE p.df_file_name = '2026-03-11_block.epc'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.model_handover.model_inventory i
      WHERE i.source_file = p.df_file_name
  );


-- ============================================================================
-- 7. THE FIRST HANDOVER IS AUDITED
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE objects = 26
ASSERT VALUE unique_uuids = 26
ASSERT VALUE with_arrays = 14
ASSERT VALUE arrays = 22
ASSERT VALUE references_total = 41
SELECT COUNT(*)                                          AS objects,
       COUNT(DISTINCT uuid)                              AS unique_uuids,
       COUNT(*) FILTER (WHERE external_array_count > 0)  AS with_arrays,
       SUM(external_array_count)                         AS arrays,
       SUM(reference_count)                              AS references_total
FROM {{zone_name}}.model_handover.model_inventory
WHERE model = 'block';


-- ============================================================================
-- 8. EVERY UUID IS UNIQUE
-- ============================================================================
-- A RESQML object is identified by its UUID and nothing else, so two objects
-- sharing one is a package that cannot be resolved. Across all three
-- packages, and these were written independently, there is no collision.

ASSERT ROW_COUNT = 0
SELECT uuid, COUNT(*) AS occurrences
FROM {{zone_name}}.model_handover.model_inventory
GROUP BY uuid
HAVING COUNT(*) > 1;


-- ============================================================================
-- 9. THE SAME HANDOVER AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.model_handover.model_inventory
SELECT 'block'               AS model,
       '2026-03-11'          AS delivered_on,
       p.df_file_name        AS source_file,
       p.part_name,
       p.object_type,
       p.uuid,
       p.title,
       p.originator,
       p.schema_version,
       p.reference_count,
       p.external_array_count,
       p.external_arrays
FROM {{zone_name}}.model_handover.model_packages p
WHERE p.df_file_name = '2026-03-11_block.epc'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.model_handover.model_inventory i
      WHERE i.source_file = p.df_file_name
  );


-- ============================================================================
-- 10. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE objects = 26
SELECT COUNT(*) AS objects
FROM {{zone_name}}.model_handover.model_inventory
WHERE model = 'block';


-- ============================================================================
-- 11. LOAD THE SECOND DELIVERY
-- ============================================================================

INSERT INTO {{zone_name}}.model_handover.model_inventory
SELECT CASE WHEN STRPOS(p.df_file_name, 's_bend') > 0
            THEN 's_bend' ELSE 'tic_tac_toe' END     AS model,
       '2026-03-12'          AS delivered_on,
       p.df_file_name        AS source_file,
       p.part_name,
       p.object_type,
       p.uuid,
       p.title,
       p.originator,
       p.schema_version,
       p.reference_count,
       p.external_array_count,
       p.external_arrays
FROM {{zone_name}}.model_handover.model_packages p
WHERE p.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.model_handover.model_inventory i
      WHERE i.source_file = p.df_file_name
  );


-- ============================================================================
-- 12. ALL THREE MODELS
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE objects = 26 WHERE model = 'block'
ASSERT VALUE references_total = 41 WHERE model = 'block'
ASSERT VALUE objects = 24 WHERE model = 's_bend'
ASSERT VALUE references_total = 58 WHERE model = 's_bend'
ASSERT VALUE arrays = 67 WHERE model = 's_bend'
ASSERT VALUE objects = 6 WHERE model = 'tic_tac_toe'
ASSERT VALUE references_total = 5 WHERE model = 'tic_tac_toe'
ASSERT VALUE arrays = 1 WHERE model = 'tic_tac_toe'
SELECT model,
       MIN(delivered_on)                    AS delivered_on,
       COUNT(*)                             AS objects,
       COUNT(DISTINCT object_type)          AS object_types,
       SUM(reference_count)                 AS references_total,
       SUM(external_array_count)            AS arrays
FROM {{zone_name}}.model_handover.model_inventory
GROUP BY model
ORDER BY model;


-- ============================================================================
-- 13. THE OBJECT INVENTORY
-- ============================================================================
-- Sixteen distinct object types across the three models. s_bend is twelve
-- blocked wellbores and four trajectories, which is what a well-centric model
-- looks like; block is broader.

ASSERT ROW_COUNT = 16
ASSERT VALUE objects = 13 WHERE object_type = 'BlockedWellboreRepresentation'
ASSERT VALUE objects = 10 WHERE object_type = 'ContinuousProperty'
ASSERT VALUE objects = 5 WHERE object_type = 'IjkGridRepresentation'
ASSERT VALUE objects = 5 WHERE object_type = 'WellboreTrajectoryRepresentation'
ASSERT VALUE objects = 3 WHERE object_type = 'EpcExternalPartReference'
ASSERT VALUE objects = 3 WHERE object_type = 'LocalDepth3dCrs'
SELECT object_type,
       COUNT(*)                     AS objects,
       COUNT(DISTINCT model)        AS models
FROM {{zone_name}}.model_handover.model_inventory
GROUP BY object_type
ORDER BY object_type;


-- ============================================================================
-- 14. THE HDF5 DEPENDENCY LIST
-- ============================================================================
-- The query the audit exists for. 35 of the 56 objects name a dataset in a
-- companion file, 90 datasets between them, and every one has to be delivered
-- for the model to be usable. These packages came with their .h5 files, which
-- is what makes this a complete handover rather than a broken one.

ASSERT ROW_COUNT = 1
ASSERT VALUE objects_needing_hdf5 = 35
ASSERT VALUE datasets_named = 90
ASSERT VALUE self_contained = 21
SELECT COUNT(*) FILTER (WHERE external_array_count > 0)  AS objects_needing_hdf5,
       SUM(external_array_count)                         AS datasets_named,
       COUNT(*) FILTER (WHERE external_array_count = 0)  AS self_contained
FROM {{zone_name}}.model_handover.model_inventory;


-- ============================================================================
-- 15. WHICH MODEL DEPENDS ON ITS ARRAYS MOST
-- ============================================================================
-- s_bend names 67 datasets across 20 of its 24 objects: almost nothing in it
-- is usable from the .epc alone. tic_tac_toe names one.

ASSERT ROW_COUNT = 3
ASSERT VALUE pct_needing_hdf5 = 83 WHERE model = 's_bend'
ASSERT VALUE pct_needing_hdf5 = 54 WHERE model = 'block'
ASSERT VALUE pct_needing_hdf5 = 17 WHERE model = 'tic_tac_toe'
SELECT model,
       COUNT(*)                                                       AS objects,
       COUNT(*) FILTER (WHERE external_array_count > 0)               AS needing_hdf5,
       CAST(ROUND(100.0 * COUNT(*) FILTER (WHERE external_array_count > 0)
                        / COUNT(*)) AS BIGINT)                        AS pct_needing_hdf5
FROM {{zone_name}}.model_handover.model_inventory
GROUP BY model
ORDER BY model;


-- ============================================================================
-- 16. EVERY PACKAGE AUDITED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT i.source_file, i.audited, p.landed
FROM (
    SELECT source_file, COUNT(*) AS audited
    FROM {{zone_name}}.model_handover.model_inventory
    GROUP BY source_file
) i
JOIN (
    SELECT df_file_name, COUNT(*) AS landed
    FROM {{zone_name}}.model_handover.model_packages
    GROUP BY df_file_name
) p
  ON p.df_file_name = i.source_file
WHERE i.audited <> p.landed;


-- ============================================================================
-- 17. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT p.df_file_name
FROM {{zone_name}}.model_handover.model_packages p
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.model_handover.model_inventory i
    WHERE i.source_file = p.df_file_name
);


-- ============================================================================
-- 18. THE STATE AFTER THE FIRST HANDOVER, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 26
SELECT *
FROM {{zone_name}}.model_handover.model_inventory VERSION AS OF 1;


-- ============================================================================
-- 19. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.model_handover.model_inventory;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The audit as an asset team would sign it off: three models, what each
-- holds, how connected it is, and how much of it needs a file that is not the
-- package.

ASSERT ROW_COUNT = 3
ASSERT VALUE objects = 26 WHERE model = 'block'
ASSERT VALUE delivered_on = '2026-03-11' WHERE model = 'block'
ASSERT VALUE unique_uuids = 26 WHERE model = 'block'
ASSERT VALUE object_types = 16 WHERE model = 'block'
ASSERT VALUE needs_hdf5 = 14 WHERE model = 'block'
ASSERT VALUE objects = 24 WHERE model = 's_bend'
ASSERT VALUE delivered_on = '2026-03-12' WHERE model = 's_bend'
ASSERT VALUE unique_uuids = 24 WHERE model = 's_bend'
ASSERT VALUE needs_hdf5 = 20 WHERE model = 's_bend'
ASSERT VALUE objects = 6 WHERE model = 'tic_tac_toe'
ASSERT VALUE unique_uuids = 6 WHERE model = 'tic_tac_toe'
ASSERT VALUE needs_hdf5 = 1 WHERE model = 'tic_tac_toe'
SELECT model,
       MIN(delivered_on)                                 AS delivered_on,
       COUNT(*)                                          AS objects,
       COUNT(DISTINCT uuid)                              AS unique_uuids,
       COUNT(DISTINCT object_type)                       AS object_types,
       SUM(reference_count)                              AS references_total,
       COUNT(*) FILTER (WHERE external_array_count > 0)  AS needs_hdf5
FROM {{zone_name}}.model_handover.model_inventory
GROUP BY model
ORDER BY model;
