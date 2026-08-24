-- ============================================================================
-- Reservoir Model Handover Audit - Incremental Load and Verification
-- ============================================================================
-- Two versions of a static model, delivered a day apart, plus one loose part:
--
--   2026-03-11  static_model_v1   14 objects,  7 naming external arrays
--   2026-03-12  static_model_v2   18 objects, 10 naming external arrays
--   2026-03-12  one unpacked .xml  1 object,   1 naming an external array
--
-- 33 rows in total. Every count below was audited from the packages by a
-- second, independent reader before the engine saw them.
--
-- The audit question is not how many objects arrived. It is which of them
-- point at bulk arrays living in a companion HDF5 file, because that is the
-- handover that fails quietly: the .epc opens, every object is present, every
-- citation is filled in, and the grid has no geometry because the .h5 it
-- names never arrived. RESQML records the reference rather than following it,
-- so the dependency is a row here instead of an absence there.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- USING RESQML, not USING EXCEL. An .epc is a ZIP holding XML parts, which is
-- also what an .xlsx is, so a detector that stops at the ZIP header calls a
-- reservoir model a spreadsheet. RESQML is claimed by its own signature
-- before the Office Open XML check ever runs.

DISCOVER {{zone_name}}.model_handover.model_packages
    PATH '{{data_subdir}}/landing/packages'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. ONE ROW PER OBJECT PART
-- ============================================================================
-- 14 + 18 = 32 objects across the two packages. Each package also carries a
-- content-type manifest and a _rels tree, which are packaging rather than
-- data and are correctly not rows: v1 holds 16 ZIP entries and yields 14.

ASSERT ROW_COUNT = 32
SELECT *
FROM {{zone_name}}.model_handover.model_packages;


-- ============================================================================
-- 3. THE LOOSE PART READS AS A SINGLE OBJECT
-- ============================================================================
-- RESQML is exchanged unpacked as well as packaged, one object per file. The
-- same reader handles both without being told which it is looking at.

ASSERT ROW_COUNT = 1
ASSERT VALUE object_type = 'obj_Grid2dRepresentation'
ASSERT VALUE external_array_count = 1
SELECT object_type, title, uuid, reference_count, external_array_count
FROM {{zone_name}}.model_handover.unpacked_parts;


-- ============================================================================
-- 4. WHAT EACH PACKAGE HOLDS
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE objects = 14 WHERE df_file_name = '2026-03-11_static_model_v1.epc'
ASSERT VALUE object_types = 12 WHERE df_file_name = '2026-03-11_static_model_v1.epc'
ASSERT VALUE with_arrays = 7 WHERE df_file_name = '2026-03-11_static_model_v1.epc'
ASSERT VALUE objects = 18 WHERE df_file_name = '2026-03-12_static_model_v2.epc'
ASSERT VALUE object_types = 12 WHERE df_file_name = '2026-03-12_static_model_v2.epc'
ASSERT VALUE with_arrays = 10 WHERE df_file_name = '2026-03-12_static_model_v2.epc'
SELECT df_file_name,
       COUNT(*)                                              AS objects,
       COUNT(DISTINCT object_type)                           AS object_types,
       COUNT(*) FILTER (WHERE external_array_count > 0)      AS with_arrays
FROM {{zone_name}}.model_handover.model_packages
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 5. LOAD THE FIRST HANDOVER
-- ============================================================================

INSERT INTO {{zone_name}}.model_handover.model_inventory
SELECT 'v1'                    AS model_version,
       '2026-03-11'            AS delivered_on,
       p.df_file_name          AS source_file,
       p.part_name,
       p.object_type,
       p.uuid,
       p.title,
       p.originator,
       p.reference_count,
       p.external_array_count,
       p.external_arrays
FROM {{zone_name}}.model_handover.model_packages p
WHERE p.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.model_handover.model_inventory i
      WHERE i.source_file = p.df_file_name
  );


-- ============================================================================
-- 6. THE FIRST HANDOVER IS AUDITED
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE objects = 14
ASSERT VALUE with_arrays = 7
ASSERT VALUE unique_uuids = 14
SELECT COUNT(*)                                          AS objects,
       COUNT(DISTINCT uuid)                              AS unique_uuids,
       COUNT(*) FILTER (WHERE external_array_count > 0)  AS with_arrays
FROM {{zone_name}}.model_handover.model_inventory
WHERE model_version = 'v1';


-- ============================================================================
-- 7. EVERY UUID IS UNIQUE
-- ============================================================================
-- A RESQML object is identified by its UUID and nothing else, so two objects
-- sharing one is a package that cannot be resolved. The count that matters is
-- zero.

ASSERT ROW_COUNT = 0
SELECT uuid, COUNT(*) AS occurrences
FROM {{zone_name}}.model_handover.model_inventory
GROUP BY uuid
HAVING COUNT(*) > 1;


-- ============================================================================
-- 8. THE SAME HANDOVER AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.model_handover.model_inventory
SELECT 'v1'                    AS model_version,
       '2026-03-11'            AS delivered_on,
       p.df_file_name          AS source_file,
       p.part_name,
       p.object_type,
       p.uuid,
       p.title,
       p.originator,
       p.reference_count,
       p.external_array_count,
       p.external_arrays
FROM {{zone_name}}.model_handover.model_packages p
WHERE p.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.model_handover.model_inventory i
      WHERE i.source_file = p.df_file_name
  );


-- ============================================================================
-- 9. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE objects = 14
SELECT COUNT(*) AS objects
FROM {{zone_name}}.model_handover.model_inventory
WHERE model_version = 'v1';


-- ============================================================================
-- 10. LOAD THE REVISED HANDOVER
-- ============================================================================

INSERT INTO {{zone_name}}.model_handover.model_inventory
SELECT 'v2'                    AS model_version,
       '2026-03-12'            AS delivered_on,
       p.df_file_name          AS source_file,
       p.part_name,
       p.object_type,
       p.uuid,
       p.title,
       p.originator,
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
-- 11. WHAT THE REVISION ADDED
-- ============================================================================
-- Four objects: a horizon, the surface that represents it, a saturation
-- property and a second well trajectory. Comparing versions by title is what
-- an asset team does before accepting a revision.

ASSERT ROW_COUNT = 4
ASSERT RESULT SET ORDERED ('Top Sleipner'), ('Top Sleipner depth surface'), ('Trajectory 15/9-F-12'), ('Water saturation')
SELECT title
FROM {{zone_name}}.model_handover.model_inventory
WHERE model_version = 'v2'
  AND title NOT IN (
      SELECT title
      FROM {{zone_name}}.model_handover.model_inventory
      WHERE model_version = 'v1'
  )
ORDER BY title;


-- ============================================================================
-- 12. THE OBJECT INVENTORY
-- ============================================================================
-- What kind of model this is, by count. Three continuous properties and one
-- discrete, two surfaces, two wells, one grid.

ASSERT ROW_COUNT = 12
ASSERT VALUE objects = 3 WHERE object_type = 'obj_ContinuousProperty'
ASSERT VALUE objects = 1 WHERE object_type = 'obj_DiscreteProperty'
ASSERT VALUE objects = 3 WHERE object_type = 'obj_HorizonInterpretation'
ASSERT VALUE objects = 2 WHERE object_type = 'obj_Grid2dRepresentation'
ASSERT VALUE objects = 1 WHERE object_type = 'obj_IjkGridRepresentation'
ASSERT VALUE objects = 2 WHERE object_type = 'obj_WellboreTrajectoryRepresentation'
SELECT object_type,
       COUNT(*) AS objects
FROM {{zone_name}}.model_handover.model_inventory
WHERE model_version = 'v2'
GROUP BY object_type
ORDER BY object_type;


-- ============================================================================
-- 13. THE HDF5 DEPENDENCY LIST
-- ============================================================================
-- The query the audit exists for. Ten of the revision's eighteen objects name
-- a dataset in a companion file, and every one of those datasets has to be
-- delivered for the model to be usable. This list is what goes back to the
-- partner as the "what else do we need" question.

ASSERT ROW_COUNT = 10
ASSERT VALUE external_array_count = 1 WHERE title = 'Porosity'
ASSERT VALUE external_arrays = '/RESQML/grid/poro' WHERE title = 'Porosity'
ASSERT VALUE external_arrays = '/RESQML/grid/points' WHERE title = 'Static model grid 60x48x22'
ASSERT VALUE external_arrays = '/RESQML/wells/f12/control_points' WHERE title = 'Trajectory 15/9-F-12'
SELECT title,
       object_type,
       external_array_count,
       external_arrays
FROM {{zone_name}}.model_handover.model_inventory
WHERE model_version = 'v2'
  AND external_array_count > 0
ORDER BY title;


-- ============================================================================
-- 14. THE OBJECTS THAT NEED NOTHING ELSE
-- ============================================================================
-- Interpretations and the coordinate reference system are self contained: no
-- bulk arrays, so they are usable from the .epc alone. Eight of eighteen.

ASSERT ROW_COUNT = 1
ASSERT VALUE self_contained = 8
ASSERT VALUE needs_hdf5 = 10
SELECT COUNT(*) FILTER (WHERE external_array_count = 0) AS self_contained,
       COUNT(*) FILTER (WHERE external_array_count > 0) AS needs_hdf5
FROM {{zone_name}}.model_handover.model_inventory
WHERE model_version = 'v2';


-- ============================================================================
-- 15. HOW CONNECTED THE MODEL IS
-- ============================================================================
-- A reference count of zero is a root object: the coordinate system, the HDF
-- proxy and the structural feature everything else hangs off. Three of them,
-- which is what a well formed model looks like.

ASSERT ROW_COUNT = 4
ASSERT VALUE objects = 3 WHERE reference_count = 0
ASSERT VALUE objects = 5 WHERE reference_count = 1
ASSERT VALUE objects = 6 WHERE reference_count = 2
ASSERT VALUE objects = 4 WHERE reference_count = 3
SELECT reference_count,
       COUNT(*) AS objects
FROM {{zone_name}}.model_handover.model_inventory
WHERE model_version = 'v2'
GROUP BY reference_count
ORDER BY reference_count;


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

ASSERT ROW_COUNT = 14
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
-- The audit as an asset team would sign it off: both versions, what grew, and
-- how much of each still depends on a file that is not in the package.

ASSERT ROW_COUNT = 2
ASSERT VALUE objects = 14 WHERE model_version = 'v1'
ASSERT VALUE delivered_on = '2026-03-11' WHERE model_version = 'v1'
ASSERT VALUE needs_hdf5 = 7 WHERE model_version = 'v1'
ASSERT VALUE unique_uuids = 14 WHERE model_version = 'v1'
ASSERT VALUE objects = 18 WHERE model_version = 'v2'
ASSERT VALUE delivered_on = '2026-03-12' WHERE model_version = 'v2'
ASSERT VALUE needs_hdf5 = 10 WHERE model_version = 'v2'
ASSERT VALUE unique_uuids = 18 WHERE model_version = 'v2'
ASSERT VALUE originator = 'Nordfjell Petroleum' WHERE model_version = 'v2'
SELECT model_version,
       MIN(delivered_on)                                 AS delivered_on,
       MIN(originator)                                   AS originator,
       COUNT(*)                                          AS objects,
       COUNT(DISTINCT uuid)                              AS unique_uuids,
       COUNT(DISTINCT object_type)                       AS object_types,
       COUNT(*) FILTER (WHERE external_array_count > 0)  AS needs_hdf5
FROM {{zone_name}}.model_handover.model_inventory
GROUP BY model_version
ORDER BY model_version;
