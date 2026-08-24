-- ============================================================================
-- Well Pad Lease Compliance - Incremental Load and Verification
-- ============================================================================
-- Two shapefile deliveries:
--
--   2026-03-11  well_pads      8 points
--   2026-03-12  lease_tracts   4 polygons, 6326 acres
--
-- Every value below was decoded from the .shp and .dbf files by a second,
-- independent reader before the engine saw them.
--
-- The finding the compliance register exists to produce: two of the eight
-- pads sit outside every leased tract. PAD-06 is north-east of the acreage
-- and PAD-07 is south-west of it, and neither is visible from the pad file
-- alone.
--
-- A note on the geometry column. The reader emits it as well-known binary,
-- which is the encoding that survives a round trip through Delta and Parquet
-- unchanged, and this demo keeps it that way. The containment test below is
-- done on the tract's bounding box from the .dbf rather than on the polygon,
-- because DeltaForge's built-in spatial predicates take well-known TEXT. That
-- is exact here, since every tract in this lease is a rectangle, and it is
-- stated rather than hidden.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.surface_land.lease_tracts
    PATH '{{data_subdir}}/landing/2026-03-12_lease_tracts.shp'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE PADS, WITH THEIR ATTRIBUTES
-- ============================================================================
-- Eight points, and the .dbf columns arrive alongside without being asked
-- for: the reader opens the companion file beside the geometry.

ASSERT ROW_COUNT = 8
ASSERT VALUE geometry_type = 'Point' WHERE pad_id = 'PAD-01'
ASSERT VALUE status = 'PRODUCING' WHERE pad_id = 'PAD-01'
ASSERT VALUE status = 'PLUGGED' WHERE pad_id = 'PAD-07'
ASSERT VALUE spud_year = 2022 WHERE pad_id = 'PAD-07'
SELECT record_index, geometry_type, pad_id, operator, spud_year, status,
       easting, northing
FROM {{zone_name}}.surface_land.well_pads
ORDER BY pad_id;


-- ============================================================================
-- 3. THE TRACTS, WITH THEIR ATTRIBUTES
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE geometry_type = 'Polygon' WHERE tract_id = 'TR-4401'
ASSERT VALUE lessor = 'Redbluff Ranch LLC' WHERE tract_id = 'TR-4401'
ASSERT VALUE expiry = 2029 WHERE tract_id = 'TR-4407'
ASSERT VALUE expiry = 2034 WHERE tract_id = 'TR-4412'
SELECT record_index, geometry_type, tract_id, lessor, expiry, acres
FROM {{zone_name}}.surface_land.lease_tracts
ORDER BY tract_id;


-- ============================================================================
-- 4. GEOMETRY AND ATTRIBUTES LINE UP ONE FOR ONE
-- ============================================================================
-- A shapefile has no key joining its geometry to its attributes: the nth
-- shape belongs to the nth .dbf row and that is the entire contract. If the
-- two ever went out of step the table would still read, and every pad would
-- carry somebody else's identifier.

ASSERT ROW_COUNT = 1
ASSERT VALUE pads = 8
ASSERT VALUE with_geometry = 8
ASSERT VALUE with_identifier = 8
ASSERT VALUE points = 8
SELECT COUNT(*)                                            AS pads,
       COUNT(*) FILTER (WHERE geometry IS NOT NULL)        AS with_geometry,
       COUNT(*) FILTER (WHERE pad_id IS NOT NULL)          AS with_identifier,
       COUNT(*) FILTER (WHERE geometry_type = 'Point')     AS points
FROM {{zone_name}}.surface_land.well_pads;


-- ============================================================================
-- 5. THE LEASED ACREAGE
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE tracts = 2 WHERE lessor = 'Redbluff Ranch LLC'
ASSERT VALUE acres = 3558 WHERE lessor = 'Redbluff Ranch LLC'
ASSERT VALUE tracts = 1 WHERE lessor = 'Pecos Land Trust'
ASSERT VALUE acres = 1483 WHERE lessor = 'Pecos Land Trust'
ASSERT VALUE tracts = 1 WHERE lessor = 'State of New Mexico'
ASSERT VALUE acres = 1285 WHERE lessor = 'State of New Mexico'
SELECT lessor,
       COUNT(*)                            AS tracts,
       CAST(ROUND(SUM(acres)) AS BIGINT)   AS acres,
       MIN(expiry)                         AS first_expiry
FROM {{zone_name}}.surface_land.lease_tracts
GROUP BY lessor
ORDER BY lessor;


-- ============================================================================
-- 6. LOAD THE COMPLIANCE REGISTER
-- ============================================================================
-- A left join, deliberately. An inner join would answer a different question:
-- it would list the pads that ARE compliant and say nothing about the ones
-- that are not, which is the half of the answer that matters.

INSERT INTO {{zone_name}}.surface_land.pad_compliance
SELECT p.pad_id,
       p.operator,
       CAST(p.spud_year AS INTEGER)      AS spud_year,
       p.status,
       CAST(p.easting AS DOUBLE)         AS easting,
       CAST(p.northing AS DOUBLE)        AS northing,
       t.tract_id,
       t.lessor,
       CAST(t.expiry AS INTEGER)         AS lease_expiry,
       t.tract_id IS NOT NULL            AS compliant,
       '2026-03-12'                      AS delivered_on,
       p.df_file_name                    AS source_file
FROM {{zone_name}}.surface_land.well_pads p
LEFT JOIN {{zone_name}}.surface_land.lease_tracts t
  ON CAST(p.easting AS DOUBLE)  BETWEEN CAST(t.min_east AS DOUBLE)
                                    AND CAST(t.max_east AS DOUBLE)
 AND CAST(p.northing AS DOUBLE) BETWEEN CAST(t.min_north AS DOUBLE)
                                    AND CAST(t.max_north AS DOUBLE)
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.surface_land.pad_compliance c
    WHERE c.source_file = p.df_file_name
);


-- ============================================================================
-- 7. THE REGISTER IS COMPLETE
-- ============================================================================
-- Eight pads in, eight rows out. No pad was dropped for failing to match.

ASSERT ROW_COUNT = 1
ASSERT VALUE pads = 8
ASSERT VALUE compliant = 6
ASSERT VALUE not_compliant = 2
SELECT COUNT(*)                                     AS pads,
       COUNT(*) FILTER (WHERE compliant)            AS compliant,
       COUNT(*) FILTER (WHERE NOT compliant)        AS not_compliant
FROM {{zone_name}}.surface_land.pad_compliance;


-- ============================================================================
-- 8. THE SAME LOAD AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.surface_land.pad_compliance
SELECT p.pad_id,
       p.operator,
       CAST(p.spud_year AS INTEGER)      AS spud_year,
       p.status,
       CAST(p.easting AS DOUBLE)         AS easting,
       CAST(p.northing AS DOUBLE)        AS northing,
       t.tract_id,
       t.lessor,
       CAST(t.expiry AS INTEGER)         AS lease_expiry,
       t.tract_id IS NOT NULL            AS compliant,
       '2026-03-12'                      AS delivered_on,
       p.df_file_name                    AS source_file
FROM {{zone_name}}.surface_land.well_pads p
LEFT JOIN {{zone_name}}.surface_land.lease_tracts t
  ON CAST(p.easting AS DOUBLE)  BETWEEN CAST(t.min_east AS DOUBLE)
                                    AND CAST(t.max_east AS DOUBLE)
 AND CAST(p.northing AS DOUBLE) BETWEEN CAST(t.min_north AS DOUBLE)
                                    AND CAST(t.max_north AS DOUBLE)
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.surface_land.pad_compliance c
    WHERE c.source_file = p.df_file_name
);


-- ============================================================================
-- 9. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE pads = 8
ASSERT VALUE compliant = 6
SELECT COUNT(*)                          AS pads,
       COUNT(*) FILTER (WHERE compliant) AS compliant
FROM {{zone_name}}.surface_land.pad_compliance;


-- ============================================================================
-- 10. THE PADS THAT ARE NOT ON LEASED GROUND
-- ============================================================================
-- The finding. PAD-06 was permitted north-east of the acreage and PAD-07 was
-- drilled south-west of it in 2022 and has since been plugged. Neither is
-- visible from the pad file alone, and neither would appear at all if the
-- register had been built with an inner join.

ASSERT ROW_COUNT = 2
ASSERT VALUE status = 'PERMITTED' WHERE pad_id = 'PAD-06'
ASSERT VALUE spud_year = 2025 WHERE pad_id = 'PAD-06'
ASSERT VALUE status = 'PLUGGED' WHERE pad_id = 'PAD-07'
ASSERT VALUE spud_year = 2022 WHERE pad_id = 'PAD-07'
ASSERT RESULT SET ORDERED ('PAD-06'), ('PAD-07')
SELECT pad_id, status, spud_year
FROM {{zone_name}}.surface_land.pad_compliance
WHERE NOT compliant
ORDER BY pad_id;


-- ============================================================================
-- 11. PADS PER TRACT
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE pads = 2 WHERE tract_id = 'TR-4401'
ASSERT VALUE pads = 1 WHERE tract_id = 'TR-4402'
ASSERT VALUE pads = 2 WHERE tract_id = 'TR-4407'
ASSERT VALUE pads = 1 WHERE tract_id = 'TR-4412'
SELECT tract_id,
       MIN(lessor)        AS lessor,
       MIN(lease_expiry)  AS lease_expiry,
       COUNT(*)           AS pads
FROM {{zone_name}}.surface_land.pad_compliance
WHERE compliant
GROUP BY tract_id
ORDER BY tract_id;


-- ============================================================================
-- 12. WHICH PRODUCING PADS SIT ON A LEASE THAT EXPIRES SOONEST
-- ============================================================================
-- The question a land manager asks before a renewal negotiation: two
-- producing pads sit on TR-4407, which expires in 2029, and that is the
-- lease to renew first.

ASSERT ROW_COUNT = 1
ASSERT VALUE tract_id = 'TR-4407'
ASSERT VALUE lease_expiry = 2029
ASSERT VALUE producing_pads = 2
SELECT tract_id,
       MIN(lessor)       AS lessor,
       MIN(lease_expiry) AS lease_expiry,
       COUNT(*)          AS producing_pads
FROM {{zone_name}}.surface_land.pad_compliance
WHERE compliant
  AND status = 'PRODUCING'
GROUP BY tract_id
ORDER BY MIN(lease_expiry)
LIMIT 1;


-- ============================================================================
-- 13. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT p.pad_id
FROM {{zone_name}}.surface_land.well_pads p
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.surface_land.pad_compliance c
    WHERE c.pad_id = p.pad_id
);


-- ============================================================================
-- 14. THE STATE AFTER THE LOAD, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 8
SELECT *
FROM {{zone_name}}.surface_land.pad_compliance VERSION AS OF 1;


-- ============================================================================
-- 15. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.surface_land.pad_compliance;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The register as a land manager would sign it off: every pad, whether it is
-- on leased ground, and which lease.

ASSERT ROW_COUNT = 8
ASSERT VALUE compliant = true WHERE pad_id = 'PAD-01'
ASSERT VALUE tract_id = 'TR-4401' WHERE pad_id = 'PAD-01'
ASSERT VALUE tract_id = 'TR-4402' WHERE pad_id = 'PAD-03'
ASSERT VALUE tract_id = 'TR-4407' WHERE pad_id = 'PAD-04'
ASSERT VALUE tract_id = 'TR-4412' WHERE pad_id = 'PAD-05'
ASSERT VALUE compliant = false WHERE pad_id = 'PAD-06'
ASSERT VALUE compliant = false WHERE pad_id = 'PAD-07'
ASSERT VALUE tract_id = 'TR-4407' WHERE pad_id = 'PAD-08'
SELECT pad_id,
       status,
       spud_year,
       tract_id,
       lessor,
       lease_expiry,
       compliant
FROM {{zone_name}}.surface_land.pad_compliance
ORDER BY pad_id;
