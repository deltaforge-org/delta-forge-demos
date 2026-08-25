-- ============================================================================
-- Offshore Lease Footprint - Incremental Load and Verification
-- ============================================================================
-- Two real BOEM shapefiles:
--
--   leases   1870 active oil and gas leases on the Outer Continental Shelf
--   blocks   29,186 official lease blocks, the grid the leases are measured on
--
-- Both are in the public domain, being works of the United States government.
-- Every value asserted below was decoded from the .shp and .dbf by a second,
-- independent reader written from the specification before the engine saw the
-- bytes.
--
-- The thing real data brings here is names. DBF truncates every field name to
-- TEN characters, so the columns are lease_numb, sale_numbe, current_ar and
-- lease_eff_, the last with its underscore left dangling where the cut landed.
-- The demo asserts those names as they are. A written fixture would have had
-- tidy ones and proved nothing.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.surface_land.leases
    PATH '{{data_subdir}}/landing/leases.shp'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. ONE SHAPE, ONE ROW
-- ============================================================================
-- 1870 leases. The .shp and the .dbf are separate files with no key between
-- them: the Nth attribute row belongs to the Nth shape and nothing in either
-- file says so. A reader that lost sync would still return 1870 rows, which is
-- why the checks below are on values and not only on counts.

ASSERT ROW_COUNT = 1870
SELECT *
FROM {{zone_name}}.surface_land.leases;


-- ============================================================================
-- 3. THE GEOMETRY CAME THROUGH
-- ============================================================================
-- Shape type 5 is Polygon, and every record has one. geometry is OGC
-- well-known binary, so it is a value rather than a coordinate explosion.

ASSERT ROW_COUNT = 1
ASSERT VALUE leases = 1870
ASSERT VALUE polygons = 1870
ASSERT VALUE with_geometry = 1870
ASSERT VALUE first_record = 0
SELECT COUNT(*)                                            AS leases,
       COUNT(*) FILTER (WHERE geometry_type = 'Polygon')   AS polygons,
       COUNT(geometry)                                     AS with_geometry,
       MIN(record_index)                                   AS first_record
FROM {{zone_name}}.surface_land.leases;


-- ============================================================================
-- 4. THE BLOCK GRID
-- ============================================================================
-- The reference layer, and a much bigger one. All 29,186 blocks are in BOEM
-- region G, the Gulf of Mexico, split across three planning areas: Central
-- 12,409, Eastern 11,537 and Western 5240.

ASSERT ROW_COUNT = 3
ASSERT VALUE blocks = 12409 WHERE mms_plan_a = 'CGM'
ASSERT VALUE blocks = 11537 WHERE mms_plan_a = 'EGM'
ASSERT VALUE blocks = 5240 WHERE mms_plan_a = 'WGM'
ASSERT VALUE regions = 1 WHERE mms_plan_a = 'CGM'
SELECT mms_plan_a,
       COUNT(*)                        AS blocks,
       COUNT(DISTINCT mms_region)      AS regions
FROM {{zone_name}}.surface_land.blocks
GROUP BY mms_plan_a
ORDER BY blocks DESC;


-- ============================================================================
-- 5. LOAD THE LEASE LAYER
-- ============================================================================
-- The truncated DBF names get readable ones here. This is the right place for
-- that: the external table shows the file as it is, and the curated table is
-- where a naming decision belongs.

INSERT INTO {{zone_name}}.surface_land.lease_register
SELECT '2026-03-11'                        AS delivered_on,
       l.df_file_name                      AS source_file,
       l.lease_numb                        AS lease_number,
       l.mineral_ty                        AS mineral_type,
       l.lease_stat                        AS lease_status,
       l.lease_eff_                        AS effective_date,
       CAST(l.royalty_ra AS DOUBLE)        AS royalty_rate,
       CAST(l.current_ar AS DOUBLE)        AS current_area,
       l.geometry_type
FROM {{zone_name}}.surface_land.leases l
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.surface_land.lease_register r
    WHERE r.source_file = l.df_file_name
);


-- ============================================================================
-- 6. THE STATUS BREAKDOWN
-- ============================================================================
-- Six statuses across 1870 leases. Most are in their primary term, 258 are
-- producing, and the small ones matter: three leases in OPERNS and six in SOP.
-- A register that dropped a rare status would still look plausible.

ASSERT ROW_COUNT = 6
ASSERT VALUE leases = 1170 WHERE lease_status = 'PRIMRY'
ASSERT VALUE leases = 422 WHERE lease_status = 'UNIT'
ASSERT VALUE leases = 258 WHERE lease_status = 'PROD'
ASSERT VALUE leases = 11 WHERE lease_status = 'DSO'
ASSERT VALUE leases = 6 WHERE lease_status = 'SOP'
ASSERT VALUE leases = 3 WHERE lease_status = 'OPERNS'
SELECT lease_status,
       COUNT(*)     AS leases
FROM {{zone_name}}.surface_land.lease_register
GROUP BY lease_status
ORDER BY leases DESC;


-- ============================================================================
-- 7. NINETY YEARS OF LEASES, ALL STILL ACTIVE
-- ============================================================================
-- The oldest took effect on 7 February 1936 and the newest on 1 June 2026.
-- 138 of the leases active today predate 1970. A date range like this is what
-- breaks a parser written against a decade of invented dates, and it is not
-- something a generator would have thought to produce.

ASSERT ROW_COUNT = 1
ASSERT VALUE leases = 1870
ASSERT VALUE oldest = '19360207'
ASSERT VALUE newest = '20260601'
ASSERT VALUE before_1970 = 138
ASSERT VALUE dated = 1870
SELECT COUNT(*)                                                     AS leases,
       MIN(effective_date)                                          AS oldest,
       MAX(effective_date)                                          AS newest,
       COUNT(*) FILTER (WHERE effective_date < '19700101')          AS before_1970,
       COUNT(effective_date)                                        AS dated
FROM {{zone_name}}.surface_land.lease_register;


-- ============================================================================
-- 8. THE ROYALTY RATES ARE A SMALL FIXED SET
-- ============================================================================
-- Five of them, because a royalty rate is set by the lease sale terms rather
-- than negotiated per tract. One lease carries 33.33 percent and four carry
-- zero, and both of those are real rather than missing data.

ASSERT ROW_COUNT = 5
ASSERT VALUE leases = 970 WHERE rate_pct = 18.75
ASSERT VALUE leases = 639 WHERE rate_pct = 12.5
ASSERT VALUE leases = 256 WHERE rate_pct = 16.67
ASSERT VALUE leases = 4 WHERE rate_pct = 0
ASSERT VALUE leases = 1 WHERE rate_pct = 33.33
SELECT ROUND(royalty_rate, 2)   AS rate_pct,
       COUNT(*)                 AS leases
FROM {{zone_name}}.surface_land.lease_register
GROUP BY ROUND(royalty_rate, 2)
ORDER BY leases DESC;


-- ============================================================================
-- 9. THE ACREAGE
-- ============================================================================
-- 10.3 million acres under active lease. The largest single lease is 114,601
-- acres and the smallest is 86, a spread of more than a thousand to one that a
-- uniformly generated dataset would not have.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_acres = 10272123
ASSERT VALUE largest = 114601
ASSERT VALUE smallest = 86
ASSERT VALUE producing_acres = 1281016
SELECT CAST(ROUND(SUM(current_area)) AS BIGINT)          AS total_acres,
       CAST(ROUND(MAX(current_area)) AS BIGINT)          AS largest,
       CAST(ROUND(MIN(current_area)) AS BIGINT)          AS smallest,
       CAST(ROUND(SUM(current_area)
            FILTER (WHERE lease_status = 'PROD')) AS BIGINT) AS producing_acres
FROM {{zone_name}}.surface_land.lease_register;


-- ============================================================================
-- 10. THE SAME LAYER AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.surface_land.lease_register
SELECT '2026-03-11'                        AS delivered_on,
       l.df_file_name                      AS source_file,
       l.lease_numb                        AS lease_number,
       l.mineral_ty                        AS mineral_type,
       l.lease_stat                        AS lease_status,
       l.lease_eff_                        AS effective_date,
       CAST(l.royalty_ra AS DOUBLE)        AS royalty_rate,
       CAST(l.current_ar AS DOUBLE)        AS current_area,
       l.geometry_type
FROM {{zone_name}}.surface_land.leases l
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.surface_land.lease_register r
    WHERE r.source_file = l.df_file_name
);


-- ============================================================================
-- 11. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE leases = 1870
ASSERT VALUE files = 1
SELECT COUNT(*)                        AS leases,
       COUNT(DISTINCT source_file)     AS files
FROM {{zone_name}}.surface_land.lease_register;


-- ============================================================================
-- 12. EVERY LEASE IS OIL AND GAS
-- ============================================================================
-- BOEM also leases sulphur and salt, and this extract has none of them. That
-- is a fact about the file rather than an assumption, so it is asserted.

ASSERT ROW_COUNT = 1
ASSERT VALUE mineral_types = 1
ASSERT VALUE oil_and_gas = 1870
SELECT COUNT(DISTINCT mineral_type)                    AS mineral_types,
       COUNT(*) FILTER (WHERE mineral_type = 'O&G')    AS oil_and_gas
FROM {{zone_name}}.surface_land.lease_register;


-- ============================================================================
-- 13. PRODUCING LEASES ARE THE OLD ONES
-- ============================================================================
-- The leases that reached production go back to 1946, while every lease still
-- in its primary term took effect in 2016 or later. That is the lifecycle
-- showing up in the data, and it is also why effective_date is worth keeping:
-- the two populations do not overlap at all.
--
-- Note what is being compared. Every DBF attribute arrives as the file's own
-- TEXT, with the declared dBase type recorded in column metadata rather than
-- applied, because dBase numerics are fixed-width ASCII that routinely carry
-- blanks and overflow markers. LEASE_EFF_DATE is declared dBase type D, and it
-- still arrives as the eight characters BOEM wrote. Casting is the caller's
-- decision, which is why royalty and acreage above are cast explicitly.

ASSERT ROW_COUNT = 2
ASSERT VALUE leases = 258 WHERE lease_status = 'PROD'
ASSERT VALUE leases = 1170 WHERE lease_status = 'PRIMRY'
ASSERT VALUE oldest = '19461126' WHERE lease_status = 'PROD'
ASSERT VALUE newest = '20230501' WHERE lease_status = 'PROD'
ASSERT VALUE oldest = '20160701' WHERE lease_status = 'PRIMRY'
ASSERT VALUE newest = '20260601' WHERE lease_status = 'PRIMRY'
SELECT lease_status,
       COUNT(*)                 AS leases,
       MIN(effective_date)      AS oldest,
       MAX(effective_date)      AS newest
FROM {{zone_name}}.surface_land.lease_register
WHERE lease_status IN ('PROD', 'PRIMRY')
GROUP BY lease_status
ORDER BY lease_status;


-- ============================================================================
-- 14. THE STATE AFTER THE FIRST DELIVERY, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 1870
SELECT *
FROM {{zone_name}}.surface_land.lease_register VERSION AS OF 1;


-- ============================================================================
-- 15. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.surface_land.lease_register;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The register as a land team would sign it off: every active lease, its
-- acreage, its status spread and its ninety-year date range.

ASSERT ROW_COUNT = 1
ASSERT VALUE leases = 1870
ASSERT VALUE statuses = 6
ASSERT VALUE rates = 5
ASSERT VALUE polygons = 1870
ASSERT VALUE total_acres = 10272123
ASSERT VALUE oldest = '19360207'
ASSERT VALUE newest = '20260601'
SELECT COUNT(*)                                            AS leases,
       COUNT(DISTINCT lease_status)                        AS statuses,
       COUNT(DISTINCT ROUND(royalty_rate, 2))              AS rates,
       COUNT(*) FILTER (WHERE geometry_type = 'Polygon')   AS polygons,
       CAST(ROUND(SUM(current_area)) AS BIGINT)            AS total_acres,
       MIN(effective_date)                                 AS oldest,
       MAX(effective_date)                                 AS newest
FROM {{zone_name}}.surface_land.lease_register;
