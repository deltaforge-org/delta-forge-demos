-- ============================================================================
-- Monthly Production Allocation - Incremental Load and Verification
-- ============================================================================
-- Three facilities filing a year of monthly returns, delivered over two days:
--
--   2026-03-11  SLEIPNER-A, GUDRUN-B    24 monthly periods
--   2026-03-12  UTGARD-C                12 monthly periods
--
-- 36 periods in total. Every value below was parsed out of the documents
-- independently before the engine saw them.
--
-- The volumes are internally consistent rather than decorative: each month's
-- water volume follows from that month's oil volume and the water cut, so
-- the crossover this demo finds is a real property of the data. GUDRUN-B is
-- the mature one and passes 50 percent water in May; SLEIPNER-A only reaches
-- it in December; UTGARD-C never does.
--
-- PRODML is read through the XML engine under a curated profile, so the
-- columns are the document's own element paths. A month's oil volume is
-- `product_volume_facility_period_volume`, which is exactly why the curated
-- table renames them.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- USING PRODML, not USING XML. The document is valid XML and the extension
-- says nothing, so detection is on the Energistics namespace and the
-- productVolume element.

DISCOVER {{zone_name}}.production.production_reports
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. ONE ROW PER REPORTING MONTH
-- ============================================================================
-- Three documents, twelve periods each. Without the profile's explode on
-- //period there would be three rows and the months would be numbered
-- columns.

ASSERT ROW_COUNT = 36
SELECT *
FROM {{zone_name}}.production.production_reports;


-- ============================================================================
-- 3. THE FACILITY FIELDS REPEAT DOWN EVERY MONTH
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE periods = 12 WHERE product_volume_facility_name = 'SLEIPNER-A'
ASSERT VALUE periods = 12 WHERE product_volume_facility_name = 'GUDRUN-B'
ASSERT VALUE periods = 12 WHERE product_volume_facility_name = 'UTGARD-C'
ASSERT VALUE product_volume_facility_product = 'oil' WHERE product_volume_facility_name = 'SLEIPNER-A'
SELECT product_volume_facility_name,
       product_volume_facility_attr_uid,
       product_volume_facility_product,
       COUNT(*) AS periods
FROM {{zone_name}}.production.production_reports
GROUP BY product_volume_facility_name,
         product_volume_facility_attr_uid,
         product_volume_facility_product
ORDER BY product_volume_facility_name;


-- ============================================================================
-- 4. LOAD THE 11 MARCH RETURNS
-- ============================================================================
-- The rename, the casts, and the month pulled out of the period's start
-- timestamp so the year can be ordered.

INSERT INTO {{zone_name}}.production.production_history
SELECT r.product_volume_facility_name                                     AS facility,
       r.product_volume_facility_attr_uid                                 AS facility_uid,
       r.product_volume_facility_product                                  AS product,
       '2026-03-11'                                                       AS delivered_on,
       r.df_file_name                                                     AS source_file,
       r.product_volume_facility_period_d_tim_start                       AS period_start,
       CAST(SUBSTR(r.product_volume_facility_period_d_tim_start, 6, 2) AS INTEGER) AS month,
       CAST(r.product_volume_facility_period_volume       AS DOUBLE)      AS oil_m3,
       CAST(r.product_volume_facility_period_water_volume AS DOUBLE)      AS water_m3,
       CAST(r.product_volume_facility_period_gas_volume   AS DOUBLE)      AS gas_m3
FROM {{zone_name}}.production.production_reports r
WHERE r.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.production.production_history h
      WHERE h.source_file = r.df_file_name
  );


-- ============================================================================
-- 5. THE FIRST TWO FACILITIES LANDED
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE periods = 12 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE first_month = 1 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE last_month = 12 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE annual_oil_m3 = 570752 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE periods = 12 WHERE facility = 'GUDRUN-B'
ASSERT VALUE annual_oil_m3 = 322427 WHERE facility = 'GUDRUN-B'
SELECT facility,
       COUNT(*)                                     AS periods,
       MIN(month)                                   AS first_month,
       MAX(month)                                   AS last_month,
       CAST(ROUND(SUM(oil_m3)) AS BIGINT)           AS annual_oil_m3
FROM {{zone_name}}.production.production_history
WHERE delivered_on = '2026-03-11'
GROUP BY facility
ORDER BY facility;


-- ============================================================================
-- 6. THE SAME RETURNS AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.production.production_history
SELECT r.product_volume_facility_name                                     AS facility,
       r.product_volume_facility_attr_uid                                 AS facility_uid,
       r.product_volume_facility_product                                  AS product,
       '2026-03-11'                                                       AS delivered_on,
       r.df_file_name                                                     AS source_file,
       r.product_volume_facility_period_d_tim_start                       AS period_start,
       CAST(SUBSTR(r.product_volume_facility_period_d_tim_start, 6, 2) AS INTEGER) AS month,
       CAST(r.product_volume_facility_period_volume       AS DOUBLE)      AS oil_m3,
       CAST(r.product_volume_facility_period_water_volume AS DOUBLE)      AS water_m3,
       CAST(r.product_volume_facility_period_gas_volume   AS DOUBLE)      AS gas_m3
FROM {{zone_name}}.production.production_reports r
WHERE r.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.production.production_history h
      WHERE h.source_file = r.df_file_name
  );


-- ============================================================================
-- 7. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE periods = 24
ASSERT VALUE facilities = 2
SELECT COUNT(*)                    AS periods,
       COUNT(DISTINCT facility)    AS facilities,
       COUNT(DISTINCT source_file) AS returns
FROM {{zone_name}}.production.production_history
WHERE delivered_on = '2026-03-11';


-- ============================================================================
-- 8. LOAD THE 12 MARCH RETURN
-- ============================================================================

INSERT INTO {{zone_name}}.production.production_history
SELECT r.product_volume_facility_name                                     AS facility,
       r.product_volume_facility_attr_uid                                 AS facility_uid,
       r.product_volume_facility_product                                  AS product,
       '2026-03-12'                                                       AS delivered_on,
       r.df_file_name                                                     AS source_file,
       r.product_volume_facility_period_d_tim_start                       AS period_start,
       CAST(SUBSTR(r.product_volume_facility_period_d_tim_start, 6, 2) AS INTEGER) AS month,
       CAST(r.product_volume_facility_period_volume       AS DOUBLE)      AS oil_m3,
       CAST(r.product_volume_facility_period_water_volume AS DOUBLE)      AS water_m3,
       CAST(r.product_volume_facility_period_gas_volume   AS DOUBLE)      AS gas_m3
FROM {{zone_name}}.production.production_reports r
WHERE r.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.production.production_history h
      WHERE h.source_file = r.df_file_name
  );


-- ============================================================================
-- 9. THE ANNUAL STATEMENT
-- ============================================================================
-- What the year totalled, per facility. The water cut is the fraction of
-- total produced liquid that is water, and it is the number the field's
-- economics turn on: every barrel of water is lifted, separated and disposed
-- of at a cost, and none of it is sold.

ASSERT ROW_COUNT = 3
ASSERT VALUE annual_oil_m3 = 570752 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE annual_water_m3 = 375676 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE water_cut_pct = 40 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE annual_oil_m3 = 322427 WHERE facility = 'GUDRUN-B'
ASSERT VALUE annual_water_m3 = 374746 WHERE facility = 'GUDRUN-B'
ASSERT VALUE water_cut_pct = 54 WHERE facility = 'GUDRUN-B'
ASSERT VALUE annual_oil_m3 = 698673 WHERE facility = 'UTGARD-C'
ASSERT VALUE annual_water_m3 = 234454 WHERE facility = 'UTGARD-C'
ASSERT VALUE water_cut_pct = 25 WHERE facility = 'UTGARD-C'
SELECT facility,
       CAST(ROUND(SUM(oil_m3)) AS BIGINT)     AS annual_oil_m3,
       CAST(ROUND(SUM(water_m3)) AS BIGINT)   AS annual_water_m3,
       CAST(ROUND(SUM(gas_m3)) AS BIGINT)     AS annual_gas_m3,
       CAST(ROUND(100.0 * SUM(water_m3) / (SUM(oil_m3) + SUM(water_m3))) AS BIGINT) AS water_cut_pct
FROM {{zone_name}}.production.production_history
GROUP BY facility
ORDER BY facility;


-- ============================================================================
-- 10. WHERE THE WATER CUT CROSSES FIFTY PERCENT
-- ============================================================================
-- The question a production engineer actually asks, because it is the point
-- at which a facility is lifting more water than oil. GUDRUN-B is the mature
-- one and crosses in May, so eight of its twelve months are over. SLEIPNER-A
-- only crosses in December. UTGARD-C never does.

ASSERT ROW_COUNT = 3
ASSERT VALUE months_over_half = 8 WHERE facility = 'GUDRUN-B'
ASSERT VALUE first_month_over_half = 5 WHERE facility = 'GUDRUN-B'
ASSERT VALUE months_over_half = 1 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE first_month_over_half = 12 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE months_over_half = 0 WHERE facility = 'UTGARD-C'
SELECT facility,
       COUNT(*) FILTER (WHERE water_m3 > oil_m3)          AS months_over_half,
       MIN(month) FILTER (WHERE water_m3 > oil_m3)        AS first_month_over_half
FROM {{zone_name}}.production.production_history
GROUP BY facility
ORDER BY facility;


-- ============================================================================
-- 11. THE DECLINE
-- ============================================================================
-- Every facility produces less oil in December than in January, which is what
-- decline means. UTGARD-C is the steepest: it starts highest and loses 38
-- percent of its rate over the year.

ASSERT ROW_COUNT = 3
ASSERT VALUE january_oil_m3 = 57350 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE december_oil_m3 = 40560 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE january_oil_m3 = 30380 WHERE facility = 'GUDRUN-B'
ASSERT VALUE december_oil_m3 = 24601 WHERE facility = 'GUDRUN-B'
ASSERT VALUE january_oil_m3 = 74400 WHERE facility = 'UTGARD-C'
ASSERT VALUE december_oil_m3 = 46408 WHERE facility = 'UTGARD-C'
ASSERT VALUE decline_pct = 38 WHERE facility = 'UTGARD-C'
SELECT facility,
       CAST(ROUND(MAX(oil_m3) FILTER (WHERE month = 1)) AS BIGINT)  AS january_oil_m3,
       CAST(ROUND(MAX(oil_m3) FILTER (WHERE month = 12)) AS BIGINT) AS december_oil_m3,
       CAST(ROUND(100.0 * (1.0 - MAX(oil_m3) FILTER (WHERE month = 12)
                              / MAX(oil_m3) FILTER (WHERE month = 1))) AS BIGINT) AS decline_pct
FROM {{zone_name}}.production.production_history
GROUP BY facility
ORDER BY facility;


-- ============================================================================
-- 12. EVERY RETURN LANDED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT h.source_file, h.curated, r.landed
FROM (
    SELECT source_file, COUNT(*) AS curated
    FROM {{zone_name}}.production.production_history
    GROUP BY source_file
) h
JOIN (
    SELECT df_file_name, COUNT(*) AS landed
    FROM {{zone_name}}.production.production_reports
    GROUP BY df_file_name
) r
  ON r.df_file_name = h.source_file
WHERE h.curated <> r.landed;


-- ============================================================================
-- 13. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT r.df_file_name
FROM {{zone_name}}.production.production_reports r
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.production.production_history h
    WHERE h.source_file = r.df_file_name
);


-- ============================================================================
-- 14. THE STATE AFTER THE FIRST DELIVERY, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 24
SELECT *
FROM {{zone_name}}.production.production_history VERSION AS OF 1;


-- ============================================================================
-- 15. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.production.production_history;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The annual return as it would be filed: one row per facility, the year
-- totalled, and how watered out each one is.

ASSERT ROW_COUNT = 3
ASSERT VALUE periods = 12 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE delivered_on = '2026-03-11' WHERE facility = 'SLEIPNER-A'
ASSERT VALUE annual_oil_m3 = 570752 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE water_cut_pct = 40 WHERE facility = 'SLEIPNER-A'
ASSERT VALUE periods = 12 WHERE facility = 'GUDRUN-B'
ASSERT VALUE water_cut_pct = 54 WHERE facility = 'GUDRUN-B'
ASSERT VALUE months_over_half = 8 WHERE facility = 'GUDRUN-B'
ASSERT VALUE periods = 12 WHERE facility = 'UTGARD-C'
ASSERT VALUE delivered_on = '2026-03-12' WHERE facility = 'UTGARD-C'
ASSERT VALUE annual_oil_m3 = 698673 WHERE facility = 'UTGARD-C'
ASSERT VALUE months_over_half = 0 WHERE facility = 'UTGARD-C'
SELECT facility,
       MIN(delivered_on)                            AS delivered_on,
       COUNT(*)                                     AS periods,
       CAST(ROUND(SUM(oil_m3)) AS BIGINT)           AS annual_oil_m3,
       CAST(ROUND(SUM(water_m3)) AS BIGINT)         AS annual_water_m3,
       CAST(ROUND(100.0 * SUM(water_m3) / (SUM(oil_m3) + SUM(water_m3))) AS BIGINT) AS water_cut_pct,
       COUNT(*) FILTER (WHERE water_m3 > oil_m3)    AS months_over_half
FROM {{zone_name}}.production.production_history
GROUP BY facility
ORDER BY facility;
