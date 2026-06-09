-- ============================================================================
-- JSON Subtree Capture — Verification Queries
-- ============================================================================
-- Each query verifies a specific aspect of json_paths subtree capture,
-- contrasting captured (JSON blob) vs flattened approaches.
-- ============================================================================


-- ============================================================================
-- 1. CAPTURED TABLE ROW COUNT — 3 residential + 2 commercial = 5
-- ============================================================================

ASSERT ROW_COUNT = 5
SELECT *
FROM {{zone_name}}.json_demos.listings_captured;


-- ============================================================================
-- 2. FLATTENED TABLE ROW COUNT — same 5 listings
-- ============================================================================

ASSERT ROW_COUNT = 5
SELECT *
FROM {{zone_name}}.json_demos.listings_flattened;


-- ============================================================================
-- 3. BROWSE CAPTURED — top-level fields + JSON blobs
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE title = 'Modern Downtown Loft' WHERE id = 'LST-001'
ASSERT VALUE type = 'condo' WHERE id = 'LST-001'
ASSERT VALUE bedrooms = 2 WHERE id = 'LST-001'
ASSERT VALUE sqft = 1200 WHERE id = 'LST-001'
ASSERT VALUE status = 'active' WHERE id = 'LST-001'
ASSERT VALUE title = 'Historic Townhouse' WHERE id = 'LST-005'
ASSERT VALUE type = 'house' WHERE id = 'LST-005'
ASSERT VALUE bedrooms = 5 WHERE id = 'LST-005'
SELECT id, title, type, bedrooms, sqft, status,
       location, pricing
FROM {{zone_name}}.json_demos.listings_captured
ORDER BY id;


-- ============================================================================
-- 4. BROWSE FLATTENED — same data with individual columns
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE location_address_city = 'San Francisco' WHERE id = 'LST-001'
ASSERT VALUE location_address_state = 'CA' WHERE id = 'LST-001'
ASSERT VALUE location_neighborhood = 'SoMa' WHERE id = 'LST-001'
ASSERT VALUE pricing_list_price = 895000 WHERE id = 'LST-001'
ASSERT VALUE pricing_tax_annual = 10740 WHERE id = 'LST-001'
ASSERT VALUE location_address_city = 'Portland' WHERE id = 'LST-002'
ASSERT VALUE location_neighborhood = 'Hawthorne' WHERE id = 'LST-002'
ASSERT VALUE pricing_list_price = 4750000 WHERE id = 'LST-005'
SELECT id, title, type, bedrooms, sqft, status,
       location_address_street, location_address_city, location_address_state, location_neighborhood,
       pricing_list_price, pricing_tax_annual, pricing_mortgage_estimate_monthly_payment
FROM {{zone_name}}.json_demos.listings_flattened
ORDER BY id;


-- ============================================================================
-- 5. LOCATION IS JSON — location_json should be a JSON object
-- ============================================================================
-- When json_paths captures a subtree, the result is a JSON string.

ASSERT VALUE json_location_count = 5
SELECT COUNT(*) FILTER (WHERE location LIKE '{%') AS json_location_count
FROM {{zone_name}}.json_demos.listings_captured;


-- ============================================================================
-- 6. PRICING IS JSON — pricing_json should be a JSON object
-- ============================================================================

ASSERT VALUE json_pricing_count = 5
SELECT COUNT(*) FILTER (WHERE pricing LIKE '{%') AS json_pricing_count
FROM {{zone_name}}.json_demos.listings_captured;


-- ============================================================================
-- 7. LOCATION CONTENT — location_json contains city names
-- ============================================================================

ASSERT VALUE city_count = 5
SELECT COUNT(*) FILTER (WHERE location LIKE '%San Francisco%'
                           OR location LIKE '%Portland%'
                           OR location LIKE '%Seattle%'
                           OR location LIKE '%Austin%'
                           OR location LIKE '%Boston%') AS city_count
FROM {{zone_name}}.json_demos.listings_captured;


-- ============================================================================
-- 8. PRICING CONTENT — pricing_json contains list_price values
-- ============================================================================

ASSERT VALUE price_count = 5
SELECT COUNT(*) FILTER (WHERE pricing LIKE '%list_price%') AS price_count
FROM {{zone_name}}.json_demos.listings_captured;


-- ============================================================================
-- 9. TOP-LEVEL FIELDS STILL WORK — title and bedrooms populated
-- ============================================================================
-- json_paths only affects the specified subtrees. Other fields flatten normally.

ASSERT VALUE populated_count = 5
SELECT COUNT(*) FILTER (WHERE title IS NOT NULL AND bedrooms IS NOT NULL) AS populated_count
FROM {{zone_name}}.json_demos.listings_captured;


-- ============================================================================
-- 10. FLATTENED HAS CITY — flattened table has direct city column
-- ============================================================================

ASSERT VALUE city_count = 5
SELECT COUNT(*) FILTER (WHERE location_address_city IS NOT NULL) AS city_count
FROM {{zone_name}}.json_demos.listings_flattened;


-- ============================================================================
-- 11. SPOT CHECK LST-001 — verify captured JSON contains expected data
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE title = 'Modern Downtown Loft'
ASSERT VALUE type = 'condo'
ASSERT VALUE bedrooms = 2
ASSERT VALUE sqft = 1200
SELECT id, title, type, bedrooms, sqft, location, pricing
FROM {{zone_name}}.json_demos.listings_captured
WHERE id = 'LST-001';


-- ============================================================================
-- 12. COMPARE APPROACHES — side-by-side captured vs flattened
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE location_address_city = 'Boston'
ASSERT VALUE location_address_state = 'MA'
ASSERT VALUE location_neighborhood = 'Back Bay'
ASSERT VALUE pricing_list_price = 4750000
ASSERT VALUE pricing_mortgage_estimate_monthly_payment = 27730
SELECT c.id,
       c.title,
       c.location,
       f.location_address_city,
       f.location_address_state,
       f.location_neighborhood,
       f.pricing_list_price,
       f.pricing_mortgage_estimate_monthly_payment
FROM {{zone_name}}.json_demos.listings_captured c
JOIN {{zone_name}}.json_demos.listings_flattened f ON c.id = f.id
WHERE c.id = 'LST-005';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 8
SELECT check_name, result FROM (

    SELECT 'captured_row_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.listings_captured) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'flattened_row_count',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.listings_flattened) = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'location_is_json',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.listings_captured WHERE location LIKE '{%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'pricing_is_json',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.listings_captured WHERE pricing LIKE '{%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'location_has_city',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.listings_captured
                      WHERE location LIKE '%San Francisco%'
                         OR location LIKE '%Portland%'
                         OR location LIKE '%Seattle%'
                         OR location LIKE '%Austin%'
                         OR location LIKE '%Boston%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'pricing_has_list_price',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.listings_captured WHERE pricing LIKE '%list_price%') = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'top_level_flattened',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.listings_captured WHERE title IS NOT NULL AND bedrooms IS NOT NULL) = 5
                THEN 'PASS' ELSE 'FAIL' END

    UNION ALL

    SELECT 'flattened_has_city',
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.listings_flattened WHERE location_address_city IS NOT NULL) = 5
                THEN 'PASS' ELSE 'FAIL' END

) checks
ORDER BY check_name;
