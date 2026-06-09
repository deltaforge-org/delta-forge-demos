-- ============================================================================
-- XML Subtree Capture — Verification Queries
-- ============================================================================
-- Each query verifies a specific aspect of xml_paths and nested_output_format.
-- ============================================================================


-- ============================================================================
-- 1. JSON TABLE ROW COUNT — 3 products (file 1) + 2 products (file 2) = 5
-- ============================================================================

ASSERT ROW_COUNT = 5
SELECT *
FROM {{zone_name}}.xml_demos.products_json;


-- ============================================================================
-- 2. XML TABLE ROW COUNT — same 5 products
-- ============================================================================

ASSERT ROW_COUNT = 5
SELECT *
FROM {{zone_name}}.xml_demos.products_xml;


-- ============================================================================
-- 3. BROWSE JSON TABLE — top-level fields + captured subtrees
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE catalog_product_name = 'Industrial Sensor Module' WHERE catalog_product_attr_id = 'PRD-001'
ASSERT VALUE catalog_product_category = 'Electronics' WHERE catalog_product_attr_id = 'PRD-001'
ASSERT VALUE catalog_product_price = '249.99' WHERE catalog_product_attr_id = 'PRD-001'
ASSERT VALUE catalog_product_price_attr_currency = 'USD' WHERE catalog_product_attr_id = 'PRD-001'
ASSERT VALUE catalog_product_attr_status = 'active' WHERE catalog_product_attr_id = 'PRD-001'
ASSERT VALUE catalog_product_name = 'Wireless Gateway' WHERE catalog_product_attr_id = 'PRD-002'
ASSERT VALUE catalog_product_category = 'Networking' WHERE catalog_product_attr_id = 'PRD-002'
ASSERT VALUE catalog_product_price = '599.00' WHERE catalog_product_attr_id = 'PRD-002'
ASSERT VALUE catalog_product_attr_status = 'discontinued' WHERE catalog_product_attr_id = 'PRD-003'
ASSERT VALUE catalog_product_name = 'Legacy Controller Board' WHERE catalog_product_attr_id = 'PRD-003'
ASSERT VALUE catalog_product_price_attr_currency = 'EUR' WHERE catalog_product_attr_id = 'PRD-004'
ASSERT VALUE catalog_product_name = 'Fiber Optic Transceiver' WHERE catalog_product_attr_id = 'PRD-005'
SELECT catalog_product_attr_id, catalog_product_attr_status, catalog_product_name, catalog_product_category, catalog_product_price, catalog_product_price_attr_currency,
       catalog_product_specifications, catalog_product_supplier, catalog_product_tags
FROM {{zone_name}}.xml_demos.products_json
ORDER BY catalog_product_attr_id;


-- ============================================================================
-- 4. BROWSE XML TABLE — same fields but subtrees as XML fragments
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE catalog_product_name = 'Industrial Sensor Module' WHERE catalog_product_attr_id = 'PRD-001'
ASSERT VALUE catalog_product_category = 'Electronics' WHERE catalog_product_attr_id = 'PRD-001'
ASSERT VALUE catalog_product_price = '249.99' WHERE catalog_product_attr_id = 'PRD-001'
ASSERT VALUE catalog_product_name = 'Wireless Gateway' WHERE catalog_product_attr_id = 'PRD-002'
ASSERT VALUE catalog_product_name = 'Power Supply Unit' WHERE catalog_product_attr_id = 'PRD-004'
ASSERT VALUE catalog_product_price_attr_currency = 'EUR' WHERE catalog_product_attr_id = 'PRD-004'
SELECT catalog_product_attr_id, catalog_product_attr_status, catalog_product_name, catalog_product_category, catalog_product_price, catalog_product_price_attr_currency,
       catalog_product_specifications, catalog_product_supplier, catalog_product_tags
FROM {{zone_name}}.xml_demos.products_xml
ORDER BY catalog_product_attr_id;


-- ============================================================================
-- 5. JSON SUBTREE FORMAT — specs_json should start with { (JSON object)
-- ============================================================================
-- When nested_output_format is "json", captured subtrees are serialized
-- as JSON objects with underscore-joined keys.

ASSERT VALUE specs_json_count = 5
SELECT COUNT(*) FILTER (WHERE catalog_product_specifications LIKE '{%') AS specs_json_count
FROM {{zone_name}}.xml_demos.products_json;


-- ============================================================================
-- 6. XML SUBTREE FORMAT — specs_xml should start with < (XML fragment)
-- ============================================================================
-- When nested_output_format is "xml", captured subtrees are serialized
-- as raw XML fragment strings preserving original element structure.

ASSERT VALUE specs_xml_count = 5
SELECT COUNT(*) FILTER (WHERE catalog_product_specifications LIKE '<%') AS specs_xml_count
FROM {{zone_name}}.xml_demos.products_xml;


-- ============================================================================
-- 7. JSON SUPPLIER CAPTURE — supplier_json contains company name
-- ============================================================================
-- The supplier subtree includes company, contact, and address. When
-- captured as JSON, the company name should appear in the string.

ASSERT VALUE supplier_json_count = 5
SELECT COUNT(*) FILTER (WHERE catalog_product_supplier LIKE '%TechParts%'
                           OR catalog_product_supplier LIKE '%NetCore%'
                           OR catalog_product_supplier LIKE '%EuroPower%') AS supplier_json_count
FROM {{zone_name}}.xml_demos.products_json;


-- ============================================================================
-- 8. XML SUPPLIER CAPTURE — supplier_xml contains company element
-- ============================================================================

ASSERT VALUE supplier_xml_count = 5
SELECT COUNT(*) FILTER (WHERE catalog_product_supplier LIKE '%TechParts%'
                           OR catalog_product_supplier LIKE '%NetCore%'
                           OR catalog_product_supplier LIKE '%EuroPower%') AS supplier_xml_count
FROM {{zone_name}}.xml_demos.products_xml;


-- ============================================================================
-- 9. TOP-LEVEL FIELDS STILL FLATTENED — product_name is not NULL
-- ============================================================================
-- xml_paths only affects the specified subtrees. Other paths should
-- still be flattened normally into their own columns.

ASSERT VALUE top_level_count = 5
SELECT COUNT(*) FILTER (WHERE catalog_product_name IS NOT NULL AND catalog_product_category IS NOT NULL) AS top_level_count
FROM {{zone_name}}.xml_demos.products_json;


-- ============================================================================
-- 10. SPOT CHECK PRD-001 JSON — verify specs contain expected keys
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE catalog_product_name = 'Industrial Sensor Module'
SELECT catalog_product_attr_id, catalog_product_name, catalog_product_specifications
FROM {{zone_name}}.xml_demos.products_json
WHERE catalog_product_attr_id = 'PRD-001';


-- ============================================================================
-- 11. SPOT CHECK PRD-001 XML — verify specs contain original XML elements
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE catalog_product_name = 'Industrial Sensor Module'
SELECT catalog_product_attr_id, catalog_product_name, catalog_product_specifications
FROM {{zone_name}}.xml_demos.products_xml
WHERE catalog_product_attr_id = 'PRD-001';


-- ============================================================================
-- 12. COMPARE FORMATS — side-by-side JSON vs XML for same product
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE catalog_product_name = 'Wireless Gateway'
SELECT j.catalog_product_attr_id,
       j.catalog_product_name,
       j.catalog_product_specifications,
       x.catalog_product_specifications
FROM {{zone_name}}.xml_demos.products_json j
JOIN {{zone_name}}.xml_demos.products_xml x ON j.catalog_product_attr_id = x.catalog_product_attr_id
WHERE j.catalog_product_attr_id = 'PRD-002';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: verifies row counts, subtree format detection,
-- supplier capture completeness, and top-level field flattening in one query.

ASSERT ROW_COUNT = 7
SELECT 'json_row_count' AS check_name,
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END AS result
FROM {{zone_name}}.xml_demos.products_json
UNION ALL
SELECT 'xml_row_count',
       CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.products_xml
UNION ALL
SELECT 'specs_is_json',
       CASE WHEN COUNT(*) FILTER (WHERE catalog_product_specifications LIKE '{%') = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.products_json
UNION ALL
SELECT 'specs_is_xml',
       CASE WHEN COUNT(*) FILTER (WHERE catalog_product_specifications LIKE '<%') = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.products_xml
UNION ALL
SELECT 'supplier_json_has_company',
       CASE WHEN COUNT(*) FILTER (WHERE catalog_product_supplier LIKE '%TechParts%'
                                    OR catalog_product_supplier LIKE '%NetCore%'
                                    OR catalog_product_supplier LIKE '%EuroPower%') = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.products_json
UNION ALL
SELECT 'supplier_xml_has_company',
       CASE WHEN COUNT(*) FILTER (WHERE catalog_product_supplier LIKE '%TechParts%'
                                    OR catalog_product_supplier LIKE '%NetCore%'
                                    OR catalog_product_supplier LIKE '%EuroPower%') = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.products_xml
UNION ALL
SELECT 'top_level_flattened',
       CASE WHEN COUNT(*) FILTER (WHERE catalog_product_name IS NOT NULL AND catalog_product_category IS NOT NULL) = 5
            THEN 'PASS' ELSE 'FAIL' END
FROM {{zone_name}}.xml_demos.products_json
ORDER BY check_name;
