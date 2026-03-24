-- ============================================================================
-- Northwind Trading Company — Demo Queries
-- ============================================================================
-- Cross-table queries showcasing joins, aggregations, and business analytics
-- across the 11 Northwind tables.
--
-- Relationships:
--   nw_customers ──< nw_orders ──< nw_order_details >── nw_products >── nw_categories
--                       │                                    │
--                       └── nw_employees                     └── nw_suppliers
--                             │
--                             └──< nw_employee_territories >── nw_territories >── nw_regions
-- ============================================================================


-- ============================================================================
-- 1. Top 10 Customers by Total Order Value
-- ============================================================================
-- Joins: nw_customers → nw_orders → nw_order_details
--
-- Expected results (top 5):
--   QUICK-Stop            | 28 orders | 110,277.31
--   Ernst Handel          | 30 orders | 104,874.98
--   Save-a-lot Markets    | 31 orders | 104,361.95
--   Rattlesnake Canyon Grocery | 18 orders | 51,097.80
--   Hungry Owl All-Night Grocers | 19 orders | 49,979.91

ASSERT ROW_COUNT = 10
ASSERT WARNING VALUE total_value = 110277.31 WHERE company_name = 'QUICK-Stop'
ASSERT WARNING VALUE total_value = 104874.98 WHERE company_name = 'Ernst Handel'
ASSERT WARNING VALUE total_value = 104361.95 WHERE company_name = 'Save-a-lot Markets'
ASSERT WARNING VALUE order_count = 28 WHERE company_name = 'QUICK-Stop'
SELECT
    c.company_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(SUM(CAST(od.unit_price AS DOUBLE) * CAST(od.quantity AS INT) * (1 - CAST(od.discount AS DOUBLE))), 2) AS total_value
FROM {{zone_name}}.csv.nw_customers c
JOIN {{zone_name}}.csv.nw_orders o ON c.customer_id = o.customer_id
JOIN {{zone_name}}.csv.nw_order_details od ON o.order_id = od.order_id
GROUP BY c.company_name
ORDER BY total_value DESC
LIMIT 10;


-- ============================================================================
-- 2. Revenue by Product Category
-- ============================================================================
-- Joins: nw_order_details → nw_products → nw_categories
--
-- Expected results (all 8 categories):
--   Beverages      | 12 products | 267,868.18
--   Dairy Products | 10 products | 234,507.28
--   Confections    | 13 products | 167,357.22
--   Meat/Poultry   |  6 products | 163,022.36
--   Seafood        | 12 products | 131,261.74
--   Condiments     | 12 products | 106,047.09
--   Produce        |  5 products |  99,984.58
--   Grains/Cereals |  7 products |  95,744.59

ASSERT ROW_COUNT = 8
ASSERT WARNING VALUE total_revenue = 267868.18 WHERE category_name = 'Beverages'
ASSERT WARNING VALUE total_revenue = 234507.28 WHERE category_name = 'Dairy Products'
ASSERT WARNING VALUE total_revenue = 167357.22 WHERE category_name = 'Confections'
ASSERT WARNING VALUE total_revenue = 163022.36 WHERE category_name = 'Meat/Poultry'
ASSERT WARNING VALUE total_revenue = 131261.74 WHERE category_name = 'Seafood'
ASSERT WARNING VALUE total_revenue = 106047.09 WHERE category_name = 'Condiments'
ASSERT WARNING VALUE total_revenue = 99984.58 WHERE category_name = 'Produce'
ASSERT WARNING VALUE total_revenue = 95744.59 WHERE category_name = 'Grains/Cereals'
SELECT
    cat.category_name,
    COUNT(DISTINCT p.product_id) AS product_count,
    ROUND(SUM(CAST(od.unit_price AS DOUBLE) * CAST(od.quantity AS INT) * (1 - CAST(od.discount AS DOUBLE))), 2) AS total_revenue
FROM {{zone_name}}.csv.nw_order_details od
JOIN {{zone_name}}.csv.nw_products p ON od.product_id = p.product_id
JOIN {{zone_name}}.csv.nw_categories cat ON p.category_id = cat.category_id
GROUP BY cat.category_name
ORDER BY total_revenue DESC;


-- ============================================================================
-- 3. Employee Sales Performance
-- ============================================================================
-- Joins: nw_employees → nw_orders → nw_order_details
--
-- Expected results (all 9 employees):
--   Margaret Peacock | Sales Representative        | 156 orders | 232,890.85
--   Janet Leverling  | Sales Representative        | 127 orders | 202,812.84
--   Nancy Davolio    | Sales Representative        | 123 orders | 192,107.60
--   Andrew Fuller    | Vice President, Sales       |  96 orders | 166,537.76
--   Laura Callahan   | Inside Sales Coordinator    | 104 orders | 126,862.28
--   Robert King      | Sales Representative        |  72 orders | 124,568.24
--   Anne Dodsworth   | Sales Representative        |  43 orders |  77,308.07
--   Michael Suyama   | Sales Representative        |  67 orders |  73,913.13
--   Steven Buchanan  | Sales Manager               |  42 orders |  68,792.28

ASSERT ROW_COUNT = 9
ASSERT WARNING VALUE total_sales = 232890.85 WHERE employee_name = 'Margaret Peacock'
ASSERT WARNING VALUE total_sales = 202812.84 WHERE employee_name = 'Janet Leverling'
ASSERT WARNING VALUE total_sales = 192107.60 WHERE employee_name = 'Nancy Davolio'
ASSERT WARNING VALUE orders_handled = 156 WHERE employee_name = 'Margaret Peacock'
ASSERT WARNING VALUE orders_handled = 42 WHERE employee_name = 'Steven Buchanan'
SELECT
    e.first_name || ' ' || e.last_name AS employee_name,
    e.title,
    COUNT(DISTINCT o.order_id) AS orders_handled,
    ROUND(SUM(CAST(od.unit_price AS DOUBLE) * CAST(od.quantity AS INT) * (1 - CAST(od.discount AS DOUBLE))), 2) AS total_sales
FROM {{zone_name}}.csv.nw_employees e
JOIN {{zone_name}}.csv.nw_orders o ON e.employee_id = o.employee_id
JOIN {{zone_name}}.csv.nw_order_details od ON o.order_id = od.order_id
GROUP BY e.first_name, e.last_name, e.title
ORDER BY total_sales DESC;


-- ============================================================================
-- 4. Monthly Order Trends
-- ============================================================================
-- Single table: nw_orders
-- 23 months from July 1996 to May 1998
--
-- Expected results (first 3 and last 3 months):
--   1996-07 | 22 orders |  1,288.18 freight
--   1996-08 | 25 orders |  1,397.17 freight
--   1996-09 | 23 orders |  1,123.48 freight
--   ...
--   1998-03 | 73 orders |  5,379.02 freight
--   1998-04 | 74 orders |  6,393.57 freight
--   1998-05 | 14 orders |    685.08 freight

ASSERT ROW_COUNT = 23
ASSERT WARNING VALUE total_freight = 1288.18 WHERE order_count = 22
ASSERT WARNING VALUE total_freight = 685.08 WHERE order_count = 14
SELECT
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(MONTH FROM o.order_date) AS month,
    COUNT(*) AS order_count,
    ROUND(SUM(CAST(o.freight AS DOUBLE)), 2) AS total_freight
FROM {{zone_name}}.csv.nw_orders o
GROUP BY year, month
ORDER BY year, month;


-- ============================================================================
-- 5. Products Below Reorder Level (Need Restocking)
-- ============================================================================
-- Joins: nw_products → nw_categories + nw_products → nw_suppliers
--
-- Expected results: 18 products below reorder level (not discontinued)
-- Top 3 by restock urgency:
--   Gorgonzola Telino    | Dairy Products | stock: 0  | reorder: 20 | on order: 70
--   Mascarpone Fabioli   | Dairy Products | stock: 9  | reorder: 25 | on order: 40
--   Louisiana Hot Spiced Okra | Condiments | stock: 4  | reorder: 20 | on order: 100

ASSERT ROW_COUNT = 18
ASSERT WARNING VALUE units_in_stock = '0' WHERE product_name = 'Gorgonzola Telino'
ASSERT WARNING VALUE category_name = 'Dairy Products' WHERE product_name = 'Gorgonzola Telino'
ASSERT WARNING VALUE category_name = 'Condiments' WHERE product_name = 'Louisiana Hot Spiced Okra'
SELECT
    p.product_name,
    cat.category_name,
    s.company_name AS supplier,
    p.units_in_stock,
    p.reorder_level,
    p.units_on_order
FROM {{zone_name}}.csv.nw_products p
JOIN {{zone_name}}.csv.nw_categories cat ON p.category_id = cat.category_id
JOIN {{zone_name}}.csv.nw_suppliers s ON p.supplier_id = s.supplier_id
WHERE CAST(p.units_in_stock AS INT) < CAST(p.reorder_level AS INT)
  AND CAST(p.discontinued AS INT) = 0
ORDER BY (CAST(p.reorder_level AS INT) - CAST(p.units_in_stock AS INT)) DESC;


-- ============================================================================
-- 6. Shipping Analysis by Carrier
-- ============================================================================
-- Joins: nw_orders → nw_shippers, nw_orders → nw_order_details
--
-- Expected results (all 3 carriers):
--   United Package   | 326 shipments | avg freight: 86.64  | value: 533,547.63
--   Federal Shipping | 255 shipments | avg freight: 80.44  | value: 383,405.47
--   Speedy Express   | 249 shipments | avg freight: 65.00  | value: 348,839.94

ASSERT ROW_COUNT = 3
ASSERT WARNING VALUE shipments = 326 WHERE shipper = 'United Package'
ASSERT WARNING VALUE shipments = 255 WHERE shipper = 'Federal Shipping'
ASSERT WARNING VALUE shipments = 249 WHERE shipper = 'Speedy Express'
ASSERT WARNING VALUE total_order_value = 533547.63 WHERE shipper = 'United Package'
ASSERT WARNING VALUE total_order_value = 383405.47 WHERE shipper = 'Federal Shipping'
ASSERT WARNING VALUE total_order_value = 348839.94 WHERE shipper = 'Speedy Express'
SELECT
    sh.company_name AS shipper,
    COUNT(DISTINCT o.order_id) AS shipments,
    ROUND(AVG(CAST(o.freight AS DOUBLE)), 2) AS avg_freight,
    ROUND(SUM(CAST(od.unit_price AS DOUBLE) * CAST(od.quantity AS INT) * (1 - CAST(od.discount AS DOUBLE))), 2) AS total_order_value
FROM {{zone_name}}.csv.nw_orders o
JOIN {{zone_name}}.csv.nw_shippers sh ON o.ship_via = sh.shipper_id
JOIN {{zone_name}}.csv.nw_order_details od ON o.order_id = od.order_id
GROUP BY sh.company_name
ORDER BY shipments DESC;


-- ============================================================================
-- 7. Customer Orders by Country
-- ============================================================================
-- Joins: nw_customers → nw_orders
--
-- Expected results (top 5 by order count):
--   Germany   | 11 customers | 122 orders | avg freight: 92.49
--   USA       | 13 customers | 122 orders | avg freight: 112.88
--   Brazil    |  9 customers |  83 orders | avg freight: 58.80
--   France    | 10 customers |  77 orders | avg freight: 55.04
--   UK        |  7 customers |  56 orders | avg freight: 52.75

ASSERT ROW_COUNT = 21
ASSERT WARNING VALUE order_count = 122 WHERE country = 'Germany'
ASSERT WARNING VALUE order_count = 122 WHERE country = 'USA'
ASSERT WARNING VALUE customer_count = 11 WHERE country = 'Germany'
ASSERT WARNING VALUE customer_count = 13 WHERE country = 'USA'
SELECT
    c.country,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(AVG(CAST(o.freight AS DOUBLE)), 2) AS avg_freight
FROM {{zone_name}}.csv.nw_customers c
JOIN {{zone_name}}.csv.nw_orders o ON c.customer_id = o.customer_id
GROUP BY c.country
ORDER BY order_count DESC;


-- ============================================================================
-- 8. Employee Territory Coverage
-- ============================================================================
-- Joins: nw_employees → nw_employee_territories → nw_territories → nw_regions
--
-- Expected results (9 employees across 4 regions):
--   Andrew Fuller    | Eastern  | 7 territories
--   Anne Dodsworth   | Northern | 7 territories
--   Janet Leverling  | Southern | 4 territories
--   Laura Callahan   | Northern | 4 territories
--   Margaret Peacock | Eastern  | 3 territories
--   Michael Suyama   | Western  | 5 territories
--   Nancy Davolio    | Eastern  | 2 territories
--   Robert King      | Western  | 10 territories
--   Steven Buchanan  | Eastern  | 7 territories

ASSERT ROW_COUNT = 9
ASSERT WARNING VALUE territory_count = 7 WHERE employee_name = 'Andrew Fuller'
ASSERT WARNING VALUE territory_count = 10 WHERE employee_name = 'Robert King'
ASSERT WARNING VALUE territory_count = 2 WHERE employee_name = 'Nancy Davolio'
ASSERT WARNING VALUE territory_count = 5 WHERE employee_name = 'Michael Suyama'
SELECT
    e.first_name || ' ' || e.last_name AS employee_name,
    r.region_description AS region,
    COUNT(t.territory_id) AS territory_count
FROM {{zone_name}}.csv.nw_employees e
JOIN {{zone_name}}.csv.nw_employee_territories et ON e.employee_id = et.employee_id
JOIN {{zone_name}}.csv.nw_territories t ON et.territory_id = t.territory_id
JOIN {{zone_name}}.csv.nw_regions r ON t.region_id = r.region_id
GROUP BY e.first_name, e.last_name, r.region_description
ORDER BY employee_name, region;


-- ============================================================================
-- 9. Top Suppliers by Revenue
-- ============================================================================
-- Joins: nw_suppliers → nw_products → nw_order_details
--
-- Expected results (top 5):
--   Aux joyeux ecclésiastiques           | France    | 2 products | 153,691.28
--   Plutzer Lebensmittelgroßmärkte AG    | Germany   | 5 products | 145,372.40
--   Gai pâturage                         | France    | 2 products | 117,981.18
--   Pavlova; Ltd.                        | Australia | 5 products | 106,459.78
--   G'day; Mate                          | Australia | 3 products |  65,626.77

ASSERT ROW_COUNT = 10
ASSERT WARNING VALUE total_revenue = 153691.28 WHERE supplier = 'Aux joyeux ecclésiastiques'
ASSERT WARNING VALUE products_supplied = 2 WHERE supplier = 'Aux joyeux ecclésiastiques'
SELECT
    s.company_name AS supplier,
    s.country,
    COUNT(DISTINCT p.product_id) AS products_supplied,
    ROUND(SUM(CAST(od.unit_price AS DOUBLE) * CAST(od.quantity AS INT) * (1 - CAST(od.discount AS DOUBLE))), 2) AS total_revenue
FROM {{zone_name}}.csv.nw_suppliers s
JOIN {{zone_name}}.csv.nw_products p ON s.supplier_id = p.supplier_id
JOIN {{zone_name}}.csv.nw_order_details od ON p.product_id = od.product_id
GROUP BY s.company_name, s.country
ORDER BY total_revenue DESC
LIMIT 10;


-- ============================================================================
-- 10. Late Shipments — Orders Shipped After Required Date
-- ============================================================================
-- Joins: nw_orders → nw_customers
--
-- Expected results: 37 late orders
-- Most recent 3 late shipments:
--   Order 10970 | Bólido Comidas preparadas  | required 1998-04-07 | shipped 1998-04-24
--   Order 10924 | Berglunds snabbköp         | required 1998-04-01 | shipped 1998-04-08
--   Order 10927 | La corne d'abondance       | required 1998-04-02 | shipped 1998-04-08

ASSERT ROW_COUNT = 37
ASSERT WARNING VALUE company_name = 'Bólido Comidas preparadas' WHERE order_id = '10970'
ASSERT WARNING VALUE company_name = 'Berglunds snabbköp' WHERE order_id = '10924'
ASSERT WARNING VALUE company_name = 'La corne d''abondance' WHERE order_id = '10927'
SELECT
    o.order_id,
    c.company_name,
    o.order_date,
    o.required_date,
    o.shipped_date
FROM {{zone_name}}.csv.nw_orders o
JOIN {{zone_name}}.csv.nw_customers c ON o.customer_id = c.customer_id
WHERE o.shipped_date > o.required_date
ORDER BY o.shipped_date DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: grand totals and key invariants across all
-- 11 Northwind tables — confirms joins, aggregations, and data integrity.

ASSERT ROW_COUNT = 1
ASSERT WARNING VALUE total_orders = 830
ASSERT WARNING VALUE total_customers = 89
ASSERT WARNING VALUE grand_total_revenue = 1265793.04
SELECT
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    ROUND(SUM(CAST(od.unit_price AS DOUBLE) * CAST(od.quantity AS INT) * (1 - CAST(od.discount AS DOUBLE))), 2) AS grand_total_revenue
FROM {{zone_name}}.csv.nw_order_details od
JOIN {{zone_name}}.csv.nw_orders o ON od.order_id = o.order_id
JOIN {{zone_name}}.csv.nw_customers c ON o.customer_id = c.customer_id;
