-- ============================================================================
-- Delta Views & Data Masking — MASK() Function Demo — Educational Queries
-- ============================================================================
-- WHAT: The MASK() function applies pseudonymisation transforms inline in SQL,
--       reusing DeltaForge's pseudonymisation engine (redact, keyed_hash,
--       generalize, encrypt, tokenize) as a composable scalar function.
-- WHY:  Production databases serve multiple audiences. Views with MASK() let
--       SQL authors enforce role-based data access without table-level policy.
--       Each view applies different masking levels to the same base table.
-- HOW:  MASK(value, transform [, param_key, param_value, ...]) applies the
--       named transform to each value. Unlike manual string concat, MASK()
--       handles format-aware redaction (email, phone, card, SSN), deterministic
--       hashing (JOINable pseudonyms), date generalisation, and more.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Full Order Table (All Columns Visible)
-- ============================================================================
-- The base table has 13 columns including sensitive fields: credit_card_last4,
-- shipping_address, phone, customer_email. Without masking, everyone with
-- table access sees everything.

ASSERT ROW_COUNT = 3
SELECT id, customer_name, customer_email, credit_card_last4, phone, product, order_total
FROM {{zone_name}}.delta_demos.customer_orders
WHERE id IN (1, 2, 19)
ORDER BY id;


-- ============================================================================
-- LEARN: MASK() Basics — Redaction Modes
-- ============================================================================
-- The MASK function's 'redact' transform supports format-aware masking modes.
-- Each mode understands the structure of its data type and masks accordingly.

ASSERT ROW_COUNT = 3
ASSERT VALUE masked_card = '****-****-****-4532' WHERE id = 1
ASSERT VALUE masked_email = 'a****@shop.com' WHERE id = 1
ASSERT VALUE masked_phone = '****0301' WHERE id = 1
SELECT id,
       MASK(credit_card_last4, 'redact', 'mode', 'card')  AS masked_card,
       MASK(customer_email, 'redact', 'mode', 'email')     AS masked_email,
       MASK(phone, 'redact', 'mode', 'phone')              AS masked_phone
FROM {{zone_name}}.delta_demos.customer_orders
WHERE id IN (1, 2, 19)
ORDER BY id;


-- ============================================================================
-- LEARN: MASK() Generalisation — Reducing Date Precision
-- ============================================================================
-- The 'generalize' transform reduces data precision. Dates can be collapsed
-- to year, month, quarter, or decade — useful for analytics that don't need
-- exact dates.

ASSERT ROW_COUNT = 3
ASSERT VALUE order_year = '2024' WHERE id = 1
ASSERT VALUE order_quarter = '2024Q1' WHERE id = 1
SELECT id, order_date,
       MASK(order_date, 'generalize', 'granularity', 'year')    AS order_year,
       MASK(order_date, 'generalize', 'granularity', 'quarter') AS order_quarter
FROM {{zone_name}}.delta_demos.customer_orders
WHERE id IN (1, 2, 19)
ORDER BY id;


-- ============================================================================
-- STEP 1: CREATE ANALYST VIEW — Pseudonymised Identity, Full Metrics
-- ============================================================================
-- Analysts need order metrics but must not identify individual customers.
-- MASK with 'keyed_hash' produces a deterministic pseudonym — the same name
-- always maps to the same hash, so analysts can still COUNT DISTINCT or GROUP
-- BY customer without seeing real names.

CREATE VIEW {{zone_name}}.delta_demos.orders_analyst AS
SELECT id,
       MASK(customer_name, 'keyed_hash') AS customer_pseudonym,
       product, quantity, unit_price, order_total,
       order_status, order_date, region
FROM {{zone_name}}.delta_demos.customer_orders;


-- ============================================================================
-- EXPLORE: Analyst View — Pseudonymised Customers, Full Metrics
-- ============================================================================
-- The analyst sees a deterministic hash instead of a real name.
-- Same customer always produces the same pseudonym — enables GROUP BY.

ASSERT ROW_COUNT = 3
ASSERT VALUE order_total = 1299.99 WHERE id = 1
SELECT id, customer_pseudonym, product, order_total, region
FROM {{zone_name}}.delta_demos.orders_analyst
WHERE id IN (1, 2, 19)
ORDER BY id;


-- ============================================================================
-- STEP 2: CREATE SUPPORT VIEW — Customer Context with Masked PII
-- ============================================================================
-- Support agents need customer names and contact info to handle tickets,
-- but must never see full credit card numbers, raw email, or phone.
-- MASK applies format-aware redaction to each sensitive field.

CREATE VIEW {{zone_name}}.delta_demos.orders_support AS
SELECT id, customer_name,
       MASK(customer_email, 'redact', 'mode', 'email')            AS masked_email,
       MASK(credit_card_last4, 'redact', 'mode', 'card')          AS masked_card,
       MASK(phone, 'redact', 'mode', 'phone')                     AS masked_phone,
       product, order_total, order_status, order_date
FROM {{zone_name}}.delta_demos.customer_orders;


-- ============================================================================
-- EXPLORE: Support View — Masked Payment & Contact, Full Customer Name
-- ============================================================================
-- Support sees the customer name (needed for tickets) but masked card & phone.

ASSERT ROW_COUNT = 2
ASSERT VALUE masked_card = '****-****-****-4532' WHERE id = 1
SELECT id, customer_name, masked_card, masked_phone, order_status
FROM {{zone_name}}.delta_demos.orders_support
WHERE id IN (1, 19)
ORDER BY id;


-- ============================================================================
-- STEP 3: CREATE EXECUTIVE VIEW — Aggregated Regional Summaries
-- ============================================================================
-- Executives need high-level KPIs: revenue per region, order counts, return
-- rates. They should never see individual records. This view pre-aggregates
-- by region, eliminating row-level detail entirely. No MASK needed — the
-- aggregation itself is the access boundary.

CREATE VIEW {{zone_name}}.delta_demos.orders_executive AS
SELECT region,
       COUNT(*) AS total_orders,
       SUM(order_total) AS revenue,
       COUNT(DISTINCT customer_name) AS unique_customers,
       COUNT(*) FILTER (WHERE order_status = 'returned') AS returns
FROM {{zone_name}}.delta_demos.customer_orders
GROUP BY region;


-- ============================================================================
-- EXPLORE: Executive View — Regional KPIs at a Glance
-- ============================================================================
-- 6 regions, each with pre-computed revenue and return counts.

ASSERT ROW_COUNT = 6
ASSERT VALUE total_orders = 13 WHERE region = 'Europe'
SELECT *
FROM {{zone_name}}.delta_demos.orders_executive
ORDER BY revenue DESC;


-- ============================================================================
-- LEARN: Revenue by Product (Through Analyst View)
-- ============================================================================
-- The analyst view supports full aggregation on the projected columns.
-- No PII leaks even when analysts run ad-hoc GROUP BY queries.

ASSERT ROW_COUNT = 7
ASSERT VALUE product_revenue = 9099.93 WHERE product = 'Laptop Pro 15'
SELECT product, COUNT(*) AS orders, SUM(order_total) AS product_revenue
FROM {{zone_name}}.delta_demos.orders_analyst
GROUP BY product
ORDER BY product_revenue DESC;


-- ============================================================================
-- LEARN: Order Status Distribution (Through Analyst View)
-- ============================================================================
-- Status breakdown across all orders — same data an operations dashboard uses.

ASSERT ROW_COUNT = 4
ASSERT VALUE status_count = 19 WHERE order_status = 'delivered'
SELECT order_status, COUNT(*) AS status_count
FROM {{zone_name}}.delta_demos.orders_analyst
GROUP BY order_status
ORDER BY status_count DESC;


-- ============================================================================
-- EXPLORE: Returns Investigation (Through Support View)
-- ============================================================================
-- Support agents investigate returns using the masked view. They see the
-- customer name, product, and masked payment info — enough context to process
-- the return without exposing raw card numbers or phone.

ASSERT ROW_COUNT = 2
SELECT id, customer_name, masked_card, masked_phone, product, order_total, order_date
FROM {{zone_name}}.delta_demos.orders_support
WHERE order_status = 'returned'
ORDER BY order_date;


-- ============================================================================
-- LEARN: Inline MASK() Without a View — Ad-hoc Masking
-- ============================================================================
-- MASK() can be used directly in any SELECT — no view required. This is
-- useful for ad-hoc queries where creating a view isn't worth it.

ASSERT ROW_COUNT = 3
ASSERT VALUE masked_address = '[REDACTED]' WHERE id = 1
SELECT id, customer_name,
       MASK(shipping_address, 'redact') AS masked_address,
       MASK(customer_email, 'redact', 'mode', 'email') AS masked_email,
       order_total
FROM {{zone_name}}.delta_demos.customer_orders
WHERE id IN (1, 2, 19)
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify base table has 30 rows
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.customer_orders;

-- Verify analyst view has 30 rows (with pseudonymised names)
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.orders_analyst;

-- Verify support view has 30 rows (with masked PII)
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.orders_support;

-- Verify executive view has 6 regions
ASSERT VALUE region_count = 6
SELECT COUNT(*) AS region_count FROM {{zone_name}}.delta_demos.orders_executive;

-- Verify total revenue
ASSERT VALUE total_revenue = 15582.61
SELECT SUM(order_total) AS total_revenue FROM {{zone_name}}.delta_demos.customer_orders;

-- Verify status counts
ASSERT VALUE delivered_count = 19
SELECT COUNT(*) AS delivered_count FROM {{zone_name}}.delta_demos.customer_orders WHERE order_status = 'delivered';

ASSERT VALUE returned_count = 2
SELECT COUNT(*) AS returned_count FROM {{zone_name}}.delta_demos.customer_orders WHERE order_status = 'returned';

ASSERT VALUE pending_count = 3
SELECT COUNT(*) AS pending_count FROM {{zone_name}}.delta_demos.customer_orders WHERE order_status = 'pending';

ASSERT VALUE shipped_count = 6
SELECT COUNT(*) AS shipped_count FROM {{zone_name}}.delta_demos.customer_orders WHERE order_status = 'shipped';

-- Verify 7 distinct products
ASSERT VALUE product_count = 7
SELECT COUNT(DISTINCT product) AS product_count FROM {{zone_name}}.delta_demos.customer_orders;

-- Verify analyst view revenue matches base table
ASSERT VALUE analyst_revenue = 15582.61
SELECT SUM(order_total) AS analyst_revenue FROM {{zone_name}}.delta_demos.orders_analyst;
