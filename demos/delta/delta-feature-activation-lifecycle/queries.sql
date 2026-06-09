-- ============================================================================
-- Delta Feature Activation Lifecycle — Educational Queries
-- ============================================================================
-- WHAT: Delta table features are activated via ALTER TABLE SET TBLPROPERTIES,
--       not at CREATE TABLE time. Features can be added incrementally as
--       requirements evolve.
-- WHY:  Production tables often start simple and gain features over time.
--       Understanding the activation lifecycle — what changes in the protocol,
--       what new behaviors activate, and whether existing data is affected —
--       is essential for platform teams managing long-lived tables.
-- HOW:  We create a plain table, inspect its baseline protocol state, then
--       progressively activate CDC and observe the effects at each step using
--       DESCRIBE DETAIL, SHOW TBLPROPERTIES, and data operations.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Plain Table, No Features
-- ============================================================================
-- The customer_events table was created without any TBLPROPERTIES. It is a
-- standard Delta table with the minimum protocol version required for basic
-- read/write operations.

ASSERT ROW_COUNT = 3
ASSERT VALUE event_count = 17 WHERE event_type = 'purchase'
ASSERT VALUE event_count = 4 WHERE event_type = 'refund'
ASSERT VALUE event_count = 4 WHERE event_type = 'signup'
SELECT event_type,
       COUNT(*) AS event_count,
       ROUND(SUM(revenue), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.customer_events
GROUP BY event_type
ORDER BY event_count DESC;


-- ============================================================================
-- LEARN: Baseline Protocol State (Before Any Features)
-- ============================================================================
-- DESCRIBE DETAIL shows the protocol metadata. Before enabling any features,
-- the table should have low protocol versions and no table_features listed.

ASSERT VALUE value = '25' WHERE property = 'estimated_rows'
ASSERT ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.customer_events;


-- ============================================================================
-- LEARN: Baseline Properties (Empty or Minimal)
-- ============================================================================
-- SHOW TBLPROPERTIES shows the TBLPROPERTIES configuration. A plain
-- table has no custom properties — only system defaults (if any).

ASSERT ROW_COUNT >= 0
SHOW TBLPROPERTIES {{zone_name}}.delta_demos.customer_events;


-- ============================================================================
-- EXPLORE: Channel Revenue — Business Context Before Changes
-- ============================================================================
-- Before activating any features, let's establish the business baseline.

ASSERT ROW_COUNT = 5
ASSERT VALUE total_revenue = 925.0 WHERE channel = 'store'
ASSERT VALUE total_revenue = 665.0 WHERE channel = 'web'
SELECT channel,
       COUNT(*) AS events,
       ROUND(SUM(revenue), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.customer_events
GROUP BY channel
ORDER BY total_revenue DESC;


-- ============================================================================
-- ACTION: Activate Change Data Feed (CDC)
-- ============================================================================
-- The business now needs incremental ETL — downstream systems should process
-- only changed rows, not the entire table. We activate CDC by setting the
-- enableChangeDataFeed property.

ALTER TABLE {{zone_name}}.delta_demos.customer_events
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');


-- ============================================================================
-- LEARN: Protocol State After CDC Activation
-- ============================================================================
-- After enabling CDC, DESCRIBE DETAIL should show updated protocol versions
-- and the table_features should now include the change data feed capability.

ASSERT VALUE value = '25' WHERE property = 'estimated_rows'
ASSERT ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.customer_events;


-- ============================================================================
-- LEARN: SHOW TBLPROPERTIES Confirms CDC Active
-- ============================================================================
-- The TBLPROPERTIES should now include delta.enableChangeDataFeed = true.
-- This is the persistent configuration stored in the transaction log.

ASSERT ROW_COUNT >= 1
SHOW TBLPROPERTIES {{zone_name}}.delta_demos.customer_events;


-- ============================================================================
-- ACTION: UPDATE with CDC Active — Generates Change Records
-- ============================================================================
-- Now that CDC is enabled, UPDATEs generate pre-image and post-image records
-- in the _change_data directory. Let's apply a 10% discount to web purchases.
-- CDC will track which rows changed and their before/after states.

ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.delta_demos.customer_events
SET revenue = ROUND(revenue * 0.90, 2)
WHERE event_type = 'purchase' AND channel = 'web';


-- Verify the web channel revenue dropped after the discount
ASSERT VALUE total_revenue = 585.0 WHERE channel = 'web'
ASSERT ROW_COUNT = 5
SELECT channel,
       COUNT(*) AS events,
       ROUND(SUM(revenue), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.customer_events
GROUP BY channel
ORDER BY total_revenue DESC;


-- ============================================================================
-- LEARN: DESCRIBE HISTORY Shows the Feature Activation
-- ============================================================================
-- DESCRIBE HISTORY records every committed transaction, including the
-- SET TBLPROPERTIES operation that activated CDC and the subsequent UPDATE.
-- This gives you a timeline of when features were enabled and what
-- operations followed.

ASSERT ROW_COUNT >= 4
DESCRIBE HISTORY {{zone_name}}.delta_demos.customer_events;


-- ============================================================================
-- EXPLORE: Customer Spend After All Changes
-- ============================================================================
-- After the web discount, let's see the per-customer revenue breakdown.

ASSERT ROW_COUNT = 8
SELECT customer_id,
       COUNT(*) AS events,
       ROUND(SUM(revenue), 2) AS total_spend
FROM {{zone_name}}.delta_demos.customer_events
GROUP BY customer_id
ORDER BY total_spend DESC;


-- ============================================================================
-- EXPLORE: Verify Data Integrity Through Feature Changes
-- ============================================================================
-- Feature activation should never corrupt existing data. The total row count
-- and non-web revenues should be unchanged. Only web purchase revenues
-- should differ (reduced by 10%).

ASSERT VALUE total_events = 25
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_events FROM {{zone_name}}.delta_demos.customer_events;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify row count unchanged (features don't add/remove rows)
ASSERT VALUE event_count = 25
SELECT COUNT(*) AS event_count FROM {{zone_name}}.delta_demos.customer_events;

-- Verify 8 distinct customers
ASSERT VALUE customer_count = 8
SELECT COUNT(DISTINCT customer_id) AS customer_count FROM {{zone_name}}.delta_demos.customer_events;

-- Verify 3 event types
ASSERT VALUE type_count = 3
SELECT COUNT(DISTINCT event_type) AS type_count FROM {{zone_name}}.delta_demos.customer_events;

-- Verify total revenue after web discount
ASSERT VALUE total_revenue = 3646.5
SELECT ROUND(SUM(revenue), 2) AS total_revenue FROM {{zone_name}}.delta_demos.customer_events;

-- Verify store revenue unchanged (not affected by web discount)
ASSERT VALUE store_revenue = 925.0
SELECT ROUND(SUM(revenue), 2) AS store_revenue FROM {{zone_name}}.delta_demos.customer_events WHERE channel = 'store';

-- Verify purchase count unchanged
ASSERT VALUE purchase_count = 17
SELECT COUNT(*) AS purchase_count FROM {{zone_name}}.delta_demos.customer_events WHERE event_type = 'purchase';

-- Verify signup count unchanged (no revenue impact)
ASSERT VALUE signup_count = 4
SELECT COUNT(*) AS signup_count FROM {{zone_name}}.delta_demos.customer_events WHERE event_type = 'signup';
