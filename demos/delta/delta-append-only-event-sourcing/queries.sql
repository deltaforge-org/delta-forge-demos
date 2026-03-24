-- ============================================================================
-- Delta Append-Only Event Sourcing — Educational Queries
-- ============================================================================
-- WHAT: Tables with delta.appendOnly = 'true' reject any commit that contains
--       "remove" file actions — meaning UPDATE and DELETE are forbidden.
-- WHY:  Immutable logs are the foundation of event sourcing: you derive current
--       state by replaying events, and the guarantee that no event can be altered
--       or deleted makes the history trustworthy for auditing and compliance.
-- HOW:  We query 60 events across 14 orders, reconstruct lifecycles, measure
--       processing times with window functions, derive order state from history,
--       then append a new batch and prove the count only grows.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Browse the Event Log
-- ============================================================================
-- The order_events table was created with TBLPROPERTIES ('delta.appendOnly' = 'true').
-- It contains 60 events across 14 orders, with 7 distinct event types tracing
-- orders from creation through delivery (or cancellation).

ASSERT ROW_COUNT = 10
SELECT id, order_id, event_type, amount, actor, created_at
FROM {{zone_name}}.delta_demos.order_events
ORDER BY id
LIMIT 10;


-- ============================================================================
-- EXPLORE: Event Type Distribution
-- ============================================================================
-- The event stream captures 7 event types. Because the log is append-only, these
-- counts can only grow over time — no events can be removed or reclassified.

ASSERT ROW_COUNT = 7
ASSERT VALUE event_count = 14 WHERE event_type = 'order.created'
ASSERT VALUE event_count = 13 WHERE event_type = 'order.confirmed'
ASSERT VALUE event_count = 12 WHERE event_type = 'payment.received'
ASSERT VALUE event_count = 9 WHERE event_type = 'order.shipped'
ASSERT VALUE event_count = 7 WHERE event_type = 'order.delivered'
SELECT event_type,
       COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.order_events
GROUP BY event_type
ORDER BY event_count DESC;


-- ============================================================================
-- LEARN: Reconstruct an Order Lifecycle
-- ============================================================================
-- Event sourcing's core pattern: trace the full history of a single entity.
-- Order 1001 went through all 5 stages from creation to delivery. Because
-- the log is immutable, this history is guaranteed complete and unaltered.

ASSERT ROW_COUNT = 5
ASSERT VALUE event_type = 'order.created' WHERE id = 1
ASSERT VALUE event_type = 'order.delivered' WHERE id = 5
ASSERT VALUE actor = 'bob' WHERE id = 4
SELECT id, event_type, actor, created_at
FROM {{zone_name}}.delta_demos.order_events
WHERE order_id = 1001
ORDER BY created_at;


-- ============================================================================
-- LEARN: Window Functions — Measure Processing Time Between Events
-- ============================================================================
-- LAG() over the immutable event stream lets us calculate how long each stage
-- took. For order 1001: confirmation was instant (5 min), payment took 7 min,
-- but shipping took over 2 hours and delivery took 5.5 hours.

ASSERT ROW_COUNT = 5
SELECT id,
       event_type,
       created_at,
       LAG(created_at) OVER (PARTITION BY order_id ORDER BY created_at) AS prev_event_at,
       LAG(event_type) OVER (PARTITION BY order_id ORDER BY created_at) AS prev_event
FROM {{zone_name}}.delta_demos.order_events
WHERE order_id = 1001
ORDER BY created_at;


-- ============================================================================
-- LEARN: Derive Current Order State from Event History
-- ============================================================================
-- In event sourcing, current state is computed — not stored. We find each
-- order's latest event to determine its status. This is the "fold" or
-- "reduce" pattern: replay events to produce a snapshot.

ASSERT ROW_COUNT = 14
ASSERT VALUE current_status = 'order.delivered' WHERE order_id = 1001
ASSERT VALUE current_status = 'payment.refunded' WHERE order_id = 1003
ASSERT VALUE current_status = 'order.cancelled' WHERE order_id = 1008
ASSERT VALUE current_status = 'order.confirmed' WHERE order_id = 1014
SELECT order_id, amount, current_status, latest_actor, latest_time
FROM (
    SELECT order_id,
           amount,
           event_type AS current_status,
           actor AS latest_actor,
           created_at AS latest_time,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY id DESC) AS rn
    FROM {{zone_name}}.delta_demos.order_events
) sub
WHERE rn = 1
ORDER BY order_id;


-- ============================================================================
-- EXPLORE: Revenue by Order Outcome
-- ============================================================================
-- Group orders by their final state to see revenue distribution. Delivered
-- orders represent earned revenue; refunded orders are returned; the rest
-- are in-progress.

ASSERT ROW_COUNT = 6
ASSERT VALUE order_count = 7 WHERE outcome = 'order.delivered'
ASSERT VALUE order_count = 2 WHERE outcome = 'payment.refunded'
SELECT current_status AS outcome,
       COUNT(*) AS order_count,
       ROUND(SUM(amount), 2) AS total_amount
FROM (
    SELECT order_id,
           amount,
           event_type AS current_status,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY id DESC) AS rn
    FROM {{zone_name}}.delta_demos.order_events
) sub
WHERE rn = 1
GROUP BY current_status
ORDER BY order_count DESC;


-- ============================================================================
-- LEARN: Actor Activity Analysis
-- ============================================================================
-- Who is doing what? 'system' handles confirmations, 'stripe' handles payments,
-- 'bob' handles all shipments. The immutable log provides a complete audit trail
-- for every actor's actions — useful for compliance and performance tracking.

ASSERT ROW_COUNT = 5
ASSERT VALUE event_count = 13 WHERE actor = 'system'
ASSERT VALUE event_count = 9 WHERE actor = 'bob'
SELECT actor,
       COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.order_events
GROUP BY actor
ORDER BY event_count DESC
LIMIT 5;


-- ============================================================================
-- STEP: Append a New Batch — Proving the Log Only Grows
-- ============================================================================
-- The append-only property means we can INSERT new events but never modify or
-- remove existing ones. Let's advance several orders: deliver 1005 and 1011,
-- ship 1007, and accept payment for 1014.

ASSERT ROW_COUNT = 5
INSERT INTO {{zone_name}}.delta_demos.order_events VALUES
    (61, 1005, 'order.delivered',   120.00, 'courier','2024-06-02 09:00:00'),
    (62, 1007, 'order.shipped',     55.00,  'bob',    '2024-06-02 10:00:00'),
    (63, 1014, 'payment.received',  125.00, 'stripe', '2024-06-02 10:30:00'),
    (64, 1011, 'order.delivered',   92.00,  'courier','2024-06-02 11:00:00'),
    (65, 1007, 'order.delivered',   55.00,  'courier','2024-06-02 15:00:00');


-- ============================================================================
-- LEARN: Verify the Log Grew — Immutability in Action
-- ============================================================================
-- After the append, total events grew from 60 to 65. The original 60 events
-- are unchanged. Delivered orders increased from 7 to 10. This is the append-
-- only guarantee: history only accumulates, never rewrites.

ASSERT VALUE total_events = 65
ASSERT VALUE delivered_count = 10
ASSERT VALUE distinct_orders = 14
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE event_type = 'order.delivered') AS delivered_count,
    COUNT(DISTINCT order_id) AS distinct_orders
FROM {{zone_name}}.delta_demos.order_events;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total event count after append
ASSERT ROW_COUNT = 65
SELECT * FROM {{zone_name}}.delta_demos.order_events;

-- Verify all 14 orders are present
ASSERT VALUE order_count = 14
SELECT COUNT(DISTINCT order_id) AS order_count FROM {{zone_name}}.delta_demos.order_events;

-- Verify event type counts after append
ASSERT VALUE created_count = 14
ASSERT VALUE delivered_count = 10
ASSERT VALUE shipped_count = 10
SELECT
    COUNT(*) FILTER (WHERE event_type = 'order.created') AS created_count,
    COUNT(*) FILTER (WHERE event_type = 'order.delivered') AS delivered_count,
    COUNT(*) FILTER (WHERE event_type = 'order.shipped') AS shipped_count
FROM {{zone_name}}.delta_demos.order_events;

-- Verify order 1001 is fully delivered (lifecycle complete)
ASSERT VALUE event_count = 5
SELECT COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.order_events
WHERE order_id = 1001;

-- Verify new deliveries (orders advanced by the batch append)
ASSERT VALUE current_status = 'order.delivered' WHERE order_id = 1005
ASSERT VALUE current_status = 'order.delivered' WHERE order_id = 1011
ASSERT VALUE current_status = 'order.delivered' WHERE order_id = 1007
ASSERT ROW_COUNT = 3
SELECT order_id, event_type AS current_status
FROM (
    SELECT order_id,
           event_type,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY id DESC) AS rn
    FROM {{zone_name}}.delta_demos.order_events
    WHERE order_id IN (1005, 1007, 1011)
) sub
WHERE rn = 1
ORDER BY order_id;

-- Verify cancelled orders remain unchanged (immutable history)
ASSERT VALUE event_count = 5
SELECT COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.order_events
WHERE order_id = 1003;
