-- ============================================================================
-- Delta JSON Event Log — Payment Gateway Analysis — Educational Queries
-- ============================================================================
-- WHAT: A fintech payment gateway stores heterogeneous JSON payloads (charges,
--       refunds, auths, payouts) in VARCHAR columns — each type has a different
--       payload shape but all share a single table.
-- WHY:  Payment systems produce events with varying structures. A rigid schema
--       would require dozens of nullable columns or separate tables per type.
-- HOW:  Queries use LIKE patterns for payload filtering, CASE WHEN for risk
--       classification, SUBSTRING for field extraction, and CTEs for multi-step
--       analytics across transaction types.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Different transaction types with different payload shapes
-- ============================================================================
-- Each txn_type has a fundamentally different payload structure:
--   charge:  {"amount", "currency", "merchant", "card_last4", "status"}
--   refund:  {"amount", "currency", "original_txn", "reason"}
--   auth:    {"amount", "currency", "merchant", "hold_expires"}
--   payout:  {"amount", "currency", "merchant", "account_type"}
-- All stored in the SAME 'payload' VARCHAR column.

ASSERT ROW_COUNT = 4
SELECT id, txn_type, source, payload
FROM {{zone_name}}.delta_demos.payment_events
WHERE id IN (1, 16, 26, 34)
ORDER BY id;


-- ============================================================================
-- LEARN: Filtering JSON payloads with LIKE patterns
-- ============================================================================
-- LIKE pattern matching lets you query inside JSON strings without a JSON
-- parser. This finds all charges where the payment processor accepted the card.
-- The pattern '%"status":"succeeded"%' matches the JSON field reliably because
-- the key-value pair is always formatted consistently by the gateway.

ASSERT ROW_COUNT = 12
SELECT id, txn_type, payload, status
FROM {{zone_name}}.delta_demos.payment_events
WHERE txn_type = 'charge' AND payload LIKE '%"status":"succeeded"%'
ORDER BY id;


-- ============================================================================
-- LEARN: Transaction type and source distribution
-- ============================================================================
-- The payment gateway receives events from multiple channels (web, mobile, POS,
-- internal). GROUP BY two dimensions reveals traffic patterns — e.g., refunds
-- are rare from POS but common from web (easier to request online).

ASSERT ROW_COUNT = 10
SELECT source, txn_type, COUNT(*) AS events
FROM {{zone_name}}.delta_demos.payment_events
GROUP BY source, txn_type
ORDER BY source, txn_type;


-- ============================================================================
-- LEARN: Currency distribution using LIKE extraction
-- ============================================================================
-- Every payload contains a "currency" field regardless of transaction type.
-- This is a field that spans all payload shapes — a natural dimension for
-- cross-type analytics. LIKE filters isolate transactions by currency.

ASSERT ROW_COUNT = 3
ASSERT VALUE event_count = 30 WHERE currency_match = 'USD'
ASSERT VALUE event_count = 5 WHERE currency_match = 'EUR'
ASSERT VALUE event_count = 5 WHERE currency_match = 'GBP'
SELECT
    CASE
        WHEN payload LIKE '%"currency":"USD"%' THEN 'USD'
        WHEN payload LIKE '%"currency":"EUR"%' THEN 'EUR'
        WHEN payload LIKE '%"currency":"GBP"%' THEN 'GBP'
    END AS currency_match,
    COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.payment_events
GROUP BY
    CASE
        WHEN payload LIKE '%"currency":"USD"%' THEN 'USD'
        WHEN payload LIKE '%"currency":"EUR"%' THEN 'EUR'
        WHEN payload LIKE '%"currency":"GBP"%' THEN 'GBP'
    END
ORDER BY currency_match;


-- ============================================================================
-- LEARN: Refund reason analysis from JSON payloads
-- ============================================================================
-- Refund payloads contain a "reason" field that charge payloads do not have.
-- This is the variant pattern in action — different event types carry different
-- fields. Fraud analysts use reason breakdowns to detect abuse patterns.

ASSERT ROW_COUNT = 8
SELECT
    CASE
        WHEN payload LIKE '%"reason":"defective"%' THEN 'defective'
        WHEN payload LIKE '%"reason":"duplicate"%' THEN 'duplicate'
        WHEN payload LIKE '%"reason":"not_received"%' THEN 'not_received'
        WHEN payload LIKE '%"reason":"partial"%' THEN 'partial'
        WHEN payload LIKE '%"reason":"returned"%' THEN 'returned'
        WHEN payload LIKE '%"reason":"unauthorized"%' THEN 'unauthorized'
        WHEN payload LIKE '%"reason":"wrong_item"%' THEN 'wrong_item'
        WHEN payload LIKE '%"reason":"wrong_size"%' THEN 'wrong_size'
    END AS refund_reason,
    COUNT(*) AS refund_count
FROM {{zone_name}}.delta_demos.payment_events
WHERE txn_type = 'refund'
GROUP BY
    CASE
        WHEN payload LIKE '%"reason":"defective"%' THEN 'defective'
        WHEN payload LIKE '%"reason":"duplicate"%' THEN 'duplicate'
        WHEN payload LIKE '%"reason":"not_received"%' THEN 'not_received'
        WHEN payload LIKE '%"reason":"partial"%' THEN 'partial'
        WHEN payload LIKE '%"reason":"returned"%' THEN 'returned'
        WHEN payload LIKE '%"reason":"unauthorized"%' THEN 'unauthorized'
        WHEN payload LIKE '%"reason":"wrong_item"%' THEN 'wrong_item'
        WHEN payload LIKE '%"reason":"wrong_size"%' THEN 'wrong_size'
    END
ORDER BY refund_reason;


-- ============================================================================
-- LEARN: Risk classification using CASE WHEN on JSON patterns
-- ============================================================================
-- Fraud detection teams classify charges into risk tiers based on the payment
-- processor's response embedded in the payload. CASE WHEN with LIKE patterns
-- acts as a rules engine: "flagged" = high risk, "declined" = medium (card
-- issuer rejected), "succeeded" = low risk (normal transaction).

ASSERT ROW_COUNT = 3
ASSERT VALUE charge_count = 1 WHERE risk_tier = 'high'
ASSERT VALUE charge_count = 2 WHERE risk_tier = 'medium'
ASSERT VALUE charge_count = 12 WHERE risk_tier = 'low'
SELECT
    CASE
        WHEN payload LIKE '%"status":"flagged"%' THEN 'high'
        WHEN payload LIKE '%"status":"declined"%' THEN 'medium'
        ELSE 'low'
    END AS risk_tier,
    COUNT(*) AS charge_count
FROM {{zone_name}}.delta_demos.payment_events
WHERE txn_type = 'charge'
GROUP BY
    CASE
        WHEN payload LIKE '%"status":"flagged"%' THEN 'high'
        WHEN payload LIKE '%"status":"declined"%' THEN 'medium'
        ELSE 'low'
    END
ORDER BY risk_tier;


-- ============================================================================
-- LEARN: UPDATE based on JSON payload content — fraud escalation
-- ============================================================================
-- When the fraud team confirms a flagged charge, they escalate it by updating
-- the status column. Delta's copy-on-write creates a new Parquet file version
-- with the updated row, preserving the original in the transaction log.

UPDATE {{zone_name}}.delta_demos.payment_events
SET status = 'blocked'
WHERE txn_type = 'charge' AND payload LIKE '%"status":"flagged"%';

-- Verify the blocked transaction
ASSERT VALUE blocked_count = 1
SELECT COUNT(*) AS blocked_count
FROM {{zone_name}}.delta_demos.payment_events
WHERE status = 'blocked';

-- Verify overall status distribution: 6 distinct statuses
ASSERT ROW_COUNT = 6
SELECT status, COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.payment_events
GROUP BY status
ORDER BY status;


-- ============================================================================
-- LEARN: CTE — charge vs refund ratio by channel
-- ============================================================================
-- A CTE (Common Table Expression) lets you build multi-step analytical queries.
-- Here we separately count charges and refunds per source channel, then join
-- them to compute the net transaction count. This reveals which channels have
-- the highest refund-to-charge ratio — a key fraud and UX health metric.

ASSERT ROW_COUNT = 3
WITH charges AS (
    SELECT source, COUNT(*) AS charge_count
    FROM {{zone_name}}.delta_demos.payment_events
    WHERE txn_type = 'charge'
    GROUP BY source
),
refunds AS (
    SELECT source, COUNT(*) AS refund_count
    FROM {{zone_name}}.delta_demos.payment_events
    WHERE txn_type = 'refund'
    GROUP BY source
)
SELECT c.source,
       c.charge_count,
       r.refund_count,
       c.charge_count - r.refund_count AS net_charges
FROM charges c
JOIN refunds r ON c.source = r.source
ORDER BY c.source;


-- ============================================================================
-- LEARN: Auth hold lifecycle — active vs captured vs voided
-- ============================================================================
-- Pre-authorization holds have a lifecycle: active (hold placed), captured
-- (charge finalized), or voided (hold released). Merchants care about the
-- ratio — too many voided auths suggest checkout abandonment.

ASSERT ROW_COUNT = 3
ASSERT VALUE auth_count = 4 WHERE status = 'active'
ASSERT VALUE auth_count = 3 WHERE status = 'captured'
ASSERT VALUE auth_count = 1 WHERE status = 'voided'
SELECT status, COUNT(*) AS auth_count
FROM {{zone_name}}.delta_demos.payment_events
WHERE txn_type = 'auth'
GROUP BY status
ORDER BY status;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 40
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.payment_events;

-- Verify 15 charge events
ASSERT VALUE charge_count = 15
SELECT COUNT(*) AS charge_count FROM {{zone_name}}.delta_demos.payment_events WHERE txn_type = 'charge';

-- Verify 10 refund events
ASSERT VALUE refund_count = 10
SELECT COUNT(*) AS refund_count FROM {{zone_name}}.delta_demos.payment_events WHERE txn_type = 'refund';

-- Verify 8 auth events
ASSERT VALUE auth_count = 8
SELECT COUNT(*) AS auth_count FROM {{zone_name}}.delta_demos.payment_events WHERE txn_type = 'auth';

-- Verify 7 payout events
ASSERT VALUE payout_count = 7
SELECT COUNT(*) AS payout_count FROM {{zone_name}}.delta_demos.payment_events WHERE txn_type = 'payout';

-- Verify 4 distinct sources
ASSERT VALUE distinct_sources = 4
SELECT COUNT(DISTINCT source) AS distinct_sources FROM {{zone_name}}.delta_demos.payment_events;

-- Verify 30 USD-denominated events via payload LIKE
ASSERT VALUE usd_count = 30
SELECT COUNT(*) AS usd_count FROM {{zone_name}}.delta_demos.payment_events WHERE payload LIKE '%"currency":"USD"%';
