-- ============================================================================
-- Delta MERGE — Computed Columns & CASE Logic — Educational Queries
-- ============================================================================
-- WHAT: CASE expressions, arithmetic, and conditional logic inside MERGE
--       UPDATE SET and INSERT VALUES to compute derived columns at merge time.
-- WHY:  Embedding business rules directly in the MERGE ensures every row —
--       whether updated or inserted — gets the same tier/discount/score
--       logic applied consistently, without a separate transformation step.
-- HOW:  The UPDATE SET and INSERT VALUES clauses both contain identical
--       CASE expressions that derive tier, discount_pct, and priority_score
--       from the raw monthly_amount and months_active values.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Subscriptions Before MERGE
-- ============================================================================
-- 12 existing subscriptions with pre-computed tier, discount, and score.
-- The source has 7 renewals (plan upgrades) and 3 new customers.

ASSERT ROW_COUNT = 12
SELECT id, customer_name, plan, monthly_amount, months_active,
       tier, discount_pct, priority_score
FROM {{zone_name}}.delta_demos.subscriptions
ORDER BY id;

ASSERT ROW_COUNT = 10
SELECT id, customer_name, plan, monthly_amount, months_active
FROM {{zone_name}}.delta_demos.subscription_changes
ORDER BY id;


-- ============================================================================
-- MERGE: Upsert with Computed Columns
-- ============================================================================
-- Business rules applied at merge time:
--
--   tier (CASE on monthly_amount):
--       >= 500  → platinum
--       >= 100  → gold
--       >= 50   → silver
--       else    → bronze
--
--   discount_pct (CASE on months_active):
--       >= 24   → 15.0%
--       >= 12   → 10.0%
--       >= 6    → 5.0%
--       else    → 0.0%
--
--   priority_score (arithmetic):
--       monthly_amount * (1 + months_active / 10.0)
--
-- These same rules appear in both UPDATE SET and INSERT VALUES.
-- rows_affected: 7 updates + 3 inserts = 10

ASSERT ROW_COUNT = 10
MERGE INTO {{zone_name}}.delta_demos.subscriptions AS target
USING {{zone_name}}.delta_demos.subscription_changes AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        plan           = source.plan,
        monthly_amount = source.monthly_amount,
        months_active  = source.months_active,
        tier = CASE
            WHEN source.monthly_amount >= 500 THEN 'platinum'
            WHEN source.monthly_amount >= 100 THEN 'gold'
            WHEN source.monthly_amount >= 50  THEN 'silver'
            ELSE 'bronze'
        END,
        discount_pct = CASE
            WHEN source.months_active >= 24 THEN 15.0
            WHEN source.months_active >= 12 THEN 10.0
            WHEN source.months_active >= 6  THEN 5.0
            ELSE 0.0
        END,
        priority_score = source.monthly_amount * (1 + source.months_active / 10.0)
WHEN NOT MATCHED THEN
    INSERT (id, customer_name, plan, monthly_amount, months_active,
            tier, discount_pct, priority_score)
    VALUES (source.id, source.customer_name, source.plan,
            source.monthly_amount, source.months_active,
            CASE
                WHEN source.monthly_amount >= 500 THEN 'platinum'
                WHEN source.monthly_amount >= 100 THEN 'gold'
                WHEN source.monthly_amount >= 50  THEN 'silver'
                ELSE 'bronze'
            END,
            CASE
                WHEN source.months_active >= 24 THEN 15.0
                WHEN source.months_active >= 12 THEN 10.0
                WHEN source.months_active >= 6  THEN 5.0
                ELSE 0.0
            END,
            source.monthly_amount * (1 + source.months_active / 10.0));


-- ============================================================================
-- EXPLORE: All Subscriptions After MERGE
-- ============================================================================
-- All 12 original + 3 new = 15 subscriptions, each with freshly
-- computed tier, discount_pct, and priority_score.

ASSERT ROW_COUNT = 15
SELECT id, customer_name, plan, monthly_amount, months_active,
       tier, discount_pct, priority_score
FROM {{zone_name}}.delta_demos.subscriptions
ORDER BY id;


-- ============================================================================
-- LEARN: Tier Computation via CASE
-- ============================================================================
-- The tier column is derived purely from monthly_amount:
--   Acme Corp upgraded to enterprise ($500) → platinum
--   Bolt Industries upgraded to professional ($89) → silver
--   Echo Systems upgraded to business ($150) → gold
--   InnoTech upgraded to business ($150) → gold

ASSERT ROW_COUNT = 4
ASSERT VALUE tier = 'platinum' WHERE id = 1
ASSERT VALUE tier = 'silver' WHERE id = 2
ASSERT VALUE tier = 'gold' WHERE id = 5
ASSERT VALUE tier = 'gold' WHERE id = 9
SELECT id, customer_name, plan, monthly_amount, tier
FROM {{zone_name}}.delta_demos.subscriptions
WHERE id IN (1, 2, 5, 9)
ORDER BY monthly_amount DESC;


-- ============================================================================
-- LEARN: Discount Brackets via CASE
-- ============================================================================
-- The discount_pct is based on months_active loyalty tiers:
--   Cascade Labs (37 months) → 15%
--   Acme Corp (25 months)    → 15%
--   InnoTech (15 months)     → 10%
--   Echo Systems (9 months)  → 5%
--   Bolt Industries (4 months) → 0%

ASSERT ROW_COUNT = 5
ASSERT VALUE discount_pct = 15.0 WHERE id = 3
ASSERT VALUE discount_pct = 15.0 WHERE id = 1
ASSERT VALUE discount_pct = 10.0 WHERE id = 9
ASSERT VALUE discount_pct = 5.0 WHERE id = 5
ASSERT VALUE discount_pct = 0.0 WHERE id = 2
SELECT id, customer_name, months_active, discount_pct
FROM {{zone_name}}.delta_demos.subscriptions
WHERE id IN (1, 2, 3, 5, 9)
ORDER BY months_active DESC;


-- ============================================================================
-- LEARN: Arithmetic Priority Score
-- ============================================================================
-- priority_score = monthly_amount * (1 + months_active / 10.0)
-- This rewards both high spend AND long tenure:
--   Cascade Labs: 500 * (1 + 37/10) = 500 * 4.7 = 2350.0
--   Acme Corp:    500 * (1 + 25/10) = 500 * 3.5 = 1750.0
--   DataFlow Inc: 500 * (1 + 12/10) = 500 * 2.2 = 1100.0

ASSERT ROW_COUNT = 3
ASSERT VALUE priority_score = 2350.0 WHERE id = 3
ASSERT VALUE priority_score = 1750.0 WHERE id = 1
ASSERT VALUE priority_score = 1100.0 WHERE id = 4
SELECT id, customer_name, monthly_amount, months_active, priority_score
FROM {{zone_name}}.delta_demos.subscriptions
WHERE id IN (1, 3, 4)
ORDER BY priority_score DESC;


-- ============================================================================
-- EXPLORE: New Subscriptions with Computed Columns
-- ============================================================================
-- The 3 new subscriptions (ids 13-15) were inserted with all derived
-- columns computed at INSERT time via the same CASE/arithmetic rules:
--   NovaStar (starter $29, 1 month) → bronze, 0%, score=31.9
--   OmniFlow (business $150, 1 month) → gold, 0%, score=165.0
--   PrismData (enterprise $500, 1 month) → platinum, 0%, score=550.0

ASSERT ROW_COUNT = 3
ASSERT VALUE tier = 'bronze' WHERE id = 13
ASSERT VALUE tier = 'gold' WHERE id = 14
ASSERT VALUE tier = 'platinum' WHERE id = 15
ASSERT VALUE priority_score = 550.0 WHERE id = 15
SELECT id, customer_name, plan, monthly_amount,
       tier, discount_pct, priority_score
FROM {{zone_name}}.delta_demos.subscriptions
WHERE id BETWEEN 13 AND 15
ORDER BY id;


-- ============================================================================
-- EXPLORE: Tier Distribution After MERGE
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE sub_count = 5 WHERE tier = 'platinum'
ASSERT VALUE sub_count = 4 WHERE tier = 'gold'
SELECT tier,
       COUNT(*) AS sub_count,
       ROUND(SUM(monthly_amount), 2) AS total_revenue,
       ROUND(AVG(priority_score), 2) AS avg_priority
FROM {{zone_name}}.delta_demos.subscriptions
GROUP BY tier
ORDER BY total_revenue DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_count: 12 + 3 new = 15
ASSERT ROW_COUNT = 15
SELECT * FROM {{zone_name}}.delta_demos.subscriptions;

-- Verify acme_tier: Acme upgraded to platinum
ASSERT VALUE tier = 'platinum' WHERE id = 1
SELECT id, tier FROM {{zone_name}}.delta_demos.subscriptions WHERE id IN (1, 2, 3, 13);

-- Verify acme_discount: Acme gets 15% loyalty discount (25 months)
ASSERT VALUE discount_pct = 15.0 WHERE id = 1
SELECT id, discount_pct FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 1;

-- Verify bolt_tier: Bolt upgraded to silver (professional $89)
ASSERT VALUE tier = 'silver' WHERE id = 2
SELECT id, tier FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 2;

-- Verify cascade_score: Cascade Labs priority = 500 * (1 + 37/10) = 2350
ASSERT VALUE priority_score = 2350.0
SELECT priority_score FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 3;

-- Verify novastar_tier: NovaStar inserted as bronze
ASSERT VALUE tier = 'bronze' WHERE id = 13
SELECT id, tier FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 13;

-- Verify platinum_count: 5 platinum subscriptions
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.subscriptions WHERE tier = 'platinum';

-- Verify unchanged_subs: Unmatched targets preserved (ids 6,8,10,11,12)
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.subscriptions WHERE id IN (6, 8, 10, 11, 12);
