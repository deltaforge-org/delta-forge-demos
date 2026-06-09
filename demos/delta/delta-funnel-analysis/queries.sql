-- ============================================================================
-- Delta Funnel Analysis — Educational Queries
-- ============================================================================
-- WHAT: A SaaS conversion funnel tracking users from trial through renewal.
--       Self-JOINs compute stage-by-stage conversion rates, and UPDATE marks
--       churned users — showing Delta Lake as a mutable analytics backend.
-- WHY:  Understanding where users drop off is the most important metric for
--       SaaS growth. Traditional analytics requires separate systems; with
--       Delta Lake, the funnel data itself is mutable and enrichable.
-- HOW:  Self-JOINs link each user's events across stages. UPDATE enriches
--       the data in place (marking churn), and the funnel query reruns on
--       the enriched dataset to show the updated picture.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Event type distribution — the raw funnel shape
-- ============================================================================
-- The funnel narrows at each stage: 40 trials → 30 activations → 20
-- subscriptions → 10 renewals. Each drop-off represents lost users.

ASSERT ROW_COUNT = 4
ASSERT VALUE events = 40 WHERE event_type = 'trial_start'
ASSERT VALUE events = 30 WHERE event_type = 'activation'
ASSERT VALUE events = 20 WHERE event_type = 'subscription'
SELECT event_type, COUNT(*) AS events
FROM {{zone_name}}.delta_demos.user_events
GROUP BY event_type
ORDER BY events DESC;


-- ============================================================================
-- EXPLORE: Acquisition channel breakdown — where users come from
-- ============================================================================
-- Organic search dominates trial volume, but volume alone does not tell you
-- which channel produces the best users. That requires funnel analysis below.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_events = 40 WHERE channel = 'organic'
SELECT channel, COUNT(*) AS total_events,
       COUNT(*) FILTER (WHERE event_type = 'trial_start') AS trials,
       COUNT(*) FILTER (WHERE event_type = 'subscription') AS subscriptions
FROM {{zone_name}}.delta_demos.user_events
GROUP BY channel
ORDER BY total_events DESC;


-- ============================================================================
-- LEARN: Funnel conversion rates — self-JOIN across stages
-- ============================================================================
-- This is the core funnel query. For each user, we join their trial_start
-- event to their activation, subscription, and renewal events. Users who
-- never reached a stage produce NULLs, which COUNT ignores — giving us
-- the exact conversion count at each stage.

ASSERT VALUE trial_users = 40
ASSERT VALUE activated_users = 30
ASSERT VALUE subscribed_users = 20
ASSERT VALUE renewed_users = 10
ASSERT ROW_COUNT = 1
SELECT COUNT(DISTINCT t.user_id) AS trial_users,
       COUNT(DISTINCT a.user_id) AS activated_users,
       COUNT(DISTINCT s.user_id) AS subscribed_users,
       COUNT(DISTINCT r.user_id) AS renewed_users
FROM {{zone_name}}.delta_demos.user_events t
LEFT JOIN {{zone_name}}.delta_demos.user_events a
    ON t.user_id = a.user_id AND a.event_type = 'activation'
LEFT JOIN {{zone_name}}.delta_demos.user_events s
    ON t.user_id = s.user_id AND s.event_type = 'subscription'
LEFT JOIN {{zone_name}}.delta_demos.user_events r
    ON t.user_id = r.user_id AND r.event_type = 'renewal'
WHERE t.event_type = 'trial_start';


-- ============================================================================
-- LEARN: Revenue by plan tier — where the money comes from
-- ============================================================================
-- Enterprise plans generate the most revenue despite having the fewest users.
-- This is the classic SaaS revenue distribution: a few high-value contracts
-- outweigh many low-value subscriptions.

ASSERT VALUE total_revenue = 796 WHERE plan_type = 'enterprise'
ASSERT ROW_COUNT = 3
SELECT plan_type,
       COUNT(*) FILTER (WHERE event_type = 'subscription') AS subscriptions,
       COUNT(*) FILTER (WHERE event_type = 'renewal') AS renewals,
       SUM(revenue) AS total_revenue
FROM {{zone_name}}.delta_demos.user_events
GROUP BY plan_type
ORDER BY total_revenue DESC;


-- ============================================================================
-- LEARN: Full journey users — trial through renewal (all 4 stages)
-- ============================================================================
-- Only users who completed every stage appear here. These are the most
-- valuable customers — they trialed, activated, paid, and renewed.

ASSERT ROW_COUNT = 10
ASSERT VALUE channel = 'organic' WHERE user_id = 'U001'
SELECT t.user_id, t.channel, t.plan_type,
       t.event_date AS trial_date,
       a.event_date AS activation_date,
       s.event_date AS subscription_date,
       r.event_date AS renewal_date
FROM {{zone_name}}.delta_demos.user_events t
JOIN {{zone_name}}.delta_demos.user_events a
    ON t.user_id = a.user_id AND a.event_type = 'activation'
JOIN {{zone_name}}.delta_demos.user_events s
    ON t.user_id = s.user_id AND s.event_type = 'subscription'
JOIN {{zone_name}}.delta_demos.user_events r
    ON t.user_id = r.user_id AND r.event_type = 'renewal'
WHERE t.event_type = 'trial_start'
ORDER BY t.user_id;


-- ============================================================================
-- ACTION: UPDATE — mark churned users (activated but never subscribed)
-- ============================================================================
-- Users U021–U030 activated their accounts but never subscribed. This UPDATE
-- relabels their activation events as 'churned' to enrich the funnel data.
-- This is a Delta-native pattern: instead of maintaining a separate churn
-- table, we mutate the event stream in place.

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.user_events
SET event_type = 'churned'
WHERE event_type = 'activation'
  AND user_id NOT IN (
      SELECT DISTINCT user_id
      FROM {{zone_name}}.delta_demos.user_events
      WHERE event_type = 'subscription'
  );


-- ============================================================================
-- EXPLORE: Post-update funnel — churned users now visible
-- ============================================================================
-- The funnel now has 5 stages: trial_start, activation, subscription, renewal,
-- and churned. The activation count dropped from 30 to 20 because 10 rows
-- were relabeled. The churned category makes drop-off explicit.

ASSERT ROW_COUNT = 5
ASSERT VALUE events = 10 WHERE event_type = 'churned'
ASSERT VALUE events = 20 WHERE event_type = 'activation'
SELECT event_type, COUNT(*) AS events
FROM {{zone_name}}.delta_demos.user_events
GROUP BY event_type
ORDER BY events DESC;


-- ============================================================================
-- LEARN: Channel effectiveness — which channels produce subscribers?
-- ============================================================================
-- Now that churn is marked, we can see true conversion rates per channel.
-- Sales has the highest subscription rate (3 of 6 trials = 50%) despite
-- low volume — enterprise sales motions convert well.

ASSERT VALUE subscriptions = 3 WHERE channel = 'sales'
ASSERT ROW_COUNT = 4
SELECT t.channel,
       COUNT(DISTINCT t.user_id) AS trials,
       COUNT(DISTINCT s.user_id) AS subscriptions
FROM {{zone_name}}.delta_demos.user_events t
LEFT JOIN {{zone_name}}.delta_demos.user_events s
    ON t.user_id = s.user_id AND s.event_type = 'subscription'
WHERE t.event_type = 'trial_start'
GROUP BY t.channel
ORDER BY subscriptions DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 100
ASSERT VALUE total_rows = 100
SELECT COUNT(*) AS total_rows FROM {{zone_name}}.delta_demos.user_events;

-- Verify 40 distinct users
ASSERT VALUE user_count = 40
SELECT COUNT(DISTINCT user_id) AS user_count FROM {{zone_name}}.delta_demos.user_events;

-- Verify total revenue is 2000
ASSERT VALUE total_revenue = 2000
SELECT SUM(revenue) AS total_revenue FROM {{zone_name}}.delta_demos.user_events;

-- Verify trial_start count unchanged
ASSERT VALUE trial_count = 40
SELECT COUNT(*) AS trial_count FROM {{zone_name}}.delta_demos.user_events WHERE event_type = 'trial_start';

-- Verify churned count
ASSERT VALUE churned_count = 10
SELECT COUNT(*) AS churned_count FROM {{zone_name}}.delta_demos.user_events WHERE event_type = 'churned';

-- Verify activation count after update
ASSERT VALUE activation_count = 20
SELECT COUNT(*) AS activation_count FROM {{zone_name}}.delta_demos.user_events WHERE event_type = 'activation';

-- Verify subscription count unchanged
ASSERT VALUE sub_count = 20
SELECT COUNT(*) AS sub_count FROM {{zone_name}}.delta_demos.user_events WHERE event_type = 'subscription';

-- Verify renewal count unchanged
ASSERT VALUE renewal_count = 10
SELECT COUNT(*) AS renewal_count FROM {{zone_name}}.delta_demos.user_events WHERE event_type = 'renewal';
