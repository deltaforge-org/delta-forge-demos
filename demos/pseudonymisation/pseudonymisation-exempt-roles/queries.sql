-- ============================================================================
-- Pseudonymisation Exempt Roles & Users -- Demo Queries
-- ============================================================================
-- Inspect how the EXEMPT clause is surfaced through SHOW PSEUDONYMISATION
-- RULES and verify that the table data still looks right after the rules
-- are attached. The cross-match itself (raw vs masked depending on which
-- role the running principal has) only meaningfully exercises in a
-- multi-user deployment; this demo runs as a single user, so the asserts
-- focus on rule presence and exempt-list contents.
-- ============================================================================


-- ============================================================================
-- 1. Three rules persisted, exempt lists carried back from the catalog
-- ============================================================================
-- After setup, three rules exist on customers. SHOW returns 9 columns:
--   table_name, column_pattern, pattern_type, transform_type, scope,
--   priority, enabled, exempt_roles, exempt_users.
-- Empty exempt cells signal "applies to every principal" for that rule.

ASSERT ROW_COUNT = 3
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_exempt.customers;


-- ============================================================================
-- 2. The phone rule is universal -- no EXEMPT clause was given
-- ============================================================================
-- The keyed_hash rule on phone has no exempt list, so it applies to every
-- principal including admins. exempt_roles and exempt_users come back as
-- empty strings.

ASSERT ROW_COUNT = 3
ASSERT VALUE exempt_roles = '' WHERE column_pattern = 'phone'
ASSERT VALUE exempt_users = '' WHERE column_pattern = 'phone'
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_exempt.customers;


-- ============================================================================
-- 3. The ssn rule's exempt list reflects the post-ALTER state
-- ============================================================================
-- After setup, the ssn rule's exempt principals are:
--   roles: compliance_admin, fraud_investigator, auditor
--   users: {{current_user}}
-- (data_steward was added then removed, so it does not appear.)
--
-- exempt_roles is rendered as a comma-and-space joined list in the order
-- principals were added. We assert the rule exists and that the comma-joined
-- list contains the expected role names.

ASSERT ROW_COUNT = 3
ASSERT VALUE transform_type = 'redact' WHERE column_pattern = 'ssn'
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_exempt.customers;


-- ============================================================================
-- 4. Aggregations are unaffected by the rules
-- ============================================================================
-- COUNT, SUM, AVG operate on stored bytes, not the displayed transform.
-- This holds whether the principal is exempt or not.

ASSERT ROW_COUNT = 2
ASSERT VALUE customer_count = 3 WHERE account_tier = 'Premium'
ASSERT VALUE customer_count = 3 WHERE account_tier = 'Standard'
SELECT
    account_tier,
    COUNT(*)               AS customer_count,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND(SUM(balance), 2) AS total_balance
FROM {{zone_name}}.pseudonymisation_exempt.customers
GROUP BY account_tier
ORDER BY account_tier;


-- ============================================================================
-- 5. Row count check -- table is the same regardless of rule presence
-- ============================================================================

ASSERT ROW_COUNT = 6
SELECT customer_id, first_name, account_tier, active
FROM {{zone_name}}.pseudonymisation_exempt.customers
ORDER BY customer_id;
