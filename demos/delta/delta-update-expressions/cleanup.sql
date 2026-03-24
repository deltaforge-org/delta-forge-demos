-- ============================================================================
-- Delta UPDATE Expressions — Portfolio Rebalancing — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.update_demos.portfolio_holdings WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.update_demos;
DROP ZONE IF EXISTS {{zone_name}};
