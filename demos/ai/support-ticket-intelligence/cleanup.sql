-- ============================================================================
-- Cleanup: Support Ticket Intelligence
-- ============================================================================
-- Reverse of creation: gold + bronze tables -> models -> connection ->
-- folder -> schema. Everything IF EXISTS so a half-completed run cleans up
-- without error. DROP MODEL removes only the registry entry; the provider
-- endpoint is untouched.
--
-- The OpenAIKey credential is intentionally NOT dropped: it is a shared
-- vault entry an admin may use across many models and demos. To remove the
-- vault secret from the OS Keychain, run `DROP CREDENTIAL IF EXISTS OpenAIKey;`
-- explicitly.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.support.ticket_enriched WITH FILES;

DROP DELTA TABLE IF EXISTS {{zone_name}}.support.tickets WITH FILES;

DROP MODEL IF EXISTS ticket_assistant;

DROP MODEL IF EXISTS ticket_embedder;

DROP CONNECTION IF EXISTS support_llm;

DROP FOLDER 'support-ticket-intelligence' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.support;
