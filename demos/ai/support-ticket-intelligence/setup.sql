-- ============================================================================
-- Demo: Support Ticket Intelligence — bring the LLM to your data
-- Feature: triage, prioritize, and search a support queue with an LLM,
--          entirely in SQL. No export, no external agent, no pipeline glue.
-- ============================================================================
--
-- The story: a support team lands raw tickets in a Delta table (bronze).
-- One SQL statement enriches every row with an LLM (category, sentiment,
-- urgency, one-line summary) into a gold table. Those AI-derived columns
-- are then first-class SQL: the team routes the queue, auto-prioritizes a
-- hot list, extracts structured fields, and does semantic search over
-- ticket embeddings, all in plain SELECTs.
--
-- The provider API key lives in the vault (OS Keychain) as the OpenAIKey
-- credential and is referenced by a CONNECTION; the model object holds only
-- non-secret config, and the token is resolved server-side at call time,
-- never on the model row or in any result.
--
-- Provider: OpenAI-shaped (bearer auth). To retarget, change the CONNECTION
-- options and the CREATE MODEL API_FORMAT:
--   Anthropic  : auth_mode='api_key_header', auth_header_name='x-api-key',
--                API_FORMAT='anthropic' (chat only; keep an OpenAI/Ollama
--                embeddings model for the semantic-search scene).
--   Azure OAI  : auth_mode='api_key_header', auth_header_name='api-key',
--                API_FORMAT='azure_openai', plus api_version + deployment.
--   Ollama     : drop the CONNECTION; give each CREATE MODEL an
--                endpoint='http://localhost:11434/...' option, API_FORMAT='ollama'.
--
-- {{api_key}} is only consumed the first time, when the OpenAIKey vault
-- credential does not yet exist. If an admin already created OpenAIKey,
-- leave it unset; the existing key is reused and no token passes through
-- this script.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Provider API token in the vault (OS Keychain), referenced by name
-- --------------------------------------------------------------------------

CREATE CREDENTIAL IF NOT EXISTS OpenAIKey
    TYPE = CREDENTIAL
    SECRET '{{api_key}}'
    DESCRIPTION 'OpenAI API key for the support-ticket intelligence demo';

-- --------------------------------------------------------------------------
-- 2. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables — support ticket intelligence';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.support
    COMMENT 'Support queue: raw tickets and LLM-enriched analytics';

-- --------------------------------------------------------------------------
-- 3. Connection to the provider (base URL + bearer auth, vault credential)
-- --------------------------------------------------------------------------
-- Same connection + credential + HTTP transport the REST ingest path uses.

CREATE CONNECTION IF NOT EXISTS support_llm
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://api.openai.com',
        auth_mode    = 'bearer',
        storage_zone = '{{zone_name}}',
        timeout_secs = '60'
    )
    CREDENTIAL = OpenAIKey;

-- --------------------------------------------------------------------------
-- 4. Register the models: one chat, one embeddings
-- --------------------------------------------------------------------------
-- The registry name (ticket_assistant / ticket_embedder) is what SQL
-- references; MODEL is the provider model id. SYSTEM_PROMPT is a governed
-- model-level instruction injected into every chat call.

CREATE MODEL IF NOT EXISTS ticket_assistant
    WITH (
        API_FORMAT    = 'openai',
        MODEL         = 'gpt-4o-mini',
        MODEL_TYPE    = CHAT,
        CONNECTION    = support_llm,
        SYSTEM_PROMPT = 'You are a support-operations assistant. Be terse and never add preamble.',
        PARAMETERS    = '{"temperature":0,"max_tokens":80}'
    );

CREATE MODEL IF NOT EXISTS ticket_embedder
    WITH (
        API_FORMAT = 'openai',
        MODEL      = 'text-embedding-3-small',
        MODEL_TYPE = EMBEDDINGS,
        CONNECTION = support_llm,
        PARAMETERS = '{"dimensions":256}'
    );

-- --------------------------------------------------------------------------
-- 5. Bronze: raw inbound tickets (small on purpose)
-- --------------------------------------------------------------------------
-- A deliberate spread across queues, sentiment, and urgency so the gold
-- aggregations tell a story. Ticket 2 (production API outage) is the clear
-- escalation the semantic-search scene surfaces.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.support.tickets (
    ticket_id  INT,
    channel    VARCHAR,
    body       VARCHAR
) LOCATION 'support-ticket-intelligence/tickets';

INSERT INTO {{zone_name}}.support.tickets (ticket_id, channel, body) VALUES
    (1, 'email', 'I was charged twice for my subscription this month and I need the duplicate charge of 49 USD refunded to order ORD-5821 as soon as possible.'),
    (2, 'chat',  'URGENT: your API has been returning 500 errors in production for the last hour and our checkout is completely down. We are losing sales right now.'),
    (3, 'email', 'Just wanted to say the new analytics dashboard is fantastic. The load times are so much faster than before, great work by the team.'),
    (4, 'phone', 'i am lockd out of my acount after i lost my mfa device and the recovery codes you gave me dont work eather'),
    (5, 'email', 'Please cancel my subscription at the end of the current billing period and confirm that there will be no further charges.'),
    (6, 'chat',  'The CSV export button on the reports page does nothing when I click it. I tried Chrome and Firefox with no luck.');

-- --------------------------------------------------------------------------
-- 6. ALTER MODEL: pin the chat model to deterministic output for triage
-- --------------------------------------------------------------------------

ALTER MODEL ticket_assistant
    SET PARAMETERS = '{"temperature":0,"max_tokens":80}';
