-- ============================================================================
-- Support Ticket Intelligence — Queries
-- ============================================================================
-- The workflow, top to bottom: register models -> enrich every ticket with
-- one SQL statement -> route the queue -> auto-prioritize -> extract
-- structured fields -> semantic search -> localize for a global team.
--
-- LLM output is non-deterministic, so validation is ASSERT ROW_COUNT > 0
-- (never exact values). Each scene RETURNS its result so it renders on
-- screen: the enriched columns and the analytics over them are the payoff.
-- Every AI_* call is a real, metered HTTPS request per row.
-- ============================================================================


-- ============================================================================
-- Scene 1: The models we registered
-- ============================================================================
-- One chat model and one embeddings model, callable by name. Secrets never
-- appear here: DESCRIBE shows config only.

SHOW MODELS;

DESCRIBE MODEL ticket_assistant;


-- ============================================================================
-- Scene 2: Bring the LLM to the data — enrich every ticket in one statement
-- ============================================================================
-- Raw ticket text in, structured columns out: queue, sentiment, urgency,
-- and a one-line summary, written straight into a gold Delta table. No
-- export, no external agent, no pipeline. This is the whole idea.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.support.ticket_enriched (
    ticket_id  INT,
    channel    VARCHAR,
    body       VARCHAR,
    category   VARCHAR,
    sentiment  VARCHAR,
    urgency    VARCHAR,
    summary    VARCHAR
) LOCATION 'support-ticket-intelligence/ticket_enriched';

INSERT INTO {{zone_name}}.support.ticket_enriched
    (ticket_id, channel, body, category, sentiment, urgency, summary)
SELECT
    ticket_id,
    channel,
    body,
    AI_CLASSIFY('ticket_assistant', body, 'billing', 'technical', 'feedback', 'account') AS category,
    AI_ANALYZE_SENTIMENT('ticket_assistant', body) AS sentiment,
    AI_GENERATE('ticket_assistant',
                'Classify the urgency of this support ticket as exactly one word (low, medium, or high): ' || body) AS urgency,
    AI_SUMMARIZE('ticket_assistant', body) AS summary
FROM {{zone_name}}.support.tickets;

-- The gold table: raw tickets, now with AI-derived columns.
ASSERT ROW_COUNT > 0
SELECT ticket_id, category, sentiment, urgency, summary
FROM {{zone_name}}.support.ticket_enriched
ORDER BY ticket_id;


-- ============================================================================
-- Scene 3: Route the queue — analytics over the AI-derived columns
-- ============================================================================
-- category and sentiment are now first-class SQL: GROUP BY them like any
-- other column to see load and unhappiness per queue.

ASSERT ROW_COUNT > 0
SELECT
    category,
    COUNT(*) AS tickets,
    SUM(CASE WHEN LOWER(sentiment) = 'negative' THEN 1 ELSE 0 END) AS unhappy
FROM {{zone_name}}.support.ticket_enriched
GROUP BY category
ORDER BY tickets DESC, category;


-- ============================================================================
-- Scene 4: Auto-prioritized hot list — negative and high urgency
-- ============================================================================
-- The tickets a lead should see first, with the AI summary so they never
-- open the raw thread.

ASSERT ROW_COUNT > 0
SELECT ticket_id, category, urgency, summary
FROM {{zone_name}}.support.ticket_enriched
WHERE LOWER(sentiment) = 'negative'
  AND LOWER(urgency) LIKE 'high%'
ORDER BY ticket_id;


-- ============================================================================
-- Scene 5: Extract structured fields from billing tickets
-- ============================================================================
-- Pull machine-usable JSON out of free text: refund amount and order id,
-- ready to feed a refund workflow.

ASSERT ROW_COUNT > 0
SELECT
    ticket_id,
    AI_EXTRACT('ticket_assistant', body, 'refund_amount', 'order_id') AS extracted
FROM {{zone_name}}.support.ticket_enriched
WHERE LOWER(category) = 'billing'
ORDER BY ticket_id;


-- ============================================================================
-- Scene 6: Semantic search — find the tickets that look like an outage
-- ============================================================================
-- Embed each ticket and the query phrase, then rank by cosine distance.
-- The production API-outage ticket surfaces at the top by MEANING, not
-- keywords. The same vectors feed CREATE INDEX ... USING HNSW at scale.

ASSERT ROW_COUNT > 0
SELECT
    ticket_id,
    body,
    cosine_distance(
        array_to_vector(AI_GENERATE_EMBEDDINGS('ticket_embedder', body)),
        array_to_vector(AI_GENERATE_EMBEDDINGS('ticket_embedder',
            'urgent production outage: the API is down and returning server errors'))
    ) AS distance
FROM {{zone_name}}.support.tickets
ORDER BY distance ASC
LIMIT 3;


-- ============================================================================
-- Scene 7: Localize for a global team — translate + clean up, USE MODEL sugar
-- ============================================================================
-- The messy phone-log ticket, translated to Norwegian and grammar-cleaned in
-- one query. AI_FIX_GRAMMAR uses the SQL-Server-style `USE MODEL` sugar.

ASSERT ROW_COUNT > 0
SELECT
    ticket_id,
    AI_TRANSLATE('ticket_assistant', body, 'Norwegian') AS body_norwegian,
    AI_FIX_GRAMMAR(body USE MODEL ticket_assistant) AS body_cleaned
FROM {{zone_name}}.support.tickets
WHERE ticket_id = 4;
