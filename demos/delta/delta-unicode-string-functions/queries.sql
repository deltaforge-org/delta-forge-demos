-- ============================================================================
-- Delta Unicode String Functions — Educational Queries
-- ============================================================================
-- WHAT: SQL string functions (LENGTH, UPPER, LOWER, SUBSTR, LIKE, CONCAT)
--       must handle multi-byte UTF-8 characters correctly — operating on
--       logical characters, not raw bytes.
-- WHY:  PolyglotDesk receives support tickets in 15+ languages. Agents need
--       to search messages with LIKE, truncate names with SUBSTR for display,
--       normalize case with UPPER, and combine fields with CONCAT — all
--       without corrupting multi-byte characters.
-- HOW:  DataFusion's string functions operate on Unicode code points. LENGTH
--       returns character count, SUBSTR slices at character boundaries, and
--       UPPER/LOWER apply Unicode case mapping rules (ü → Ü, not U).
-- ============================================================================


-- ============================================================================
-- EXPLORE: All 20 support tickets — 15+ scripts in one table
-- ============================================================================
-- A single VARCHAR column stores names from Japanese, Greek, Cyrillic,
-- Arabic, Hebrew, Korean, Vietnamese, Turkish, and 8 European languages.

ASSERT ROW_COUNT = 20
SELECT id, customer_name, locale, subject, priority, status
FROM {{zone_name}}.delta_demos.support_tickets
ORDER BY id;


-- ============================================================================
-- LEARN: LENGTH returns character count, not byte count
-- ============================================================================
-- UTF-8 uses variable-width encoding: ASCII = 1 byte, Latin diacritics = 2,
-- Cyrillic/Greek/Arabic/Hebrew = 2, CJK/Korean = 3, emoji = 4.
-- LENGTH must return the NUMBER OF CHARACTERS, not bytes. A Korean name
-- '김서연' is 3 characters (9 bytes) — LENGTH must return 3.

ASSERT VALUE char_length = 11
SELECT LENGTH(customer_name) AS char_length FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 1;

ASSERT VALUE char_length = 16
SELECT LENGTH(customer_name) AS char_length FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 4;

ASSERT VALUE char_length = 13
SELECT LENGTH(customer_name) AS char_length FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 5;

ASSERT VALUE char_length = 10
SELECT LENGTH(customer_name) AS char_length FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 6;

-- Korean '김서연' = 3 characters, 9 bytes — LENGTH must return 3
ASSERT VALUE char_length = 3
SELECT LENGTH(customer_name) AS char_length FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 16;

-- Japanese message: 13 characters (all CJK, 39 bytes)
ASSERT VALUE msg_length = 13
SELECT LENGTH(message_snippet) AS msg_length FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 1;


-- ============================================================================
-- LEARN: UPPER preserves diacritics — ü → Ü, not U
-- ============================================================================
-- Unicode case mapping is more complex than ASCII. UPPER('ü') must produce
-- 'Ü' (U+00DC), not strip the umlaut. Similarly ñ → Ñ, é → É, ł → Ł.
-- Non-casing scripts (CJK, Arabic, Hebrew) pass through unchanged.

ASSERT VALUE upper_name = 'MÜLLER HANS'
SELECT UPPER(customer_name) AS upper_name FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 2;

ASSERT VALUE upper_name = 'FRANÇOIS DUBOIS'
SELECT UPPER(customer_name) AS upper_name FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 3;

ASSERT VALUE upper_name = 'JOSÉ GARCÍA'
SELECT UPPER(customer_name) AS upper_name FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 7;

ASSERT VALUE upper_name = 'ŁUKASZ KOWALSKI'
SELECT UPPER(customer_name) AS upper_name FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 14;

ASSERT VALUE upper_name = 'ŽOFIA NOVÁKOVÁ'
SELECT UPPER(customer_name) AS upper_name FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 17;


-- ============================================================================
-- LEARN: LIKE pattern matching with Unicode characters
-- ============================================================================
-- LIKE must correctly match multi-byte patterns. '%質問%' must scan the
-- UTF-8 byte sequence for the exact bytes of '質問' — not confuse them
-- with partial matches on other CJK characters that share byte prefixes.

-- Japanese: find tickets containing '質問' (question)
ASSERT ROW_COUNT = 1
SELECT id, customer_name, message_snippet
FROM {{zone_name}}.delta_demos.support_tickets
WHERE message_snippet LIKE '%質問%';

-- French diacritics: find tickets containing 'crème'
ASSERT ROW_COUNT = 1
SELECT id, customer_name, message_snippet
FROM {{zone_name}}.delta_demos.support_tickets
WHERE message_snippet LIKE '%crème%';


-- ============================================================================
-- LEARN: SUBSTR respects character boundaries
-- ============================================================================
-- SUBSTR(name, 1, 5) must return the first 5 CHARACTERS, not 5 bytes.
-- For Greek 'Αλέξανδρος Νίκου', the first 5 characters are 'Αλέξα' (10 bytes).
-- For Korean '김서연' (only 3 chars), SUBSTR returns all 3 characters.

ASSERT VALUE first_five = 'Αλέξα'
SELECT SUBSTR(customer_name, 1, 5) AS first_five FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 4;

ASSERT VALUE first_five = 'Ивано'
SELECT SUBSTR(customer_name, 1, 5) AS first_five FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 5;

-- Korean: only 3 chars available, SUBSTR returns all 3
ASSERT VALUE first_five = '김서연'
SELECT SUBSTR(customer_name, 1, 5) AS first_five FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 16;


-- ============================================================================
-- LEARN: CONCAT mixing multiple scripts in one value
-- ============================================================================
-- Support agents need combined display strings. CONCAT must safely join
-- text from different scripts without byte corruption at the boundaries.

ASSERT ROW_COUNT = 4
SELECT id, customer_name || ' — ' || message_snippet AS combined,
       LENGTH(customer_name || ' — ' || message_snippet) AS combined_length
FROM {{zone_name}}.delta_demos.support_tickets
WHERE id IN (1, 5, 6, 16)
ORDER BY id;


-- ============================================================================
-- EXPLORE: Priority distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
SELECT priority, COUNT(*) AS ticket_count
FROM {{zone_name}}.delta_demos.support_tickets
GROUP BY priority
ORDER BY priority;


-- ============================================================================
-- MUTATE: Resolve closed tickets — UPDATE status
-- ============================================================================
-- Update all 'closed' tickets to 'resolved'. Tests that Unicode customer
-- names and messages survive a copy-on-write rewrite triggered by a
-- non-Unicode column change.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.support_tickets
SET status = 'resolved'
WHERE status = 'closed';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total ticket count unchanged
ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.delta_demos.support_tickets;

-- Verify high-priority count
ASSERT VALUE high_count = 7
SELECT COUNT(*) AS high_count FROM {{zone_name}}.delta_demos.support_tickets WHERE priority = 'high';

-- Verify escalated count
ASSERT VALUE esc_count = 2
SELECT COUNT(*) AS esc_count FROM {{zone_name}}.delta_demos.support_tickets WHERE status = 'escalated';

-- Verify resolved count (was 3 closed → now 3 resolved)
ASSERT VALUE resolved_count = 3
SELECT COUNT(*) AS resolved_count FROM {{zone_name}}.delta_demos.support_tickets WHERE status = 'resolved';

-- Verify Greek name preserved
ASSERT VALUE customer_name = 'Αλέξανδρος Νίκου'
SELECT customer_name FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 4;

-- Verify Hebrew name preserved
ASSERT VALUE customer_name = 'שרה כהן'
SELECT customer_name FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 12;

-- Verify Persian name preserved
ASSERT VALUE customer_name = 'محمد رضایی'
SELECT customer_name FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 19;

-- Verify Korean name preserved
ASSERT VALUE customer_name = '김서연'
SELECT customer_name FROM {{zone_name}}.delta_demos.support_tickets WHERE id = 16;
