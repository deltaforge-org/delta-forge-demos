-- ============================================================================
-- Library Catalog — Index Lifecycle, Staleness, and Rebuild
-- ============================================================================
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                INDEXES ARE MANAGED OBJECTS                           │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │                                                                      │
--  │ A row-level index is built from the table at a specific moment.     │
--  │ When the table changes (INSERT, UPDATE, DELETE, MERGE), the index   │
--  │ has to change too — otherwise it would point at rows that no       │
--  │ longer exist or miss rows that just got added.                       │
--  │                                                                      │
--  │ There are two ways to keep an index in sync with its table:         │
--  │                                                                      │
--  │   1. AUTO-UPDATE      — every write to the table also updates the   │
--  │   (auto_update=true)   index, in the same commit. Simple and       │
--  │                        always-correct, but adds work to writers.   │
--  │                                                                      │
--  │   2. MANUAL REBUILD   — writes leave the index alone. The index    │
--  │   (auto_update=false)  goes STALE the moment the table changes.    │
--  │                        An operator runs REBUILD INDEX later to     │
--  │                        bring it back in sync.                       │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                  INDEX STATUS STATE MACHINE                          │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │                                                                      │
--  │   building  ──→  current  ──→  stale  ──→  current                  │
--  │                    ↑              │           ↑                      │
--  │                    │              │           │                      │
--  │              CREATE INDEX     write to       REBUILD INDEX           │
--  │                                table                                 │
--  │                                                                      │
--  │   • current     — index version equals table version. USABLE.       │
--  │   • stale       — table moved on, index didn't. IGNORED by readers. │
--  │   • building    — initial CREATE INDEX still running. NOT YET USABLE│
--  │   • tombstoned  — DROP INDEX issued, awaiting VACUUM. NOT USED.     │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                    THE SAFETY GUARANTEE                              │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │                                                                      │
--  │ A stale index NEVER causes wrong answers. When the engine sees a    │
--  │ stale index it ignores it and falls back to ordinary file pruning. │
--  │ Queries during the stale window are CORRECT, just possibly SLOWER.  │
--  │                                                                      │
--  │ This means you can safely turn auto-update off, batch your writes,  │
--  │ and rebuild on a schedule. You never have to choose between fast    │
--  │ writes and correct reads.                                            │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │              WHEN TO TURN AUTO-UPDATE OFF                            │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ ✓ Writes are batched (e.g. a nightly bulk load) and a single         │
--  │   REBUILD afterward is cheaper than per-write maintenance            │
--  │ ✓ Writers are latency-sensitive and you can tolerate a stale         │
--  │   window between writes and the rebuild                              │
--  │ ✓ External tools write to the table without going through Delta     │
--  │   Forge — they can't update the index, so auto-update is moot       │
--  │                                                                      │
--  │ When to leave auto-update ON (the default):                          │
--  │ ✓ Writes are continuous and small — many tiny writes amortise the   │
--  │   index maintenance cheaply                                          │
--  │ ✓ Reads can't tolerate a stale window                                │
--  │ ✓ You don't want to remember to schedule a rebuild                   │
--  └──────────────────────────────────────────────────────────────────────┘
--
-- This demo creates an index with auto_update=false on purpose, then
-- walks the full lifecycle: BUILD → use → write (which makes it stale)
-- → correct-but-slower lookups → REBUILD → ALTER to switch on
-- auto-update.
-- ============================================================================


-- ============================================================================
-- BUILD: Create the Index — auto_update DISABLED on purpose
-- ============================================================================
-- The reading workload is heavy (lookup by isbn) but writes are
-- batched into a nightly vendor load. We want the writers to stay
-- cheap, so we disable auto-update and plan to REBUILD INDEX once
-- per night after the load finishes. This is the configuration that
-- exposes the staleness lifecycle the rest of this demo walks through.

CREATE INDEX idx_isbn
    ON TABLE {{zone_name}}.delta_demos.library_catalog (isbn)
    WITH (auto_update = false);


-- ============================================================================
-- EXPLORE: Initial Catalog
-- ============================================================================
-- 40 books across 4 branches and several genres.

ASSERT ROW_COUNT = 4
ASSERT VALUE book_count = 10 WHERE branch = 'central'
ASSERT VALUE book_count = 10 WHERE branch = 'east'
ASSERT VALUE book_count = 10 WHERE branch = 'west'
ASSERT VALUE book_count = 10 WHERE branch = 'south'
SELECT branch,
       COUNT(*)                       AS book_count,
       SUM(copies)                    AS total_copies,
       SUM(available)                 AS available_copies
FROM {{zone_name}}.delta_demos.library_catalog
GROUP BY branch
ORDER BY branch;


-- ============================================================================
-- LEARN: Index Just-Built — Status Should Be `current`
-- ============================================================================
-- Right after CREATE INDEX, the index version equals the table
-- version. Aware readers will use it for any predicate on isbn.

DESCRIBE INDEX idx_isbn ON TABLE {{zone_name}}.delta_demos.library_catalog;


-- ============================================================================
-- LEARN: Lookup While Index is Current
-- ============================================================================
-- This point lookup is what we built the index for: routes straight
-- to the slice carrying the matching isbn.

ASSERT ROW_COUNT = 1
ASSERT VALUE title = 'Citadel of Glass'
ASSERT VALUE author = 'Roderik Stam'
ASSERT VALUE branch = 'east'
ASSERT VALUE copies = 8
SELECT isbn, title, author, genre, publish_year, copies, available, branch
FROM {{zone_name}}.delta_demos.library_catalog
WHERE isbn = '978-0016';


-- ============================================================================
-- LEARN: A Write Happens — Nightly Acquisitions
-- ============================================================================
-- The vendor delivers 5 new books overnight. With auto_update = false,
-- the parent's version moves forward but the index's version doesn't.
-- The index is now stale.

INSERT INTO {{zone_name}}.delta_demos.library_catalog VALUES
    ('978-0041','Cinder Bay',           'Aurelio Cifuentes',  'fiction',  2024, 5, 5, 'central'),
    ('978-0042','The Velvet Equation',  'Emiko Tanizaki',     'science',  2024, 4, 4, 'east'),
    ('978-0043','Reed and Saltwater',   'Ferdia Mac Cana',    'fiction',  2024, 6, 6, 'west'),
    ('978-0044','Telegraph Towers',     'Konstantin Veres',   'history',  2024, 3, 3, 'south'),
    ('978-0045','The Persimmon Year',   'Naoko Hartmann',     'fiction',  2024, 7, 7, 'central');

ASSERT ROW_COUNT = 1
ASSERT VALUE total_books = 45
SELECT COUNT(*) AS total_books
FROM {{zone_name}}.delta_demos.library_catalog;


-- ============================================================================
-- LEARN: Lookup During Stale Window — Correct Answer, Slower Path
-- ============================================================================
-- The new book is findable. The stale index is silently ignored;
-- the engine falls back to ordinary file pruning. This is the
-- safety guarantee — wrong answers are never possible, only slower
-- ones.

ASSERT ROW_COUNT = 1
ASSERT VALUE title = 'Cinder Bay'
ASSERT VALUE branch = 'central'
ASSERT VALUE publish_year = 2024
SELECT isbn, title, branch, publish_year, copies
FROM {{zone_name}}.delta_demos.library_catalog
WHERE isbn = '978-0041';


-- ============================================================================
-- LEARN: REBUILD INDEX — Bring it Back to Current
-- ============================================================================
-- Operator runs REBUILD as the nightly job's last step. Index
-- regenerates from the current table state and matches the parent
-- version again. Subsequent reads use it.

REBUILD INDEX idx_isbn ON TABLE {{zone_name}}.delta_demos.library_catalog;

DESCRIBE INDEX idx_isbn ON TABLE {{zone_name}}.delta_demos.library_catalog;


-- ============================================================================
-- LEARN: Lookup After Rebuild
-- ============================================================================
-- Same point lookup as before — now back on the fast path.

ASSERT ROW_COUNT = 1
ASSERT VALUE title = 'The Persimmon Year'
ASSERT VALUE author = 'Naoko Hartmann'
ASSERT VALUE branch = 'central'
SELECT isbn, title, author, branch, copies
FROM {{zone_name}}.delta_demos.library_catalog
WHERE isbn = '978-0045';


-- ============================================================================
-- LEARN: Switching to auto_update — Skip Manual Rebuilds
-- ============================================================================
-- If the team decides nightly rebuilds are operationally annoying,
-- ALTER flips on auto_update. Going forward, writes maintain the
-- index as part of the same commit.

ALTER INDEX idx_isbn ON TABLE {{zone_name}}.delta_demos.library_catalog
    SET (auto_update = true);

INSERT INTO {{zone_name}}.delta_demos.library_catalog VALUES
    ('978-0046','Quartet for Late Trains','Roselin Chambers','fiction',2024, 5, 5, 'east');

ASSERT ROW_COUNT = 1
ASSERT VALUE title = 'Quartet for Late Trains'
SELECT title FROM {{zone_name}}.delta_demos.library_catalog WHERE isbn = '978-0046';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_books = 46
ASSERT VALUE distinct_isbn = 46
ASSERT VALUE distinct_branches = 4
ASSERT VALUE genre_count = 6
ASSERT VALUE total_copies = 236
SELECT COUNT(*)                         AS total_books,
       COUNT(DISTINCT isbn)             AS distinct_isbn,
       COUNT(DISTINCT branch)           AS distinct_branches,
       COUNT(DISTINCT genre)            AS genre_count,
       SUM(copies)                      AS total_copies
FROM {{zone_name}}.delta_demos.library_catalog;
