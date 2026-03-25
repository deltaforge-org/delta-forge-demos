-- ============================================================================
-- Delta Unicode Partitioning — Educational Queries
-- ============================================================================
-- WHAT: Delta creates partition directories named after partition key values.
--       When keys are non-ASCII (日本語, العربية, Русский), the directory names
--       contain UTF-8 encoded multi-byte characters.
-- WHY:  StreamNet CDN partitions content metadata by locale in the user's native
--       script. Partition pruning must correctly match multi-byte WHERE predicates
--       to the right partition directory — a byte-level comparison, not ASCII.
-- HOW:  Delta URL-encodes partition values in directory names. The engine decodes
--       them back when reading, so WHERE locale = '日本語' correctly prunes to
--       a single partition regardless of the encoding scheme.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Partition overview — all 5 Unicode-keyed partitions
-- ============================================================================
-- Each partition is named after a locale in its native script. This query
-- touches ALL partitions to build a summary — no pruning, full scan.

ASSERT ROW_COUNT = 5
SELECT locale, COUNT(*) AS items,
       SUM(size_mb) AS total_size_mb,
       COUNT(DISTINCT origin_city) AS cities
FROM {{zone_name}}.delta_demos.cdn_content
GROUP BY locale
ORDER BY locale;


-- ============================================================================
-- LEARN: Partition pruning with Japanese key — WHERE locale = '日本語'
-- ============================================================================
-- Delta reads ONLY the 日本語 partition directory. The other 4 partitions
-- (العربية, Русский, Français, Português) are never opened. The UTF-8
-- bytes of '日本語' in the WHERE clause must match the URL-encoded directory
-- name exactly for pruning to work.

ASSERT ROW_COUNT = 6
SELECT id, content_key, title, title_local, content_type, size_mb
FROM {{zone_name}}.delta_demos.cdn_content
WHERE locale = '日本語'
ORDER BY id;


-- ============================================================================
-- LEARN: Partition pruning with Arabic key — WHERE locale = 'العربية'
-- ============================================================================
-- Arabic is right-to-left, but the bytes are stored left-to-right in UTF-8.
-- Partition pruning works on byte comparison, so RTL display order is
-- irrelevant to the storage engine.

ASSERT ROW_COUNT = 6
SELECT id, content_key, title, title_local, content_type, size_mb
FROM {{zone_name}}.delta_demos.cdn_content
WHERE locale = 'العربية'
ORDER BY id;

-- Verify Arabic text preserved within the partition
ASSERT VALUE title_local = 'الأخبار العاجلة'
SELECT title_local FROM {{zone_name}}.delta_demos.cdn_content WHERE id = 7;


-- ============================================================================
-- LEARN: Cross-partition aggregation by content type
-- ============================================================================
-- This query spans ALL partitions but groups by content_type (ASCII).
-- It proves that cross-partition queries work identically whether partition
-- keys are ASCII or multi-byte Unicode.

ASSERT ROW_COUNT = 5
SELECT content_type, COUNT(*) AS items,
       SUM(size_mb) AS total_size_mb
FROM {{zone_name}}.delta_demos.cdn_content
GROUP BY content_type
ORDER BY content_type;


-- ============================================================================
-- MUTATE: UPDATE within Русский partition — double video sizes
-- ============================================================================
-- Scoping an UPDATE to a single Unicode partition. Delta must correctly
-- identify the Русский partition directory and rewrite only those Parquet
-- files. The Cyrillic text in title_local must survive the copy-on-write.

ASSERT ROW_COUNT = 2
UPDATE {{zone_name}}.delta_demos.cdn_content
SET size_mb = size_mb * 2
WHERE locale = 'Русский' AND content_type = 'video';


-- ============================================================================
-- LEARN: Verify Cyrillic text preserved after partition-scoped UPDATE
-- ============================================================================
-- The UPDATE above rewrote Parquet files in the Русский partition. Assert
-- that the Cyrillic title_local values survived intact alongside the new
-- size_mb values.

-- Russian Cinema Classics — size doubled from 2800 to 5600
ASSERT VALUE title_local = 'Классика русского кино'
SELECT title_local FROM {{zone_name}}.delta_demos.cdn_content WHERE id = 14;

ASSERT VALUE size_mb = 5600
SELECT size_mb FROM {{zone_name}}.delta_demos.cdn_content WHERE id = 14;

-- Python Course — size doubled from 950 to 1900
ASSERT VALUE title_local = 'Курс Python на русском'
SELECT title_local FROM {{zone_name}}.delta_demos.cdn_content WHERE id = 16;

ASSERT VALUE size_mb = 1900
SELECT size_mb FROM {{zone_name}}.delta_demos.cdn_content WHERE id = 16;


-- ============================================================================
-- MUTATE: DELETE small text items across ALL Unicode partitions
-- ============================================================================
-- DELETE WHERE content_type = 'text' AND size_mb < 10 spans 4 of 5 partitions
-- (日本語 has no match since its text item is 12 MB). Delta must locate and
-- rewrite files in each affected Unicode-keyed partition directory.
-- Deleted: ids 7,10 (العربية), 13 (Русский), 19 (Français), 25,30 (Português)

ASSERT ROW_COUNT = 6
DELETE FROM {{zone_name}}.delta_demos.cdn_content
WHERE content_type = 'text' AND size_mb < 10;


-- ============================================================================
-- MUTATE: INSERT new content into Français partition
-- ============================================================================
-- Adding content to an existing Unicode partition. Delta appends new Parquet
-- files into the Français directory alongside existing ones.

INSERT INTO {{zone_name}}.delta_demos.cdn_content VALUES
    (31, 'docu-ocean',     'Documentaire Océan',  'Océan : les profondeurs',  'video', 1750, 'Marseille', 'Français'),
    (32, 'poetry-digital', 'Poésie Numérique',    'Anthologie poétique',      'text',  25,   'Paris',     'Français');


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total: 30 - 6 deleted + 2 inserted = 26
ASSERT ROW_COUNT = 26
SELECT * FROM {{zone_name}}.delta_demos.cdn_content;

-- Verify all 5 Unicode partitions still exist
ASSERT VALUE locale_count = 5
SELECT COUNT(DISTINCT locale) AS locale_count FROM {{zone_name}}.delta_demos.cdn_content;

-- Verify 日本語 partition: 6 items (no deletes, no inserts)
ASSERT VALUE jp_count = 6
SELECT COUNT(*) AS jp_count FROM {{zone_name}}.delta_demos.cdn_content WHERE locale = '日本語';

-- Verify العربية partition: 4 items (2 deleted)
ASSERT VALUE ar_count = 4
SELECT COUNT(*) AS ar_count FROM {{zone_name}}.delta_demos.cdn_content WHERE locale = 'العربية';

-- Verify Русский partition: 5 items (1 deleted)
ASSERT VALUE ru_count = 5
SELECT COUNT(*) AS ru_count FROM {{zone_name}}.delta_demos.cdn_content WHERE locale = 'Русский';

-- Verify Français partition: 7 items (1 deleted + 2 inserted)
ASSERT VALUE fr_count = 7
SELECT COUNT(*) AS fr_count FROM {{zone_name}}.delta_demos.cdn_content WHERE locale = 'Français';

-- Verify Português partition: 4 items (2 deleted)
ASSERT VALUE pt_count = 4
SELECT COUNT(*) AS pt_count FROM {{zone_name}}.delta_demos.cdn_content WHERE locale = 'Português';

-- Verify Japanese text preserved through all mutations
ASSERT VALUE title_local = '鬼滅の刃 第4期'
SELECT title_local FROM {{zone_name}}.delta_demos.cdn_content WHERE id = 1;

-- Verify Arabic text preserved after partition-level DELETE
ASSERT VALUE title_local = 'تلاوات قرآنية'
SELECT title_local FROM {{zone_name}}.delta_demos.cdn_content WHERE id = 9;

-- Verify Cyrillic preserved after UPDATE + DELETE in same partition
ASSERT VALUE title_local = 'Коллекция Чайковского'
SELECT title_local FROM {{zone_name}}.delta_demos.cdn_content WHERE id = 15;
