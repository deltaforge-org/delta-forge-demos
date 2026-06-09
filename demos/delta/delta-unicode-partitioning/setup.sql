-- ============================================================================
-- Delta Unicode Partitioning — Non-ASCII Partition Keys — Setup Script
-- ============================================================================
-- Demonstrates partitioning where the partition key values are non-ASCII:
--   - 日本語 (Japanese), العربية (Arabic), Русский (Russian),
--     Français (French), Português (Portuguese)
--   - Partition pruning with multi-byte directory names
--   - DML operations scoped to Unicode partitions
--
-- Tables created:
--   1. cdn_content — 30 CDN content items partitioned by locale (native script)
--
-- Operations performed:
--   1. CREATE DELTA TABLE PARTITIONED BY (locale)
--   2. INSERT — 30 items (6 per locale) with localized titles
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: cdn_content — global CDN content metadata
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.cdn_content (
    id              INT,
    content_key     VARCHAR,
    title           VARCHAR,
    title_local     VARCHAR,
    content_type    VARCHAR,
    size_mb         INT,
    origin_city     VARCHAR,
    locale          VARCHAR
) PARTITIONED BY (locale)
  LOCATION 'delta-unicode-partitioning/cdn_content';


-- STEP 2: Insert 30 content items across 5 locales

-- 日本語 partition (6 items)
INSERT INTO {{zone_name}}.delta_demos.cdn_content VALUES
    (1,  'anime-stream-001',  'Demon Slayer S4',           '鬼滅の刃 第4期',         'video',  2048, 'Tokyo',     '日本語'),
    (2,  'manga-scan-042',    'One Piece Ch 1120',         'ワンピース 第1120話',      'image',  85,   'Osaka',     '日本語'),
    (3,  'game-patch-jp',     'Final Fantasy Patch',       'ファイナルファンタジー',    'binary', 4500, 'Tokyo',     '日本語'),
    (4,  'news-feed-nhk',     'NHK Morning News',          'NHKおはよう日本',          'text',   12,   'Tokyo',     '日本語'),
    (5,  'music-stream-jp',   'City Pop Collection',       'シティポップ名曲集',       'audio',  340,  'Osaka',     '日本語'),
    (6,  'podcast-tech-jp',   'Tech Talk Japan',           'テックトーク日本',         'audio',  95,   'Nagoya',    '日本語');

-- العربية partition (6 items)
INSERT INTO {{zone_name}}.delta_demos.cdn_content VALUES
    (7,  'news-aljazeera',    'Breaking News Feed',        'الأخبار العاجلة',         'text',   8,    'Doha',      'العربية'),
    (8,  'stream-mbc',        'Ramadan Series',            'مسلسل رمضان',             'video',  3200, 'Dubai',     'العربية'),
    (9,  'quran-audio',       'Recitation Collection',     'تلاوات قرآنية',           'audio',  450,  'Mecca',     'العربية'),
    (10, 'news-cairo',        'Egypt Daily Digest',        'ملخص أخبار مصر',          'text',   5,    'Cairo',     'العربية'),
    (11, 'edu-arabic-lang',   'Arabic Grammar Course',     'دورة النحو العربي',       'video',  1800, 'Beirut',    'العربية'),
    (12, 'podcast-tech-ar',   'Arab Tech Weekly',          'تقنية الأسبوع',           'audio',  120,  'Riyadh',    'العربية');

-- Русский partition (6 items)
INSERT INTO {{zone_name}}.delta_demos.cdn_content VALUES
    (13, 'news-tass',         'TASS Daily Brief',          'Ежедневный обзор ТАСС',   'text',   6,    'Moscow',         'Русский'),
    (14, 'stream-kinopoisk',  'Russian Cinema Classics',   'Классика русского кино',   'video',  2800, 'Moscow',         'Русский'),
    (15, 'music-classical',   'Tchaikovsky Collection',    'Коллекция Чайковского',    'audio',  580,  'St Petersburg',  'Русский'),
    (16, 'edu-code-ru',       'Python Course (RU)',        'Курс Python на русском',   'video',  950,  'Novosibirsk',    'Русский'),
    (17, 'podcast-science',   'Russian Science Weekly',    'Наука за неделю',          'audio',  78,   'Moscow',         'Русский'),
    (18, 'ebook-tolstoy',     'War and Peace Digital',     'Война и мир',              'text',   15,   'Moscow',         'Русский');

-- Français partition (6 items)
INSERT INTO {{zone_name}}.delta_demos.cdn_content VALUES
    (19, 'news-lemonde',      'Le Monde Headlines',        'Les titres du Monde',      'text',   4,    'Paris',     'Français'),
    (20, 'stream-canal',      'Canal+ Exclusive',          'Exclusivité Canal+',       'video',  2200, 'Paris',     'Français'),
    (21, 'music-chanson',     'French Chanson Best Of',    'Chanson française',        'audio',  290,  'Lyon',      'Français'),
    (22, 'edu-cuisine',       'Cordon Bleu Masterclass',   'Maîtrise culinaire',       'video',  1500, 'Paris',     'Français'),
    (23, 'podcast-culture',   'Culture Café',              'Café culturel',            'audio',  65,   'Marseille', 'Français'),
    (24, 'ebook-hugo',        'Les Misérables Digital',    'Les Misérables numérique', 'text',   18,   'Paris',     'Français');

-- Português partition (6 items)
INSERT INTO {{zone_name}}.delta_demos.cdn_content VALUES
    (25, 'news-globo',        'Globo News Update',         'Atualização Globo',        'text',   7,    'São Paulo',      'Português'),
    (26, 'stream-telenovela', 'Telenovela Season 3',       'Temporada de novela',      'video',  2600, 'Rio de Janeiro', 'Português'),
    (27, 'music-bossa',       'Bossa Nova Essentials',     'Essenciais da Bossa',      'audio',  310,  'São Paulo',      'Português'),
    (28, 'edu-capoeira',      'Capoeira Training',         'Treinamento de Capoeira',  'video',  800,  'Salvador',       'Português'),
    (29, 'podcast-futebol',   'Futebol Semanal',           'Análise semanal',          'audio',  55,   'Porto',          'Português'),
    (30, 'news-lisbon',       'Lisbon Daily',              'Diário de Lisboa',         'text',   3,    'Lisbon',         'Português');
