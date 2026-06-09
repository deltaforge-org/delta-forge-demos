-- ============================================================================
-- Delta Unicode Roundtrip — Mutation Fidelity Proof — Setup Script
-- ============================================================================
-- Demonstrates UTF-8 roundtrip fidelity through every DML operation:
--   - Multi-script product names (CJK, Arabic, Hebrew, Cyrillic, Latin diacritics)
--   - Partitioning by region (ASCII keys, Unicode data within)
--   - INSERT 30 products across 3 regions with local-script names
--
-- Tables created:
--   1. global_bazaar — 30 marketplace listings with local-script product names
--
-- Operations performed:
--   1. CREATE DELTA TABLE PARTITIONED BY (region)
--   2. INSERT — 30 products (10 per region) with native-script names
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: global_bazaar — international marketplace listings
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.global_bazaar (
    id                  INT,
    product_name        VARCHAR,
    product_name_local  VARCHAR,
    category            VARCHAR,
    price               DOUBLE,
    currency            VARCHAR,
    country             VARCHAR,
    region              VARCHAR
) PARTITIONED BY (region)
  LOCATION 'delta-unicode-roundtrip/global_bazaar';


-- STEP 2: Insert 30 products across 3 regions

-- Asia — CJK scripts (Japanese kanji/katakana, Korean hangul, Chinese simplified)
INSERT INTO {{zone_name}}.delta_demos.global_bazaar VALUES
    (1,  'Matcha Ceremony Set',     '抹茶セレモニーセット',     'Tea & Beverages', 89.00,  'JPY', 'Japan',        'Asia'),
    (2,  'Silk Hanbok Fabric',      '실크 한복 원단',           'Textiles',        125.00, 'KRW', 'South Korea',  'Asia'),
    (3,  'Dim Sum Steamer',         '点心蒸笼',                'Kitchenware',     35.00,  'CNY', 'China',        'Asia'),
    (4,  'Sake Brewing Kit',        '日本酒醸造キット',         'Beverages',       210.00, 'JPY', 'Japan',        'Asia'),
    (5,  'Ginseng Root Extract',    '인삼 뿌리 추출물',         'Health',          45.00,  'KRW', 'South Korea',  'Asia'),
    (6,  'Calligraphy Brush Set',   '书法毛笔套装',             'Art Supplies',    28.00,  'CNY', 'China',        'Asia'),
    (7,  'Origami Master Kit',      '折り紙マスターキット',      'Crafts',          15.50,  'JPY', 'Japan',        'Asia'),
    (8,  'Kimchi Fermentation Jar', '김치 발효 항아리',          'Kitchenware',     42.00,  'KRW', 'South Korea',  'Asia'),
    (9,  'Dragon Boat Incense',     '龙舟香',                   'Home & Garden',   12.00,  'CNY', 'China',        'Asia'),
    (10, 'Tatami Floor Mat',        '畳フロアマット',            'Home & Garden',   78.00,  'JPY', 'Japan',        'Asia');

-- Europe — Latin diacritics, Cyrillic (Russian, Ukrainian), Greek
INSERT INTO {{zone_name}}.delta_demos.global_bazaar VALUES
    (11, 'Bavarian Pretzel Mix',    'Bayerische Brézelmischung', 'Food',           8.50,   'EUR', 'Germany',       'Europe'),
    (12, 'Crème Fraîche Culture',   'Crème fraîche starter',     'Food',           6.75,   'EUR', 'France',        'Europe'),
    (13, 'Matryoshka Nesting Doll', 'Матрёшка',                  'Souvenirs',      32.00,  'RUB', 'Russia',        'Europe'),
    (14, 'Greek Feta Brine',        'Φέτα σε άλμη',              'Food',           11.00,  'EUR', 'Greece',        'Europe'),
    (15, 'Czech Pilsner Yeast',     'Plzeňský kvasinkový kmen',  'Beverages',      14.50,  'CZK', 'Czech Republic','Europe'),
    (16, 'Icelandic Skyr Starter',  'Skyr ræktun',               'Food',           9.25,   'ISK', 'Iceland',       'Europe'),
    (17, 'Polish Pierogi Mold',     'Foremka do pierogów',       'Kitchenware',    18.00,  'PLN', 'Poland',        'Europe'),
    (18, 'Ukrainian Honey',         'Український мед',           'Food',           22.50,  'UAH', 'Ukraine',       'Europe'),
    (19, 'Spanish Paella Spice',    'Especias para paella',      'Spices',         7.00,   'EUR', 'Spain',         'Europe'),
    (20, 'Swedish Lingonberry Jam', 'Lingonsylt från Sverige',   'Food',           10.50,  'SEK', 'Sweden',        'Europe');

-- MENA — Arabic, Hebrew, Turkish (with İ/ı and ş/ç)
INSERT INTO {{zone_name}}.delta_demos.global_bazaar VALUES
    (21, 'Lebanese Cedar Oil',      'زيت الأرز اللبناني',       'Beauty',          38.00,  'LBP', 'Lebanon',  'MENA'),
    (22, 'Israeli Dead Sea Salt',   'מלח ים המוות',             'Beauty',          15.00,  'ILS', 'Israel',   'MENA'),
    (23, 'Turkish Baklava Box',     'Türk Baklavası',            'Food',            25.00,  'TRY', 'Turkey',   'MENA'),
    (24, 'Moroccan Ras el Hanout',  'رأس الحانوت المغربي',      'Spices',          12.00,  'MAD', 'Morocco',  'MENA'),
    (25, 'Persian Saffron Threads', 'زعفران نخ ایرانی',         'Spices',          95.00,  'IRR', 'Iran',     'MENA'),
    (26, 'Egyptian Papyrus Sheet',  'ورق بردي مصري',            'Art Supplies',    20.00,  'EGP', 'Egypt',    'MENA'),
    (27, 'Jordanian Za''atar Blend','زعتر أردني مميز',          'Spices',          8.50,   'JOD', 'Jordan',   'MENA'),
    (28, 'Omani Luban Incense',     'لبان عماني فاخر',          'Fragrance',       18.00,  'OMR', 'Oman',     'MENA'),
    (29, 'Dubai Date Syrup',        'دبس التمر الإماراتي',      'Food',            14.00,  'AED', 'UAE',      'MENA'),
    (30, 'Istanbul Ceramic Tile',   'İstanbul Çini Karolar',     'Home & Garden',   55.00,  'TRY', 'Turkey',   'MENA');
