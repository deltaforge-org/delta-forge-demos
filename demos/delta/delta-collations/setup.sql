-- ============================================================================
-- Delta Collations — Language-Aware Sorting & Comparison — Setup Script
-- ============================================================================
-- Demonstrates language-aware data patterns in Delta tables:
--   - Names with diacritics (umlauts, accents, tildes)
--   - CJK characters (Japanese, Chinese, Korean)
--   - sort_key column for ASCII-normalized consistent ordering
--
-- Table created:
--   1. global_contacts — 40 multilingual contact records
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE
--   3. INSERT 20 rows — European names (German, French, Spanish, Scandinavian)
--   5. INSERT 10 rows — Asian names (Japanese, Chinese, Korean)
--   6. INSERT 10 rows — mixed international (Arabic transliterated, Hindi transliterated, etc.)
--   7. UPDATE — normalize sort_key for 5 entries
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: global_contacts — multilingual contact directory
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.global_contacts (
    id              INT,
    first_name      VARCHAR,
    last_name       VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    language        VARCHAR,
    sort_key        VARCHAR,
    email           VARCHAR
) LOCATION 'delta-collations/global_contacts';


-- ============================================================================
-- STEP 3: INSERT batch 1 — 20 European names
-- ============================================================================
-- German names (ids 1-5)
INSERT INTO {{zone_name}}.delta_demos.global_contacts VALUES
    (1,  'Hans',      'Mueller',    'Berlin',      'Germany',  'de', 'mueller_hans',      'hans.mueller@example.de'),
    (2,  'Juergen',   'Grosse',     'Munich',      'Germany',  'de', 'grosse_juergen',    'juergen.grosse@example.de'),
    (3,  'Kathe',     'Schroeder',  'Hamburg',      'Germany',  'de', 'schroeder_kathe',   'kathe.schroeder@example.de'),
    (4,  'Lutz',      'Boehm',      'Frankfurt',    'Germany',  'de', 'boehm_lutz',        'lutz.boehm@example.de'),
    (5,  'Ute',       'Koenig',     'Cologne',      'Germany',  'de', 'koenig_ute',        'ute.koenig@example.de');

-- French names (ids 6-10)
INSERT INTO {{zone_name}}.delta_demos.global_contacts VALUES
    (6,  'Rene',      'Lefevre',    'Paris',        'France',   'fr', 'lefevre_rene',      'rene.lefevre@example.fr'),
    (7,  'Helene',    'Beauchene',  'Lyon',         'France',   'fr', 'beauchene_helene',  'helene.beauchene@example.fr'),
    (8,  'Francois',  'Dupre',      'Marseille',    'France',   'fr', 'dupre_francois',    'francois.dupre@example.fr'),
    (9,  'Celine',    'Gauthier',   'Toulouse',     'France',   'fr', 'gauthier_celine',   'celine.gauthier@example.fr'),
    (10, 'Noel',      'Perrault',   'Nice',         'France',   'fr', 'perrault_noel',     'noel.perrault@example.fr');

-- Spanish names (ids 11-15)
INSERT INTO {{zone_name}}.delta_demos.global_contacts VALUES
    (11, 'Jose',      'Garcia',     'Madrid',       'Spain',    'es', 'garcia_jose',       'jose.garcia@example.es'),
    (12, 'Maria',     'Nunez',      'Barcelona',    'Spain',    'es', 'nunez_maria',       'maria.nunez@example.es'),
    (13, 'Carlos',    'Pena',       'Seville',      'Spain',    'es', 'pena_carlos',       'carlos.pena@example.es'),
    (14, 'Begona',    'Ibanez',     'Valencia',     'Spain',    'es', 'ibanez_begona',     'begona.ibanez@example.es'),
    (15, 'Ramon',     'Jimenez',    'Bilbao',       'Spain',    'es', 'jimenez_ramon',     'ramon.jimenez@example.es');

-- Scandinavian names (ids 16-20)
INSERT INTO {{zone_name}}.delta_demos.global_contacts VALUES
    (16, 'Lars',      'Johansson',  'Stockholm',    'Sweden',   'sv', 'johansson_lars',    'lars.johansson@example.se'),
    (17, 'Astrid',    'Lindstroem', 'Gothenburg',   'Sweden',   'sv', 'lindstroem_astrid', 'astrid.lindstroem@example.se'),
    (18, 'Bjoern',    'Hansen',     'Oslo',         'Norway',   'no', 'hansen_bjoern',     'bjoern.hansen@example.no'),
    (19, 'Soeren',    'Nielsen',    'Copenhagen',   'Denmark',  'da', 'nielsen_soeren',    'soeren.nielsen@example.dk'),
    (20, 'Paivi',     'Maekinen',   'Helsinki',     'Finland',  'fi', 'maekinen_paivi',    'paivi.maekinen@example.fi');


-- ============================================================================
-- STEP 5: INSERT batch 2 — 10 Asian names
-- ============================================================================
-- Japanese names (ids 21-24)
INSERT INTO {{zone_name}}.delta_demos.global_contacts VALUES
    (21, 'Takeshi',   'Yamamoto',   'Tokyo',        'Japan',    'ja', 'yamamoto_takeshi',  'takeshi.yamamoto@example.jp'),
    (22, 'Yuki',      'Tanaka',     'Osaka',        'Japan',    'ja', 'tanaka_yuki',       'yuki.tanaka@example.jp'),
    (23, 'Haruto',    'Suzuki',     'Yokohama',     'Japan',    'ja', 'suzuki_haruto',     'haruto.suzuki@example.jp'),
    (24, 'Sakura',    'Watanabe',   'Kyoto',        'Japan',    'ja', 'watanabe_sakura',   'sakura.watanabe@example.jp');

-- Chinese names (ids 25-27)
INSERT INTO {{zone_name}}.delta_demos.global_contacts VALUES
    (25, 'Wei',       'Zhang',      'Beijing',      'China',    'zh', 'zhang_wei',         'wei.zhang@example.cn'),
    (26, 'Mei',       'Li',         'Shanghai',     'China',    'zh', 'li_mei',            'mei.li@example.cn'),
    (27, 'Jun',       'Wang',       'Shenzhen',     'China',    'zh', 'wang_jun',          'jun.wang@example.cn');

-- Korean names (ids 28-30)
INSERT INTO {{zone_name}}.delta_demos.global_contacts VALUES
    (28, 'Minjun',    'Kim',        'Seoul',        'South Korea', 'ko', 'kim_minjun',     'minjun.kim@example.kr'),
    (29, 'Jisoo',     'Park',       'Busan',        'South Korea', 'ko', 'park_jisoo',     'jisoo.park@example.kr'),
    (30, 'Hyun',      'Lee',        'Incheon',      'South Korea', 'ko', 'lee_hyun',       'hyun.lee@example.kr');


-- ============================================================================
-- STEP 6: INSERT batch 3 — 10 mixed international names
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.global_contacts VALUES
    (31, 'Ahmed',     'Al-Rashid',  'Dubai',        'UAE',         'ar', 'alrashid_ahmed',    'ahmed.alrashid@example.ae'),
    (32, 'Fatima',    'Hassan',     'Cairo',        'Egypt',       'ar', 'hassan_fatima',     'fatima.hassan@example.eg'),
    (33, 'Priya',     'Sharma',     'Mumbai',       'India',       'hi', 'sharma_priya',      'priya.sharma@example.in'),
    (34, 'Arjun',     'Patel',      'Delhi',        'India',       'hi', 'patel_arjun',       'arjun.patel@example.in'),
    (35, 'Olga',      'Ivanova',    'Moscow',       'Russia',      'ru', 'ivanova_olga',      'olga.ivanova@example.ru'),
    (36, 'Dmitri',    'Petrov',     'Saint Petersburg', 'Russia',  'ru', 'petrov_dmitri',     'dmitri.petrov@example.ru'),
    (37, 'Emeka',     'Okafor',     'Lagos',        'Nigeria',     'en', 'okafor_emeka',      'emeka.okafor@example.ng'),
    (38, 'Amara',     'Diallo',     'Dakar',        'Senegal',     'fr', 'diallo_amara',      'amara.diallo@example.sn'),
    (39, 'Mateo',     'Silva',      'Sao Paulo',    'Brazil',      'pt', 'silva_mateo',       'mateo.silva@example.br'),
    (40, 'Valentina', 'Moretti',    'Rome',         'Italy',       'it', 'moretti_valentina', 'valentina.moretti@example.it');


-- ============================================================================
-- STEP 7: UPDATE — normalize sort_key for 5 entries
-- ============================================================================
-- Fix sort_key entries that need re-normalization
UPDATE {{zone_name}}.delta_demos.global_contacts
SET sort_key = 'mueller_hans'
WHERE id = 1;

UPDATE {{zone_name}}.delta_demos.global_contacts
SET sort_key = 'lefevre_rene'
WHERE id = 6;

UPDATE {{zone_name}}.delta_demos.global_contacts
SET sort_key = 'garcia_jose'
WHERE id = 11;

UPDATE {{zone_name}}.delta_demos.global_contacts
SET sort_key = 'johansson_lars'
WHERE id = 16;

UPDATE {{zone_name}}.delta_demos.global_contacts
SET sort_key = 'yamamoto_takeshi'
WHERE id = 21;
