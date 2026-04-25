-- ============================================================================
-- Library Catalog — Index Lifecycle, Staleness, and Rebuild — Setup
-- ============================================================================
-- A library catalog seeded with the initial collection. The full
-- index lifecycle (CREATE → use → write → stale → REBUILD → ALTER)
-- is exercised in queries.sql so each transition is part of the
-- lesson, not buried in setup.
--
-- Tables created:
--   1. library_catalog — 40 books
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.library_catalog (
    isbn         VARCHAR,
    title        VARCHAR,
    author       VARCHAR,
    genre        VARCHAR,
    publish_year INT,
    copies       INT,
    available    INT,
    branch       VARCHAR
) LOCATION 'library_catalog';


INSERT INTO {{zone_name}}.delta_demos.library_catalog VALUES
    ('978-0001','The Quiet Lighthouse',     'Hannah Voigt',       'fiction',     2018, 6, 4, 'central'),
    ('978-0002','Ironwood Roads',           'Jonas Marek',        'mystery',     2019, 4, 2, 'central'),
    ('978-0003','Atlas of Forgotten Maps',  'Liesel Auerbach',    'reference',   2014, 3, 3, 'central'),
    ('978-0004','Cornerstone',              'Ottilie Bauer',      'fiction',     2021, 8, 6, 'central'),
    ('978-0005','Quicksilver Quarters',     'Yannick Faulkner',   'fiction',     2020, 5, 4, 'central'),
    ('978-0006','Daybreak Sonata',          'Marisol Esteva',     'fiction',     2022, 7, 5, 'central'),
    ('978-0007','Old Iron Bridges',         'Henrik Lindblad',    'history',     2017, 4, 4, 'central'),
    ('978-0008','Vast Inland Plains',       'Cecelia Borgesi',    'history',     2016, 3, 2, 'central'),
    ('978-0009','The Watchmakers Atlas',    'Petr Konvalinka',    'reference',   2015, 2, 1, 'central'),
    ('978-0010','Saltmarsh Stories',        'Wren Kavanagh',      'fiction',     2023, 9, 7, 'central'),
    ('978-0011','Blue River Mornings',      'Anneliese Sturm',    'fiction',     2019, 4, 3, 'east'),
    ('978-0012','Cipher of the Coast',      'Theodoros Lyras',    'mystery',     2020, 5, 4, 'east'),
    ('978-0013','Wildflower Almanac',       'Sigrid Halvorsen',   'reference',   2018, 3, 3, 'east'),
    ('978-0014','Stone Walls',              'Gianluca Pirozzi',   'fiction',     2017, 6, 4, 'east'),
    ('978-0015','Whispering Pines',         'Maeve Donlon',       'fiction',     2021, 7, 6, 'east'),
    ('978-0016','Citadel of Glass',         'Roderik Stam',       'fantasy',     2022, 8, 7, 'east'),
    ('978-0017','Compass and Sextant',      'Iulia Manolescu',    'history',     2015, 3, 2, 'east'),
    ('978-0018','Fields of Argent',         'Alaric Vinter',      'fantasy',     2020, 6, 5, 'east'),
    ('978-0019','River Bend Journals',      'Niamh Casey',        'fiction',     2019, 5, 4, 'east'),
    ('978-0020','Twilight Mariners',        'Salvatore Aprea',    'fiction',     2022, 8, 6, 'east'),
    ('978-0021','The Cartographer is In',   'Dora Jankowska',     'mystery',     2018, 4, 3, 'west'),
    ('978-0022','Beacon Hill Diaries',      'Cyprian Wojcik',     'fiction',     2020, 6, 5, 'west'),
    ('978-0023','Bronze Bell Ringers',      'Aidan Llewellyn',    'history',     2014, 2, 2, 'west'),
    ('978-0024','Pale Moon Theorem',        'Kerstin Olofsson',   'science',     2021, 5, 4, 'west'),
    ('978-0025','The Fern Room',            'Beatrix Quintero',   'fiction',     2023, 9, 8, 'west'),
    ('978-0026','Salt and Cedar',           'Florent Beaulieu',   'fiction',     2019, 6, 4, 'west'),
    ('978-0027','Lantern Festival Logs',    'Renske Tibbe',       'history',     2016, 3, 2, 'west'),
    ('978-0028','Pivot Codex',              'Tomislav Vukovic',   'reference',   2020, 4, 4, 'west'),
    ('978-0029','Wisteria Court',           'Calandra Iovine',    'fiction',     2022, 7, 5, 'west'),
    ('978-0030','Brass Compass',            'Mikael Dragos',      'mystery',     2017, 4, 3, 'west'),
    ('978-0031','Wayfinder Notes',          'Saoirse OCallaghan', 'history',     2018, 3, 3, 'south'),
    ('978-0032','The Honey Mile',           'Heinrich Pfeifer',   'fiction',     2021, 6, 4, 'south'),
    ('978-0033','Rookery Lectures',         'Lyssandra Mavros',   'science',     2019, 4, 3, 'south'),
    ('978-0034','Beneath the Bramble',      'Cosmin Petrescu',    'fiction',     2020, 5, 4, 'south'),
    ('978-0035','Monsoon Annals',           'Aishath Naseema',    'history',     2015, 2, 2, 'south'),
    ('978-0036','Foxglove Tariffs',         'Quentin Brassard',   'fiction',     2022, 8, 6, 'south'),
    ('978-0037','The Ice Ledger',           'Vidar Snorrason',    'mystery',     2023, 9, 7, 'south'),
    ('978-0038','Granary Dispatches',       'Lavanya Subramani',  'history',     2017, 3, 3, 'south'),
    ('978-0039','Bone-White Mornings',      'Ezra Korhonen',      'fiction',     2021, 6, 5, 'south'),
    ('978-0040','Dim Stars Distant Suns',   'Solenne Mercier',    'science',     2020, 4, 3, 'south');
