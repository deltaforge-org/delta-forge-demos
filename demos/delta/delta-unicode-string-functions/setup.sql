-- ============================================================================
-- Delta Unicode String Functions — Multi-Byte Operations — Setup Script
-- ============================================================================
-- Demonstrates string function behavior on multi-byte UTF-8 text:
--   - LENGTH returns character count, not byte count
--   - UPPER/LOWER preserve diacritics (ü → Ü, ñ → Ñ)
--   - SUBSTR respects character boundaries
--   - LIKE matches Unicode patterns
--   - CONCAT mixes scripts safely
--
-- Tables created:
--   1. support_tickets — 20 multilingual support tickets
--
-- Operations performed:
--   1. CREATE DELTA TABLE
--   2. INSERT — 20 tickets with names/messages in 15+ scripts
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: support_tickets — multilingual customer support
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.support_tickets (
    id                INT,
    customer_name     VARCHAR,
    locale            VARCHAR,
    subject           VARCHAR,
    message_snippet   VARCHAR,
    priority          VARCHAR,
    status            VARCHAR
) LOCATION 'delta-unicode-string-functions/support_tickets';


-- STEP 2: Insert 20 tickets spanning 15+ scripts
INSERT INTO {{zone_name}}.delta_demos.support_tickets VALUES
    (1,  'Tanaka Yūki',              'ja', 'Product inquiry',        '製品について質問があります',                              'medium', 'open'),
    (2,  'Müller Hans',              'de', 'Shipping delay',         'Meine Lieferung ist verspätet',                        'high',   'open'),
    (3,  'François Dubois',          'fr', 'Return request',         'Je souhaite retourner ma crème brûlée machine',        'medium', 'open'),
    (4,  'Αλέξανδρος Νίκου',         'el', 'Warranty claim',         'Η συσκευή χάλασε μετά από 2 μήνες',                    'high',   'open'),
    (5,  'Иванова Мария',            'ru', 'Account issue',          'Не могу войти в аккаунт',                              'high',   'escalated'),
    (6,  'أحمد محمود',                'ar', 'Billing question',       'لدي استفسار عن الفاتورة',                              'low',    'open'),
    (7,  'José García',              'es', 'Feature request',        'Quisiera solicitar una función nueva',                  'low',    'open'),
    (8,  'Çelik Ayşe',              'tr', 'Order cancel',           'Siparişimi iptal etmek istiyorum',                      'medium', 'open'),
    (9,  'Søren Nielsen',            'da', 'Product feedback',       'Produktet er fremragende kvalitet',                     'low',    'closed'),
    (10, 'Козак Олена',              'uk', 'Technical support',      'Додаток не працює на моєму пристрої',                  'high',   'open'),
    (11, 'Björk Sigurðsson',         'is', 'Size exchange',          'Ég þarf að skipta um stærð',                           'medium', 'open'),
    (12, 'שרה כהן',                   'he', 'Delivery update',        'מתי ההזמנה שלי תגיע',                                   'medium', 'open'),
    (13, 'Nguyễn Thị Mai',           'vi', 'Payment failed',         'Thanh toán không thành công',                          'high',   'open'),
    (14, 'Łukasz Kowalski',          'pl', 'Missing item',           'Brakuje jednego przedmiotu',                            'high',   'open'),
    (15, 'Ólafur Jónsson',           'is', 'Compliment',             'Frábær þjónusta og gæði',                              'low',    'closed'),
    (16, '김서연',                     'ko', 'Exchange request',       '교환을 원합니다',                                        'medium', 'open'),
    (17, 'Žofia Nováková',           'sk', 'Promo code issue',       'Zľavový kód nefunguje',                                'medium', 'open'),
    (18, 'Åsa Lindström',            'sv', 'Subscription cancel',    'Jag vill avsluta prenumerationen',                      'low',    'open'),
    (19, 'محمد رضایی',                'fa', 'Product defect',         'محصول معیوب دریافت کردم',                               'high',   'escalated'),
    (20, 'Résumé Bot',               'en', 'Test ticket',            'Testing Unicode: café naïve résumé',                    'low',    'closed');
