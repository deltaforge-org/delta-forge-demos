-- ============================================================================
-- Delta UPDATE String Cleansing — CRM Data Normalization — Setup Script
-- ============================================================================
-- Demonstrates string functions in UPDATE SET clauses: TRIM, LOWER, LPAD,
-- UPPER, and concatenation (||) to clean messy data imported from a legacy
-- CRM system. Five UPDATE passes normalize 25 customer records in place.
--
-- Tables created:
--   1. customer_imports — 25 rows of messy CRM data
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE
--   3. INSERT — 25 customer records with realistic quality issues
--   4. UPDATE — TRIM leading/trailing whitespace from names
--   5. UPDATE — LOWER all email addresses
--   6. UPDATE — LPAD account codes to 6 digits with '0'
--   7. UPDATE — Concatenate first + last name into full_name
--   8. UPDATE — UPPER country codes
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: customer_imports — Legacy CRM data with quality issues
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customer_imports (
    id             INT,
    first_name     VARCHAR,
    last_name      VARCHAR,
    full_name      VARCHAR,
    email          VARCHAR,
    account_code   VARCHAR,
    phone          VARCHAR,
    city           VARCHAR,
    country_code   VARCHAR
) LOCATION 'delta-update-string-cleansing/customer_imports';


-- ============================================================================
-- VERSION 1: Raw import — 25 rows of messy CRM data
-- ============================================================================
-- Data issues:
--   - first_name / last_name have leading/trailing whitespace
--   - email has mixed case (e.g. 'Alice.Johnson@GMAIL.COM')
--   - account_code is unpadded (e.g. '42', '7', '1')
--   - full_name is empty string (needs concatenation)
--   - country_code is lowercase (e.g. 'us', 'uk', 'de')
INSERT INTO {{zone_name}}.delta_demos.customer_imports VALUES
    (1,  '  Alice  ',  'Johnson ',  '', 'Alice.Johnson@GMAIL.COM',    '42',    '555-0101', 'New York',      'us'),
    (2,  'Bob',        '  Smith',   '', 'BOB.SMITH@yahoo.com',        '7',     '555-0102', 'Los Angeles',   'us'),
    (3,  ' Charlie ',  'Williams',  '', 'charlie.W@OUTLOOK.COM',      '123',   '555-0103', 'Chicago',       'uk'),
    (4,  'Diana',      'Brown  ',   '', 'DIANA.BROWN@Hotmail.com',    '5',     '555-0104', 'Houston',       'de'),
    (5,  '  Edward',   ' Davis ',   '', 'Edward.Davis@Gmail.COM',     '891',   '555-0105', 'Phoenix',       'us'),
    (6,  'Fiona  ',    'Miller',    '', 'FIONA.miller@YAHOO.COM',     '23',    '555-0106', 'Philadelphia',  'uk'),
    (7,  '  George ',  '  Wilson ', '', 'george.wilson@Outlook.com',  '4567',  '555-0107', 'San Antonio',   'ca'),
    (8,  'Hannah',     'Moore',     '', 'Hannah.MOORE@gmail.com',     '89',    '555-0108', 'San Diego',     'us'),
    (9,  ' Ivan  ',    'Taylor ',   '', 'IVAN.TAYLOR@YAHOO.COM',      '1',     '555-0109', 'Dallas',        'de'),
    (10, 'Julia',      '  Anderson','', 'julia.anderson@Gmail.Com',   '345',   '555-0110', 'San Jose',      'us'),
    (11, '  Kevin ',   'Thomas',    '', 'KEVIN.Thomas@outlook.COM',   '67',    '555-0111', 'Austin',        'uk'),
    (12, 'Laura',      ' Jackson ', '', 'Laura.JACKSON@Hotmail.com',  '8',     '555-0112', 'Jacksonville',  'us'),
    (13, ' Michael ',  'White  ',   '', 'michael.white@GMAIL.COM',    '2345',  '555-0113', 'Fort Worth',    'ca'),
    (14, 'Nancy',      'Harris',    '', 'NANCY.HARRIS@Yahoo.com',     '56',    '555-0114', 'Columbus',      'de'),
    (15, '  Oscar  ',  ' Martin ',  '', 'Oscar.Martin@OUTLOOK.com',   '9',     '555-0115', 'Charlotte',     'us'),
    (16, 'Patricia',   'Garcia ',   '', 'PATRICIA.garcia@gmail.COM',  '1234',  '555-0116', 'Indianapolis',  'uk'),
    (17, ' Quinn ',    'Martinez',  '', 'quinn.Martinez@Yahoo.COM',   '78',    '555-0117', 'San Francisco', 'us'),
    (18, 'Rachel',     '  Lopez ',  '', 'RACHEL.LOPEZ@outlook.com',   '3',     '555-0118', 'Seattle',       'ca'),
    (19, '  Sam  ',    'Lee',       '', 'Sam.LEE@Gmail.com',          '456',   '555-0119', 'Denver',        'us'),
    (20, 'Tina',       ' Clark  ',  '', 'tina.clark@YAHOO.COM',       '12',    '555-0120', 'Nashville',     'de'),
    (21, ' Uma ',      'Robinson',  '', 'UMA.Robinson@Hotmail.COM',   '6789',  '555-0121', 'Oklahoma City', 'uk'),
    (22, 'Victor',     'Hall  ',    '', 'victor.HALL@gmail.com',       '34',    '555-0122', 'Portland',      'us'),
    (23, '  Wendy ',   ' Young ',   '', 'WENDY.young@Outlook.Com',    '567',   '555-0123', 'Las Vegas',     'ca'),
    (24, 'Xavier',     'King',      '', 'Xavier.KING@yahoo.COM',      '2',     '555-0124', 'Memphis',       'us'),
    (25, ' Yara  ',    'Wright ',   '', 'yara.wright@GMAIL.COM',      '90',    '555-0125', 'Louisville',    'de');


-- ============================================================================
-- VERSION 2: TRIM — Remove leading/trailing whitespace from names
-- ============================================================================
-- Names like '  Alice  ' and 'Johnson ' become 'Alice' and 'Johnson'.
-- TRIM strips both leading and trailing spaces in one pass.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.delta_demos.customer_imports
SET first_name = TRIM(first_name),
    last_name  = TRIM(last_name);


-- ============================================================================
-- VERSION 3: LOWER — Normalize all email addresses to lowercase
-- ============================================================================
-- Mixed-case emails like 'Alice.Johnson@GMAIL.COM' become
-- 'alice.johnson@gmail.com'. Critical for deduplication and lookups.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.delta_demos.customer_imports
SET email = LOWER(email);


-- ============================================================================
-- VERSION 4: LPAD — Pad account codes to 6 digits with leading zeros
-- ============================================================================
-- Unpadded codes like '42', '7', '1' become '000042', '000007', '000001'.
-- Ensures uniform formatting for downstream system integration.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.delta_demos.customer_imports
SET account_code = LPAD(account_code, 6, '0');


-- ============================================================================
-- VERSION 5: Concatenation — Build full_name from first + last
-- ============================================================================
-- The full_name column was empty on import. Now that names are trimmed,
-- we can safely concatenate: 'Alice' || ' ' || 'Johnson' = 'Alice Johnson'.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.delta_demos.customer_imports
SET full_name = first_name || ' ' || last_name;


-- ============================================================================
-- VERSION 6: UPPER — Standardize country codes to uppercase
-- ============================================================================
-- Lowercase codes like 'us', 'uk', 'de' become 'US', 'UK', 'DE'.
-- ISO 3166-1 alpha-2 codes are uppercase by convention.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.delta_demos.customer_imports
SET country_code = UPPER(country_code);
