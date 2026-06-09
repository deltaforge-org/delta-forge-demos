-- ============================================================================
-- Pseudonymisation Quickstart — Banking KYC — Setup Script
-- ============================================================================
-- Creates a single bank_customers table with 6 sample rows representing
-- retail banking KYC data, then applies 4 pseudonymisation rules to protect
-- PII columns while preserving analytics capability.
--
-- Table created:
--   1. bank_customers — Retail bank customer records with PII
--
-- Pseudonymisation rules (4 total):
--   redact       — SSN fully replaced with ***-**-****
--   mask         — Phone number partially visible (last 5 chars)
--   keyed_hash   — Last name hashed for deterministic linkage
--   generalize   — Date of birth rounded to decade
--
-- Compliance context:
--   GDPR Article 4(5)  — Pseudonymisation as a safeguard measure
--   KYC Regulations    — Customer due diligence with data protection
--
-- Variables (auto-injected by DeltaForge):
--   zone_name     — Target zone name
--   data_path     — Root path where demo data files are stored
--   current_user  — Username of the current logged-in user
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'Demo zone for pseudonymisation quickstart';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.pseudonymisation_demos
    COMMENT 'Schema for banking KYC pseudonymisation demo';


-- ============================================================================
-- STEP 2: Table — bank_customers
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation_demos.bank_customers (
    customer_id    VARCHAR,
    first_name     VARCHAR,
    last_name      VARCHAR,
    date_of_birth  DATE,
    email          VARCHAR,
    phone          VARCHAR,
    ssn            VARCHAR,
    address_line   VARCHAR,
    city           VARCHAR,
    state          VARCHAR,
    zip_code       VARCHAR,
    account_tier   VARCHAR,
    balance        DOUBLE,
    active         BOOLEAN
) LOCATION 'pseudonymisation-quickstart/bank_customers';


DELETE FROM {{zone_name}}.pseudonymisation_demos.bank_customers WHERE 1=1;

INSERT INTO {{zone_name}}.pseudonymisation_demos.bank_customers VALUES
    ('C001', 'Alice',  'Johnson',  '1985-03-15', 'alice.j@email.com',   '(212)555-0101', '411-22-3344', '100 Broadway',    'New York',      'NY', '10001', 'Premium',  125000.50, true),
    ('C002', 'Bob',    'Martinez', '1972-08-22', 'bob.m@corp.net',      '(310)555-0202', '522-33-4455', '200 Sunset Blvd', 'Los Angeles',   'CA', '90028', 'Standard', 45200.75,  true),
    ('C003', 'Carol',  'Lee',      '1990-11-30', 'carol.lee@bank.org',  '(312)555-0303', '633-44-5566', '300 Michigan Ave', 'Chicago',      'IL', '60601', 'Premium',  89750.00,  true),
    ('C004', 'David',  'Kim',      '1965-06-10', 'david.k@mail.com',    '(415)555-0404', '744-55-6677', '400 Market St',   'San Francisco', 'CA', '94105', 'Standard', 32100.25,  true),
    ('C005', 'Eva',    'Petrov',   '1998-01-25', 'eva.p@startup.io',    '(617)555-0505', '855-66-7788', '500 Beacon St',   'Boston',        'MA', '02108', 'Premium',  210500.00, true),
    ('C006', 'Frank',  'O''Brien', '1955-12-01', 'frank.ob@retire.net', '(305)555-0606', '966-77-8899', '600 Ocean Dr',    'Miami',         'FL', '33139', 'Standard', 67800.50,  false);


-- ============================================================================
-- STEP 3: Pseudonymisation Rules
-- ============================================================================

-- Rule 1: Redact SSN — fully replaced with mask string
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.bank_customers (ssn)
    TRANSFORM redact
    PARAMS (mask = '***-**-****');

-- Rule 2: Mask phone — show last 5 characters only
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.bank_customers (phone)
    TRANSFORM mask
    PARAMS (show = 5);

-- Rule 3: Keyed hash last name — deterministic pseudonym for linkage
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.bank_customers (last_name)
    TRANSFORM keyed_hash
    SCOPE person
    PARAMS (salt = 'bank_name_salt_2024');

-- Rule 4: Generalize date of birth — round to decade
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.bank_customers (date_of_birth)
    TRANSFORM generalize
    SCOPE relationship
    PARAMS (range = 10);
