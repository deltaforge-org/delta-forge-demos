-- ============================================================================
-- Pseudonymisation Exempt Roles & Users -- Setup Script
-- ============================================================================
-- Creates a single customers table and attaches three pseudonymisation rules,
-- each demonstrating a different exempt-list shape:
--
--   ssn   redact     EXEMPT ROLES (compliance_admin, fraud_investigator)
--   email mask       EXEMPT USERS ('{{current_user}}')
--   phone keyed_hash (no EXEMPT clause -- universal, today's default)
--
-- The fourth section runs ALTER PSEUDONYMISATION RULE ADD/REMOVE EXEMPT
-- to demonstrate mutating the lists after rule creation. Both ADD and REMOVE
-- are idempotent.
--
-- Variables (auto-injected by DeltaForge):
--   zone_name     -- Target zone name
--   data_path     -- Root path where demo data files are stored
--   current_user  -- Username of the current logged-in user
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'Demo zone for pseudonymisation exempt-roles demo';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.pseudonymisation_exempt
    COMMENT 'Schema for the pseudonymisation exempt-roles demo';


-- ============================================================================
-- STEP 2: Table -- customers
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation_exempt.customers (
    customer_id  VARCHAR,
    first_name   VARCHAR,
    last_name    VARCHAR,
    email        VARCHAR,
    phone        VARCHAR,
    ssn          VARCHAR,
    account_tier VARCHAR,
    balance      DOUBLE,
    active       BOOLEAN
) LOCATION 'pseudonymisation-exempt-roles/customers';

DELETE FROM {{zone_name}}.pseudonymisation_exempt.customers WHERE 1=1;

INSERT INTO {{zone_name}}.pseudonymisation_exempt.customers VALUES
    ('C001', 'Alice',  'Johnson',  'alice.j@email.com',   '(212)555-0101', '411-22-3344', 'Premium',  125000.50, true),
    ('C002', 'Bob',    'Martinez', 'bob.m@corp.net',      '(310)555-0202', '522-33-4455', 'Standard', 45200.75,  true),
    ('C003', 'Carol',  'Lee',      'carol.lee@bank.org',  '(312)555-0303', '633-44-5566', 'Premium',  89750.00,  true),
    ('C004', 'David',  'Kim',      'david.k@mail.com',    '(415)555-0404', '744-55-6677', 'Standard', 32100.25,  true),
    ('C005', 'Eva',    'Petrov',   'eva.p@startup.io',    '(617)555-0505', '855-66-7788', 'Premium',  210500.00, true),
    ('C006', 'Frank',  'O''Brien', 'frank.ob@retire.net', '(305)555-0606', '966-77-8899', 'Standard', 67800.50,  false);


-- ============================================================================
-- STEP 3: Rules with exempt lists
-- ============================================================================

-- Rule 1: redact SSN universally except for two named roles. Members of
--         compliance_admin or fraud_investigator read raw SSN; everyone
--         else (analysts, the demo user, every pipeline whose service
--         principal is not in those roles) reads '***-**-****'.
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_exempt.customers (ssn)
    TRANSFORM redact
    PARAMS (mask = '***-**-****')
    EXEMPT ROLES (compliance_admin, fraud_investigator);

-- Rule 2: mask email except for the current demo user. Demonstrates the
--         per-user exempt path. Quoted-string identifiers are required when
--         the value contains '@' so the lexer reads it as one token.
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_exempt.customers (email)
    TRANSFORM mask
    PARAMS (show = 5)
    EXEMPT USERS ('{{current_user}}');

-- Rule 3: keyed_hash phone, no EXEMPT clause. This rule applies to every
--         principal -- it is the historical default behaviour preserved
--         for rules that omit the EXEMPT clause.
CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_exempt.customers (phone)
    TRANSFORM keyed_hash
    SCOPE person
    PARAMS (salt = 'phone_salt_2024');


-- ============================================================================
-- STEP 4: ALTER ADD/REMOVE EXEMPT -- mutate exempt lists in place
-- ============================================================================

-- Add an additional role to rule 1's exempt list. Idempotent: re-running
-- this command on an already-exempt principal is a no-op.
ALTER PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_exempt.customers (ssn)
    ADD EXEMPT ROLE auditor;

-- Add a third role, then remove it again. The end state has compliance_admin,
-- fraud_investigator, and auditor exempt; the temporary 'data_steward' role
-- does not appear in SHOW output.
ALTER PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_exempt.customers (ssn)
    ADD EXEMPT ROLE data_steward;
ALTER PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_exempt.customers (ssn)
    REMOVE EXEMPT ROLE data_steward;

-- Add the demo user to rule 1 by user identifier. Combining role-based and
-- user-based exempts on the same rule is supported.
ALTER PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_exempt.customers (ssn)
    ADD EXEMPT USER '{{current_user}}';
