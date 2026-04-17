-- ==========================================================================
-- Demo: Regulatory Compliance Recovery — RESTORE with UniForm
-- Feature: RESTORE TO VERSION on UniForm Iceberg tables
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos COMMENT 'RESTORE with UniForm';

-- --------------------------------------------------------------------------
-- Compliance Records Table
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.compliance_records (
    record_id         INT,
    entity_name       VARCHAR,
    regulation        VARCHAR,
    compliance_status VARCHAR,
    risk_score        INT,
    review_date       DATE
) LOCATION '{{data_path}}/compliance_records'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.compliance_records TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- Seed Data — 20 compliance records across 5 entities, 4 regulations
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.iceberg_demos.compliance_records VALUES
    (1,  'Acme Corp',   'SOX',     'compliant',     15, '2025-01-10'),
    (2,  'Acme Corp',   'GDPR',    'compliant',     22, '2025-01-10'),
    (3,  'Beta Inc',    'SOX',     'non_compliant', 68, '2025-01-12'),
    (4,  'Beta Inc',    'HIPAA',   'partial',       45, '2025-01-12'),
    (5,  'Gamma LLC',   'GDPR',    'compliant',     10, '2025-01-14'),
    (6,  'Gamma LLC',   'PCI_DSS', 'compliant',     18, '2025-01-14'),
    (7,  'Delta Co',    'SOX',     'partial',       52, '2025-01-15'),
    (8,  'Delta Co',    'HIPAA',   'compliant',     25, '2025-01-15'),
    (9,  'Epsilon SA',  'GDPR',    'non_compliant', 72, '2025-01-17'),
    (10, 'Epsilon SA',  'PCI_DSS', 'partial',       41, '2025-01-17'),
    (11, 'Acme Corp',   'HIPAA',   'compliant',     20, '2025-01-18'),
    (12, 'Beta Inc',    'GDPR',    'partial',       55, '2025-01-20'),
    (13, 'Gamma LLC',   'SOX',     'compliant',     12, '2025-01-22'),
    (14, 'Delta Co',    'GDPR',    'compliant',     30, '2025-01-23'),
    (15, 'Epsilon SA',  'HIPAA',   'non_compliant', 80, '2025-01-25'),
    (16, 'Acme Corp',   'PCI_DSS', 'compliant',      8, '2025-01-26'),
    (17, 'Beta Inc',    'PCI_DSS', 'non_compliant', 65, '2025-01-28'),
    (18, 'Gamma LLC',   'HIPAA',   'compliant',     14, '2025-01-29'),
    (19, 'Delta Co',    'PCI_DSS', 'partial',       38, '2025-01-30'),
    (20, 'Epsilon SA',  'SOX',     'non_compliant', 75, '2025-02-01');
