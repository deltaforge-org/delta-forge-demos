-- ============================================================================
-- Delta Row Tracking — Stable Row IDs & Audit Trail — Setup Script
-- ============================================================================
-- Demonstrates row-level tracking using explicit audit columns that simulate
-- Delta's row tracking feature at the protocol level. A financial compliance
-- system records every auditable action with before/after values and version
-- tags for lineage.
--
-- Tables created:
--   1. compliance_audit — 50 final rows
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE
--   3. INSERT — 30 initial audit entries (action='create', version_tag=1)
--   5. INSERT — 10 update actions (action='update', version_tag=2)
--   6. INSERT — 5 review actions (action='review', version_tag=1)
--   7. INSERT — 5 delete actions (action='delete', version_tag=1)
--   8. UPDATE — increment version_tag for 8 re-reviewed entries
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: compliance_audit — Financial compliance audit trail
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.compliance_audit (
    id                INT,
    record_id         VARCHAR,
    entity_type       VARCHAR,
    entity_name       VARCHAR,
    action            VARCHAR,
    old_value         VARCHAR,
    new_value         VARCHAR,
    actor             VARCHAR,
    version_tag       INT,
    audit_timestamp   VARCHAR
) LOCATION 'delta-row-tracking/compliance_audit';


-- ============================================================================
-- STEP 3: INSERT 30 initial audit entries — action='create', version_tag=1
-- ============================================================================
-- Entity types: account, transaction, customer, policy (4 types)
-- record_ids: REC-001 through REC-030
INSERT INTO {{zone_name}}.delta_demos.compliance_audit VALUES
    (1,  'REC-001', 'account',     'Acme Corp Checking',       'create', NULL, 'balance=50000.00',       'sys_admin',    1, '2024-01-02 08:15:00'),
    (2,  'REC-002', 'account',     'Globex Savings',           'create', NULL, 'balance=125000.00',      'sys_admin',    1, '2024-01-02 08:16:00'),
    (3,  'REC-003', 'transaction', 'Wire Transfer #4401',      'create', NULL, 'amount=25000.00',        'ops_clerk',    1, '2024-01-03 09:00:00'),
    (4,  'REC-004', 'customer',    'John Martinez',            'create', NULL, 'tier=standard',          'onboarding',   1, '2024-01-03 09:30:00'),
    (5,  'REC-005', 'policy',      'AML Policy v2.1',          'create', NULL, 'status=active',          'compliance',   1, '2024-01-04 10:00:00'),
    (6,  'REC-006', 'account',     'Initech Operating',        'create', NULL, 'balance=78000.00',       'sys_admin',    1, '2024-01-04 10:15:00'),
    (7,  'REC-007', 'transaction', 'ACH Batch #7720',          'create', NULL, 'amount=12500.00',        'ops_clerk',    1, '2024-01-05 11:00:00'),
    (8,  'REC-008', 'customer',    'Sarah Chen',               'create', NULL, 'tier=premium',           'onboarding',   1, '2024-01-05 11:30:00'),
    (9,  'REC-009', 'policy',      'KYC Policy v3.0',          'create', NULL, 'status=active',          'compliance',   1, '2024-01-06 08:00:00'),
    (10, 'REC-010', 'account',     'Wayne Enterprises Trust',  'create', NULL, 'balance=500000.00',      'sys_admin',    1, '2024-01-06 08:15:00'),
    (11, 'REC-011', 'transaction', 'Check Deposit #1190',      'create', NULL, 'amount=8400.00',         'ops_clerk',    1, '2024-01-07 09:00:00'),
    (12, 'REC-012', 'customer',    'Maria Lopez',              'create', NULL, 'tier=standard',          'onboarding',   1, '2024-01-07 09:30:00'),
    (13, 'REC-013', 'policy',      'Fraud Detection v1.5',     'create', NULL, 'status=draft',           'compliance',   1, '2024-01-08 10:00:00'),
    (14, 'REC-014', 'account',     'Stark Industries Reserve', 'create', NULL, 'balance=250000.00',      'sys_admin',    1, '2024-01-08 10:15:00'),
    (15, 'REC-015', 'transaction', 'FX Swap #3305',            'create', NULL, 'amount=150000.00',       'ops_clerk',    1, '2024-01-09 11:00:00'),
    (16, 'REC-016', 'customer',    'David Kim',                'create', NULL, 'tier=standard',          'onboarding',   1, '2024-01-09 11:30:00'),
    (17, 'REC-017', 'policy',      'Data Retention v4.0',      'create', NULL, 'status=active',          'compliance',   1, '2024-01-10 08:00:00'),
    (18, 'REC-018', 'account',     'Umbrella Corp Escrow',     'create', NULL, 'balance=92000.00',       'sys_admin',    1, '2024-01-10 08:15:00'),
    (19, 'REC-019', 'transaction', 'Wire Transfer #4402',      'create', NULL, 'amount=35000.00',        'ops_clerk',    1, '2024-01-11 09:00:00'),
    (20, 'REC-020', 'customer',    'Emily Nakamura',           'create', NULL, 'tier=premium',           'onboarding',   1, '2024-01-11 09:30:00'),
    (21, 'REC-021', 'policy',      'Sanctions Screening v2.0', 'create', NULL, 'status=active',          'compliance',   1, '2024-01-12 10:00:00'),
    (22, 'REC-022', 'account',     'Cyberdyne Payroll',        'create', NULL, 'balance=61000.00',       'sys_admin',    1, '2024-01-12 10:15:00'),
    (23, 'REC-023', 'transaction', 'ACH Batch #7721',          'create', NULL, 'amount=19000.00',        'ops_clerk',    1, '2024-01-13 11:00:00'),
    (24, 'REC-024', 'customer',    'James O''Brien',           'create', NULL, 'tier=standard',          'onboarding',   1, '2024-01-13 11:30:00'),
    (25, 'REC-025', 'policy',      'Transaction Limits v1.2',  'create', NULL, 'status=draft',           'compliance',   1, '2024-01-14 08:00:00'),
    (26, 'REC-026', 'account',     'Oscorp Trading',           'create', NULL, 'balance=175000.00',      'sys_admin',    1, '2024-01-14 08:15:00'),
    (27, 'REC-027', 'transaction', 'Check Deposit #1191',      'create', NULL, 'amount=5600.00',         'ops_clerk',    1, '2024-01-15 09:00:00'),
    (28, 'REC-028', 'customer',    'Aisha Patel',              'create', NULL, 'tier=premium',           'onboarding',   1, '2024-01-15 09:30:00'),
    (29, 'REC-029', 'policy',      'Whistleblower v1.0',       'create', NULL, 'status=active',          'compliance',   1, '2024-01-16 10:00:00'),
    (30, 'REC-030', 'transaction', 'FX Swap #3306',            'create', NULL, 'amount=88000.00',        'ops_clerk',    1, '2024-01-16 11:00:00');


-- ============================================================================
-- STEP 5: INSERT 10 update actions — action='update', version_tag=2
-- ============================================================================
-- Updates reference existing record_ids REC-001 through REC-010
-- Each captures old_value and new_value showing what changed
INSERT INTO {{zone_name}}.delta_demos.compliance_audit VALUES
    (31, 'REC-001', 'account',     'Acme Corp Checking',       'update', 'balance=50000.00',   'balance=52500.00',   'ops_clerk',    2, '2024-02-01 08:30:00'),
    (32, 'REC-002', 'account',     'Globex Savings',           'update', 'balance=125000.00',  'balance=131250.00',  'ops_clerk',    2, '2024-02-01 08:45:00'),
    (33, 'REC-003', 'transaction', 'Wire Transfer #4401',      'update', 'status=pending',     'status=completed',   'ops_clerk',    2, '2024-02-02 09:15:00'),
    (34, 'REC-004', 'customer',    'John Martinez',            'update', 'tier=standard',      'tier=premium',       'onboarding',   2, '2024-02-02 09:45:00'),
    (35, 'REC-005', 'policy',      'AML Policy v2.1',          'update', 'status=active',      'status=superseded',  'compliance',   2, '2024-02-03 10:30:00'),
    (36, 'REC-006', 'account',     'Initech Operating',        'update', 'balance=78000.00',   'balance=74100.00',   'ops_clerk',    2, '2024-02-03 10:45:00'),
    (37, 'REC-007', 'transaction', 'ACH Batch #7720',          'update', 'status=pending',     'status=settled',     'ops_clerk',    2, '2024-02-04 11:15:00'),
    (38, 'REC-008', 'customer',    'Sarah Chen',               'update', 'tier=premium',       'tier=vip',           'onboarding',   2, '2024-02-04 11:45:00'),
    (39, 'REC-009', 'policy',      'KYC Policy v3.0',          'update', 'version=3.0',        'version=3.1',        'compliance',   2, '2024-02-05 08:30:00'),
    (40, 'REC-010', 'account',     'Wayne Enterprises Trust',  'update', 'balance=500000.00',  'balance=525000.00',  'sys_admin',    2, '2024-02-05 08:45:00');


-- ============================================================================
-- STEP 6: INSERT 5 review actions — action='review', version_tag=1
-- ============================================================================
-- Reviews reference existing record_ids REC-011 through REC-015
INSERT INTO {{zone_name}}.delta_demos.compliance_audit VALUES
    (41, 'REC-011', 'transaction', 'Check Deposit #1190',      'review', NULL, 'approved',       'auditor_1',    1, '2024-02-10 14:00:00'),
    (42, 'REC-012', 'customer',    'Maria Lopez',              'review', NULL, 'verified',       'auditor_1',    1, '2024-02-10 14:15:00'),
    (43, 'REC-013', 'policy',      'Fraud Detection v1.5',     'review', NULL, 'needs_revision', 'auditor_2',    1, '2024-02-10 14:30:00'),
    (44, 'REC-014', 'account',     'Stark Industries Reserve', 'review', NULL, 'approved',       'auditor_2',    1, '2024-02-10 14:45:00'),
    (45, 'REC-015', 'transaction', 'FX Swap #3305',            'review', NULL, 'flagged',        'auditor_1',    1, '2024-02-10 15:00:00');


-- ============================================================================
-- STEP 7: INSERT 5 delete actions — action='delete', version_tag=1
-- ============================================================================
-- Deletes reference existing record_ids REC-016 through REC-020
INSERT INTO {{zone_name}}.delta_demos.compliance_audit VALUES
    (46, 'REC-016', 'customer',    'David Kim',                'delete', 'tier=standard',      NULL, 'sys_admin',    1, '2024-03-01 09:00:00'),
    (47, 'REC-017', 'policy',      'Data Retention v4.0',      'delete', 'status=active',      NULL, 'compliance',   1, '2024-03-01 09:15:00'),
    (48, 'REC-018', 'account',     'Umbrella Corp Escrow',     'delete', 'balance=92000.00',   NULL, 'sys_admin',    1, '2024-03-01 09:30:00'),
    (49, 'REC-019', 'transaction', 'Wire Transfer #4402',      'delete', 'amount=35000.00',    NULL, 'ops_clerk',    1, '2024-03-01 09:45:00'),
    (50, 'REC-020', 'customer',    'Emily Nakamura',           'delete', 'tier=premium',       NULL, 'sys_admin',    1, '2024-03-01 10:00:00');


-- ============================================================================
-- STEP 8: UPDATE — increment version_tag for 8 re-reviewed entries
-- ============================================================================
-- Re-review bumps version_tag from 1 to 2 for 8 rows (ids: 41-45 reviews + ids: 46-48 deletes)
UPDATE {{zone_name}}.delta_demos.compliance_audit
SET version_tag = 2
WHERE id IN (41, 42, 43, 44, 45, 46, 47, 48);
