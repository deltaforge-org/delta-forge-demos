-- ============================================================================
-- Delta Complex Types — Structs, Arrays & Maps — Setup Script
-- ============================================================================
-- Demonstrates complex data modelling patterns in Delta tables:
--   - Flat columns representing struct fields (address_*)
--   - Comma-delimited strings representing arrays (skills)
--   - Key=value strings representing map entries (metadata)
--
-- Tables created:
--   1. employees — 40 staff across 5 departments with structured data
--
-- Operations performed:
--   1. CREATE DELTA TABLE with 14 columns
--   2. INSERT INTO VALUES — 30 employees with full structured data
--   3. UPDATE — salary increase for Engineering department
--   4. INSERT — 10 more employees with different skill sets
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: employees — 40 staff with struct-like, array-like, map-like columns
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.employees (
    id               INT,
    name             VARCHAR,
    department       VARCHAR,
    salary           DOUBLE,
    address_street   VARCHAR,
    address_city     VARCHAR,
    address_state    VARCHAR,
    address_zip      VARCHAR,
    skills           VARCHAR,
    metadata         VARCHAR,
    hire_date        VARCHAR,
    is_active        BOOLEAN,
    manager_id       INT,
    level            VARCHAR
) LOCATION 'delta-complex-types/employees';


-- STEP 2: Insert 30 employees across 5 departments
INSERT INTO {{zone_name}}.delta_demos.employees VALUES
    (1,  'Alice Chen',       'Engineering', 125000.00, '100 Tech Blvd',      'San Jose',      'CA', '95110', 'python,java,sql',         'team=backend,role=senior',    '2020-03-15', true,  NULL, 'L5'),
    (2,  'Bob Martinez',     'Engineering', 115000.00, '200 Code Ave',       'San Francisco', 'CA', '94105', 'javascript,react,nodejs',  'team=frontend,role=mid',      '2021-06-01', true,  1,    'L4'),
    (3,  'Carol Williams',   'Engineering', 135000.00, '300 Dev Lane',       'Seattle',       'WA', '98101', 'rust,go,kubernetes',       'team=infra,role=lead',        '2019-01-10', true,  NULL, 'L6'),
    (4,  'David Kim',        'Engineering', 105000.00, '400 Stack St',       'Portland',      'OR', '97201', 'python,ml,tensorflow',     'team=ml,role=junior',         '2022-09-01', true,  3,    'L3'),
    (5,  'Eva Johnson',      'Engineering', 120000.00, '500 Algo Way',       'Austin',        'TX', '73301', 'java,spring,microservices','team=backend,role=mid',       '2021-02-15', true,  1,    'L4'),
    (6,  'Frank Lopez',      'Engineering', 110000.00, '600 Byte Rd',        'Denver',        'CO', '80201', 'python,django,postgresql', 'team=backend,role=mid',       '2021-11-20', true,  1,    'L4'),
    (7,  'Grace Park',       'Sales',       95000.00,  '700 Commerce Dr',    'New York',      'NY', '10001', 'salesforce,negotiation',   'region=east,role=account',    '2020-05-10', true,  NULL, 'L4'),
    (8,  'Henry Brown',      'Sales',       85000.00,  '800 Market Blvd',    'Chicago',       'IL', '60601', 'hubspot,cold-calling',     'region=midwest,role=sdr',     '2022-01-15', true,  7,    'L3'),
    (9,  'Irene Davis',      'Sales',       105000.00, '900 Deal Ave',       'Boston',        'MA', '02101', 'salesforce,analytics,sql', 'region=east,role=manager',    '2019-08-01', true,  NULL, 'L5'),
    (10, 'Jack Wilson',      'Sales',       78000.00,  '1000 Prospect St',   'Miami',         'FL', '33101', 'hubspot,email-marketing',  'region=south,role=sdr',       '2023-03-01', true,  9,    'L2'),
    (11, 'Karen Miller',     'Sales',       92000.00,  '1100 Revenue Rd',    'Atlanta',       'GA', '30301', 'salesforce,presentations', 'region=south,role=account',   '2021-04-15', true,  9,    'L3'),
    (12, 'Leo Zhang',        'Marketing',   88000.00,  '1200 Brand Ave',     'Los Angeles',   'CA', '90001', 'seo,google-ads,analytics', 'channel=digital,role=mid',    '2021-07-01', true,  NULL, 'L4'),
    (13, 'Maria Garcia',     'Marketing',   92000.00,  '1300 Content Blvd',  'San Diego',     'CA', '92101', 'copywriting,social-media', 'channel=content,role=mid',    '2020-11-15', true,  12,   'L4'),
    (14, 'Nick Patel',       'Marketing',   78000.00,  '1400 Campaign St',   'Phoenix',       'AZ', '85001', 'email-marketing,mailchimp','channel=email,role=junior',   '2022-06-01', true,  12,   'L3'),
    (15, 'Olivia Taylor',    'Marketing',   98000.00,  '1500 Media Way',     'Dallas',        'TX', '75201', 'analytics,tableau,sql',    'channel=analytics,role=lead', '2019-09-10', true,  NULL, 'L5'),
    (16, 'Peter Adams',      'HR',          82000.00,  '1600 People Dr',     'Nashville',     'TN', '37201', 'recruiting,interviewing',  'focus=talent,role=recruiter', '2021-03-01', true,  NULL, 'L3'),
    (17, 'Quinn Roberts',    'HR',          95000.00,  '1700 Culture Ave',   'Charlotte',     'NC', '28201', 'compensation,benefits,hris','focus=comp,role=manager',    '2019-06-15', true,  NULL, 'L5'),
    (18, 'Rachel Lee',       'HR',          75000.00,  '1800 Talent Blvd',   'Raleigh',       'NC', '27601', 'onboarding,training',      'focus=learning,role=coord',   '2022-08-01', true,  17,   'L2'),
    (19, 'Sam Thompson',     'Finance',     105000.00, '1900 Ledger St',     'Hartford',      'CT', '06101', 'excel,sql,sap',            'dept=accounting,role=senior', '2020-01-10', true,  NULL, 'L5'),
    (20, 'Tina Anderson',    'Finance',     95000.00,  '2000 Budget Ave',    'Stamford',      'CT', '06901', 'forecasting,excel,python', 'dept=fp-a,role=analyst',      '2021-05-01', true,  19,   'L4'),
    (21, 'Uma Krishnan',     'Finance',     88000.00,  '2100 Audit Way',     'New Haven',     'CT', '06510', 'auditing,compliance,sox',  'dept=audit,role=mid',         '2021-10-15', true,  19,   'L4'),
    (22, 'Victor Nguyen',    'Finance',     72000.00,  '2200 Tax Blvd',      'Bridgeport',    'CT', '06601', 'tax-prep,quickbooks',      'dept=tax,role=junior',        '2023-01-10', true,  19,   'L2'),
    (23, 'Wendy Clark',      'Engineering', 130000.00, '2300 Cloud St',      'Seattle',       'WA', '98102', 'aws,terraform,docker',     'team=devops,role=senior',     '2019-04-01', true,  3,    'L5'),
    (24, 'Xavier Reed',      'Sales',       88000.00,  '2400 Pipeline Rd',   'Houston',       'TX', '77001', 'salesforce,crm-admin',     'region=south,role=ops',       '2022-02-15', true,  9,    'L3'),
    (25, 'Yuki Tanaka',      'Engineering', 118000.00, '2500 Data Dr',       'San Jose',      'CA', '95112', 'sql,spark,delta-lake',     'team=data,role=mid',          '2021-08-01', true,  3,    'L4'),
    (26, 'Zara Hussein',     'Marketing',   85000.00,  '2600 Design Ave',    'Portland',      'OR', '97202', 'figma,photoshop,branding', 'channel=design,role=mid',     '2022-03-01', true,  12,   'L3'),
    (27, 'Aaron Scott',      'HR',          68000.00,  '2700 Payroll St',    'Columbus',      'OH', '43201', 'payroll,adp,compliance',   'focus=payroll,role=coord',    '2023-02-01', true,  17,   'L2'),
    (28, 'Beth Morgan',      'Finance',     110000.00, '2800 Capital Blvd',  'New York',      'NY', '10002', 'treasury,risk,bloomberg',  'dept=treasury,role=senior',   '2018-11-01', true,  NULL, 'L5'),
    (29, 'Chris Turner',     'Sales',       82000.00,  '2900 Close Ave',     'Philadelphia',  'PA', '19101', 'demos,presentations',      'region=east,role=se',         '2022-07-01', true,  7,    'L3'),
    (30, 'Diana Foster',     'Engineering', 140000.00, '3000 Arch Way',      'San Francisco', 'CA', '94107', 'system-design,mentoring',  'team=platform,role=principal','2017-06-01', true,  NULL, 'L7');


-- ============================================================================
-- STEP 3: UPDATE — 15% salary increase for Engineering department
-- ============================================================================
-- Engineering employees: ids 1-6, 23, 25, 30
UPDATE {{zone_name}}.delta_demos.employees
SET salary = ROUND(salary * 1.15, 2)
WHERE department = 'Engineering';


-- ============================================================================
-- STEP 4: INSERT — 10 more employees with diverse skill sets
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.employees
SELECT * FROM (VALUES
    (31, 'Edward Price',     'Engineering', 108000.00, '3100 API Rd',        'Austin',        'TX', '73302', 'graphql,typescript,aws',   'team=api,role=mid',           '2022-04-01', true,  1,    'L4'),
    (32, 'Fiona Campbell',   'Sales',       96000.00,  '3200 Quota Blvd',    'San Diego',     'CA', '92102', 'salesforce,sql,analytics', 'region=west,role=account',    '2020-10-15', true,  7,    'L4'),
    (33, 'George White',     'Marketing',   80000.00,  '3300 Viral Ave',     'Nashville',     'TN', '37202', 'tiktok,youtube,video-edit','channel=social,role=junior',  '2023-04-01', true,  12,   'L2'),
    (34, 'Hannah Brooks',    'HR',          90000.00,  '3400 Policy St',     'Denver',        'CO', '80202', 'dei,policy,employment-law','focus=dei,role=specialist',   '2020-12-01', true,  17,   'L4'),
    (35, 'Ian Cooper',       'Finance',     98000.00,  '3500 Margin Way',    'Chicago',       'IL', '60602', 'financial-modeling,python','dept=fp-a,role=senior',       '2019-07-01', true,  28,   'L5'),
    (36, 'Julia Stewart',    'Engineering', 112000.00, '3600 Debug Ln',      'Portland',      'OR', '97203', 'python,pytest,ci-cd',      'team=qa,role=mid',            '2021-09-01', true,  3,    'L4'),
    (37, 'Kyle Bennett',     'Sales',       74000.00,  '3700 Lead Ave',      'Tampa',         'FL', '33601', 'outreach,linkedin-sales',  'region=south,role=bdr',       '2023-06-01', true,  9,    'L2'),
    (38, 'Laura Ramirez',    'Marketing',   86000.00,  '3800 Funnel Blvd',   'Seattle',       'WA', '98103', 'ppc,google-ads,meta-ads',  'channel=paid,role=mid',       '2022-01-15', true,  15,   'L3'),
    (39, 'Mike Sullivan',    'HR',          105000.00, '3900 Benefits Dr',   'Boston',        'MA', '02102', 'hris,workday,analytics',   'focus=hris,role=lead',        '2018-09-01', true,  NULL, 'L5'),
    (40, 'Nina Hoffman',     'Finance',     82000.00,  '4000 Invoice St',    'Minneapolis',   'MN', '55401', 'accounts-payable,netsuite','dept=ap,role=mid',            '2022-05-01', true,  19,   'L3')
) AS t(id, name, department, salary, address_street, address_city, address_state, address_zip, skills, metadata, hire_date, is_active, manager_id, level);
