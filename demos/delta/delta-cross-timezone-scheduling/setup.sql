-- ============================================================================
-- SETUP: Delta Cross-Timezone Scheduling — Global Conference Planner
-- ============================================================================
-- Global conference scheduler for a multinational company with 6 offices:
-- New York (EDT, UTC-4), London (BST, UTC+1), Tokyo (JST, UTC+9),
-- Sydney (AEST, UTC+10), Berlin (CEST, UTC+2), Dubai (GST, UTC+4).
--
-- Each meeting stores both local wall-clock times and UTC times, enabling
-- true chronological ordering and conflict detection across timezones.
--
-- 40 meetings inserted in 4 batches, followed by UPDATE and DELETE operations.
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';

-- ============================================================================
-- Table DDL
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.conference_schedule (
    id                  INT,
    meeting_title       VARCHAR,
    office              VARCHAR,
    timezone_label      VARCHAR,
    utc_offset_hours    INT,
    start_local         VARCHAR,
    end_local           VARCHAR,
    start_utc           VARCHAR,
    end_utc             VARCHAR,
    duration_minutes    INT,
    room                VARCHAR,
    organizer           VARCHAR,
    priority            VARCHAR
) LOCATION 'delta-cross-timezone-scheduling/conference_schedule';


-- ============================================================================
-- Batch 1: New York (ids 1-8) + London (ids 9-10)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.conference_schedule VALUES
    (1,  'Q3 Revenue Review',         'New York', 'EDT',  -4, '2025-09-15 08:00', '2025-09-15 09:00', '2025-09-15 12:00', '2025-09-15 13:00', 60,  'NY-Boardroom',  'Sarah Chen',    'normal'),
    (2,  'Product Roadmap Sync',      'New York', 'EDT',  -4, '2025-09-15 09:30', '2025-09-15 10:15', '2025-09-15 13:30', '2025-09-15 14:15', 45,  'NY-Huddle-A',   'Sarah Chen',    'normal'),
    (3,  'Client Onboarding Call',    'New York', 'EDT',  -4, '2025-09-15 10:00', '2025-09-15 11:30', '2025-09-15 14:00', '2025-09-15 15:30', 90,  'NY-Boardroom',  'Marcus Webb',   'normal'),
    (4,  'Investor Relations Brief',  'New York', 'EDT',  -4, '2025-09-15 11:00', '2025-09-15 12:00', '2025-09-15 15:00', '2025-09-15 16:00', 60,  'NY-Boardroom',  'Marcus Webb',   'normal'),
    (5,  'Engineering Standup',       'New York', 'EDT',  -4, '2025-09-15 09:00', '2025-09-15 09:30', '2025-09-15 13:00', '2025-09-15 13:30', 30,  'NY-Summit',     'Tom Bradley',   'low'),
    (6,  'Marketing Campaign Review', 'New York', 'EDT',  -4, '2025-09-15 13:00', '2025-09-15 13:45', '2025-09-15 17:00', '2025-09-15 17:45', 45,  'NY-Huddle-A',   'Sarah Chen',    'normal'),
    (7,  'Legal Compliance Update',   'New York', 'EDT',  -4, '2025-09-15 14:30', '2025-09-15 15:30', '2025-09-15 18:30', '2025-09-15 19:30', 60,  'NY-Boardroom',  'Tom Bradley',   'normal'),
    (8,  'End-of-Day Wrap-up',        'New York', 'EDT',  -4, '2025-09-15 16:00', '2025-09-15 16:30', '2025-09-15 20:00', '2025-09-15 20:30', 30,  'NY-Summit',     'Marcus Webb',   'low'),
    (9,  'EMEA Sales Pipeline',       'London',   'BST',   1, '2025-09-15 09:00', '2025-09-15 10:00', '2025-09-15 08:00', '2025-09-15 09:00', 60,  'LON-Thames',    'Elena Rossi',   'normal'),
    (10, 'Sprint Planning',           'London',   'BST',   1, '2025-09-15 10:30', '2025-09-15 12:00', '2025-09-15 09:30', '2025-09-15 11:00', 90,  'LON-Victoria',  'Oliver Grant',  'normal');

-- ============================================================================
-- Batch 2: London (ids 11-15) + Tokyo (ids 16-20)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.conference_schedule VALUES
    (11, 'Design Review Session',      'London', 'BST',   1, '2025-09-15 13:00', '2025-09-15 13:45', '2025-09-15 12:00', '2025-09-15 12:45', 45,  'LON-Thames',   'Elena Rossi',  'normal'),
    (12, 'Partner Integration Call',   'London', 'BST',   1, '2025-09-15 14:00', '2025-09-15 15:00', '2025-09-15 13:00', '2025-09-15 14:00', 60,  'LON-Canary',   'Oliver Grant', 'normal'),
    (13, 'Talent Acquisition Sync',    'London', 'BST',   1, '2025-09-15 15:30', '2025-09-15 16:00', '2025-09-15 14:30', '2025-09-15 15:00', 30,  'LON-Victoria', 'Elena Rossi',  'low'),
    (14, 'Finance Quarter Close',      'London', 'BST',   1, '2025-09-15 11:00', '2025-09-15 12:00', '2025-09-15 10:00', '2025-09-15 11:00', 60,  'LON-Canary',   'Oliver Grant', 'normal'),
    (15, 'Security Audit Briefing',    'London', 'BST',   1, '2025-09-15 16:00', '2025-09-15 16:45', '2025-09-15 15:00', '2025-09-15 15:45', 45,  'LON-Thames',   'Elena Rossi',  'normal'),
    (16, 'APAC Strategy Kickoff',      'Tokyo',  'JST',   9, '2025-09-15 09:00', '2025-09-15 10:00', '2025-09-15 00:00', '2025-09-15 01:00', 60,  'TKY-Sakura',   'Yuki Tanaka',  'normal'),
    (17, 'Supply Chain Review',        'Tokyo',  'JST',   9, '2025-09-15 10:30', '2025-09-15 11:15', '2025-09-15 01:30', '2025-09-15 02:15', 45,  'TKY-Fuji',     'Hana Kimura',  'normal'),
    (18, 'Firmware Release Go/No-Go',  'Tokyo',  'JST',   9, '2025-09-15 13:00', '2025-09-15 14:30', '2025-09-15 04:00', '2025-09-15 05:30', 90,  'TKY-Zen',      'Yuki Tanaka',  'normal'),
    (19, 'QA Test Results Review',     'Tokyo',  'JST',   9, '2025-09-15 14:00', '2025-09-15 15:00', '2025-09-15 05:00', '2025-09-15 06:00', 60,  'TKY-Zen',      'Hana Kimura',  'normal'),
    (20, 'Customer Success Debrief',   'Tokyo',  'JST',   9, '2025-09-15 15:30', '2025-09-15 16:00', '2025-09-15 06:30', '2025-09-15 07:00', 30,  'TKY-Sakura',   'Yuki Tanaka',  'low');

-- ============================================================================
-- Batch 3: Tokyo (ids 21-22) + Sydney (ids 23-28) + Berlin (ids 29-30)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.conference_schedule VALUES
    (21, 'Vendor Negotiation Prep',      'Tokyo',  'JST',   9, '2025-09-15 11:30', '2025-09-15 12:15', '2025-09-15 02:30', '2025-09-15 03:15', 45,  'TKY-Sakura',      'Hana Kimura',  'normal'),
    (22, 'Tokyo All-Hands',              'Tokyo',  'JST',   9, '2025-09-15 16:30', '2025-09-15 17:30', '2025-09-15 07:30', '2025-09-15 08:30', 60,  'TKY-Fuji',        'Yuki Tanaka',  'normal'),
    (23, 'Board Presentation Dry Run',   'Sydney', 'AEST', 10, '2025-09-15 08:00', '2025-09-15 10:00', '2025-09-14 22:00', '2025-09-15 00:00', 120, 'SYD-Harbour',     'Priya Sharma', 'normal'),
    (24, 'Infrastructure Capacity Plan', 'Sydney', 'AEST', 10, '2025-09-15 10:30', '2025-09-15 11:30', '2025-09-15 00:30', '2025-09-15 01:30', 60,  'SYD-Opera',       'Lisa Park',    'normal'),
    (25, 'Data Migration Status',        'Sydney', 'AEST', 10, '2025-09-15 12:00', '2025-09-15 12:45', '2025-09-15 02:00', '2025-09-15 02:45', 45,  'SYD-Reef',        'Priya Sharma', 'normal'),
    (26, 'Regional Compliance Check',    'Sydney', 'AEST', 10, '2025-09-15 13:30', '2025-09-15 14:00', '2025-09-15 03:30', '2025-09-15 04:00', 30,  'SYD-Harbour',     'Lisa Park',    'normal'),
    (27, 'UX Research Findings',         'Sydney', 'AEST', 10, '2025-09-15 14:30', '2025-09-15 15:30', '2025-09-15 04:30', '2025-09-15 05:30', 60,  'SYD-Opera',       'Priya Sharma', 'normal'),
    (28, 'Incident Postmortem',          'Sydney', 'AEST', 10, '2025-09-15 16:00', '2025-09-15 16:45', '2025-09-15 06:00', '2025-09-15 06:45', 45,  'SYD-Reef',        'Lisa Park',    'low'),
    (29, 'EU Regulatory Update',         'Berlin', 'CEST',  2, '2025-09-15 09:00', '2025-09-15 10:00', '2025-09-15 07:00', '2025-09-15 08:00', 60,  'BER-Brandenburg', 'James Walker', 'normal'),
    (30, 'DevOps Pipeline Review',       'Berlin', 'CEST',  2, '2025-09-15 10:30', '2025-09-15 11:15', '2025-09-15 08:30', '2025-09-15 09:15', 45,  'BER-Spree',       'James Walker', 'normal');

-- ============================================================================
-- Batch 4: Berlin (ids 31-34) + Dubai (ids 35-40)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.conference_schedule VALUES
    (31, 'Cross-Team Architecture',     'Berlin', 'CEST',  2, '2025-09-15 12:00', '2025-09-15 13:30', '2025-09-15 10:00', '2025-09-15 11:30', 90,  'BER-Brandenburg', 'Ahmed Hassan',  'normal'),
    (32, 'Sustainability Report Draft', 'Berlin', 'CEST',  2, '2025-09-15 14:00', '2025-09-15 15:00', '2025-09-15 12:00', '2025-09-15 13:00', 60,  'BER-Spree',       'James Walker',  'normal'),
    (33, 'Hiring Committee',            'Berlin', 'CEST',  2, '2025-09-15 15:30', '2025-09-15 16:00', '2025-09-15 13:30', '2025-09-15 14:00', 30,  'BER-Brandenburg', 'Ahmed Hassan',  'low'),
    (34, 'R&D Innovation Showcase',     'Berlin', 'CEST',  2, '2025-09-15 16:30', '2025-09-15 17:15', '2025-09-15 14:30', '2025-09-15 15:15', 45,  'BER-Spree',       'Ahmed Hassan',  'normal'),
    (35, 'Middle East Expansion Plan',  'Dubai',  'GST',   4, '2025-09-15 09:00', '2025-09-15 10:00', '2025-09-15 05:00', '2025-09-15 06:00', 60,  'DXB-Burj',        'Fatima Al-Said','normal'),
    (36, 'Procurement Review',          'Dubai',  'GST',   4, '2025-09-15 10:30', '2025-09-15 11:15', '2025-09-15 06:30', '2025-09-15 07:15', 45,  'DXB-Marina',      'Ahmed Hassan',  'normal'),
    (37, 'VIP Client Engagement',       'Dubai',  'GST',   4, '2025-09-15 12:00', '2025-09-15 13:30', '2025-09-15 08:00', '2025-09-15 09:30', 90,  'DXB-Burj',        'Fatima Al-Said','normal'),
    (38, 'Logistics Coordination',      'Dubai',  'GST',   4, '2025-09-15 14:00', '2025-09-15 14:30', '2025-09-15 10:00', '2025-09-15 10:30', 30,  'DXB-Marina',      'Ahmed Hassan',  'normal'),
    (39, 'Risk Assessment Workshop',    'Dubai',  'GST',   4, '2025-09-15 15:00', '2025-09-15 16:00', '2025-09-15 11:00', '2025-09-15 12:00', 60,  'DXB-Burj',        'Fatima Al-Said','normal'),
    (40, 'Evening Social Planning',     'Dubai',  'GST',   4, '2025-09-15 16:30', '2025-09-15 17:15', '2025-09-15 12:30', '2025-09-15 13:15', 45,  'DXB-Marina',      'Ahmed Hassan',  'low');

-- ============================================================================
-- DML: Priority upgrades
-- ============================================================================

UPDATE {{zone_name}}.delta_demos.conference_schedule SET priority = 'high' WHERE id = 3;

UPDATE {{zone_name}}.delta_demos.conference_schedule SET priority = 'high' WHERE id = 18;

UPDATE {{zone_name}}.delta_demos.conference_schedule SET priority = 'high' WHERE id = 35;

-- ============================================================================
-- DML: Room reassignments
-- ============================================================================

UPDATE {{zone_name}}.delta_demos.conference_schedule SET room = 'NY-Summit' WHERE id = 6;

UPDATE {{zone_name}}.delta_demos.conference_schedule SET room = 'BER-Brandenburg' WHERE id = 30;

-- ============================================================================
-- DML: Cancelled meeting
-- ============================================================================

DELETE FROM {{zone_name}}.delta_demos.conference_schedule WHERE id = 40;
