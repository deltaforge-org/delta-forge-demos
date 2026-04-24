-- ============================================================================
-- Protobuf Address Book Contacts — Setup Script
-- ============================================================================
-- Creates two external tables from 3 protobuf binary files (.pb):
--   1. contacts         — flattened: one row per person, phones joined (13 rows)
--   2. contact_phones   — exploded: one row per phone number (22 rows)
--
-- Demonstrates:
--   - Proto3 binary format: schema-driven reading from .proto definitions
--   - Nested messages: Person.PhoneNumber flattened to top-level columns
--   - Repeated fields: phones as comma-joined list vs. exploded rows
--   - Enum decoding: PhoneType (0=MOBILE, 1=HOME, 2=WORK) → string labels
--   - Well-known types: google.protobuf.Timestamp → ISO 8601 datetime
--   - Sparse data: missing email, empty phone list (executives file)
--   - Multi-file reading: 3 team files merged into one table
--   - File metadata: df_file_name, df_row_number for traceability
--   - Column mappings: proto field paths → friendly column names
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.protobuf_demos
    COMMENT 'Protobuf-backed external tables';

-- ============================================================================
-- TABLE 1: contacts — One row per person (13 total)
-- ============================================================================
-- Each Person message becomes a row. Repeated PhoneNumber messages are
-- joined into a comma-separated string. The PhoneType enum is decoded to
-- its string label. Timestamps are converted to ISO 8601 format.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.protobuf_demos.contacts
USING PROTOBUF
LOCATION '{{data_path}}'
OPTIONS (
    proto_schema = '{{schema_path}}',
    message_type = 'tutorial.AddressBook',
    proto_flatten_config = '{
        "row_path": "people",
        "include_paths": [
            "people.name",
            "people.id",
            "people.email",
            "people.phones.number",
            "people.phones.type",
            "people.last_updated"
        ],
        "default_repeat_handling": "join_comma",
        "column_mappings": {
            "people.name": "contact_name",
            "people.id": "contact_id",
            "people.email": "email",
            "people.phones.number": "phone_numbers",
            "people.phones.type": "phone_types",
            "people.last_updated": "last_updated"
        },
        "decode_enums": true,
        "timestamp_format": "iso8601",
        "separator": "_",
        "max_depth": 10
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: contact_phones — One row per phone number (22 total)
-- ============================================================================
-- Exploded view: each PhoneNumber within each Person becomes its own row.
-- Person-level fields (name, id, email) are duplicated per phone row.
-- Contacts with no phones (Luis Hernandez) produce zero rows here.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.protobuf_demos.contact_phones
USING PROTOBUF
LOCATION '{{data_path}}'
OPTIONS (
    proto_schema = '{{schema_path}}',
    message_type = 'tutorial.AddressBook',
    proto_flatten_config = '{
        "row_path": "people",
        "explode_paths": ["people.phones"],
        "include_paths": [
            "people.name",
            "people.id",
            "people.email",
            "people.phones.number",
            "people.phones.type",
            "people.last_updated"
        ],
        "column_mappings": {
            "people.name": "contact_name",
            "people.id": "contact_id",
            "people.email": "email",
            "people.phones.number": "phone_number",
            "people.phones.type": "phone_type",
            "people.last_updated": "last_updated"
        },
        "decode_enums": true,
        "timestamp_format": "iso8601",
        "separator": "_",
        "max_depth": 10
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
