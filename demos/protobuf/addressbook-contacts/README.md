# Protobuf Address Book — Contact Directory

Demonstrates how DeltaForge reads Protocol Buffers (proto3) binary files using the classic Google addressbook schema. Three team roster files are read into a flattened contacts table and an exploded phone-number table, showcasing nested message flattening, repeated field handling, enum decoding, and well-known type support.

## Data Story

A company maintains its internal contact directory as protobuf-serialized `AddressBook` messages. Each team exports a `.pb` file containing their members. The `Person` message includes nested `PhoneNumber` messages (with a `PhoneType` enum) and a `google.protobuf.Timestamp` for the last update. Some executive contacts have sparse data — missing email or no phone numbers at all.

| File | Team | Contacts | Phone Numbers | Notes |
|------|------|----------|---------------|-------|
| `01_engineering_team.pb` | Engineering | 5 | 9 | Mixed mobile/home/work, international |
| `02_sales_team.pb` | Sales | 5 | 9 | International numbers (KR, AE, SE) |
| `03_executives.pb` | Executives | 3 | 4 | Sparse: missing email, empty phone list |
| **Total** | | **13** | **22** | |

## Proto Schema Structure

```protobuf
message AddressBook {
  repeated Person people = 1;
}

message Person {
  string name = 1;
  int32 id = 2;
  string email = 3;

  enum PhoneType {
    MOBILE = 0;
    HOME = 1;
    WORK = 2;
  }

  message PhoneNumber {
    string number = 1;
    PhoneType type = 2;
  }

  repeated PhoneNumber phones = 4;
  google.protobuf.Timestamp last_updated = 5;
}
```

## Tables

### `contacts` — One row per person (13 rows)

Flattened view. Repeated phone numbers joined into comma-separated strings.

| Column | Source | Notes |
|--------|--------|-------|
| `contact_id` | `people.id` | Unique person ID (int32) |
| `contact_name` | `people.name` | Full name |
| `email` | `people.email` | Empty string for Maria Schmidt |
| `phone_numbers` | `people.phones.number` | Comma-joined (e.g., "+1-555-0101, +1-555-0102") |
| `phone_types` | `people.phones.type` | Comma-joined enum labels (e.g., "MOBILE, WORK") |
| `last_updated` | `people.last_updated` | ISO 8601 from Timestamp well-known type |
| `df_file_name` | (file metadata) | Source .pb filename |

### `contact_phones` — One row per phone number (22 rows)

Exploded view. Each PhoneNumber within each Person becomes its own row.

| Column | Source | Notes |
|--------|--------|-------|
| `contact_id` | `people.id` | Duplicated per phone row |
| `contact_name` | `people.name` | Duplicated per phone row |
| `email` | `people.email` | Duplicated per phone row |
| `phone_number` | `people.phones.number` | Single phone number |
| `phone_type` | `people.phones.type` | Decoded enum: MOBILE, HOME, or WORK |
| `last_updated` | `people.last_updated` | ISO 8601 timestamp |
| `df_file_name` | (file metadata) | Source .pb filename |

## How to Verify

Run the **Summary** query (#14) to see PASS/FAIL for each check:

```sql
SELECT check_name, result FROM (
    SELECT 'contact_count_13' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM external.protobuf.contacts) = 13
                THEN 'PASS' ELSE 'FAIL' END AS result
    UNION ALL ...
) checks
ORDER BY check_name;
```

## What This Tests

1. **Proto3 binary reading** — Schema-driven deserialization using `.proto` definitions
2. **Nested messages** — `Person.PhoneNumber` flattened to top-level columns
3. **Repeated fields (join)** — Multiple phones joined as comma-separated string in `contacts`
4. **Repeated fields (explode)** — One row per phone number in `contact_phones`
5. **Enum decoding** — `PhoneType` (0/1/2) decoded to MOBILE/HOME/WORK string labels
6. **Well-known types** — `google.protobuf.Timestamp` converted to ISO 8601 datetime
7. **Sparse data** — Empty repeated field (no phones) and empty string field (no email)
8. **Multi-file reading** — 3 team files merged into unified tables
9. **File metadata** — `df_file_name`, `df_row_number` system columns for traceability
10. **Column mappings** — Proto field paths mapped to friendly column names
