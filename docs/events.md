# Understanding events

English | [简体中文](events.zh-CN.md)

[Documentation index](README.md)

This guide explains what a database change produces, what each output field means, and how MINIMAL and FULL differ. No knowledge of Rust types is required.

## Start with one change

Suppose source `east` has this record in `app.users`, with an unsigned integer primary key `id` and nullable `age`:

| id | name | age | status |
| --- | --- | --- | --- |
| 42 | Alice | 20 | active |

An application updates it and commits the transaction:

```sql
UPDATE app.users SET name = 'Bob', age = NULL WHERE id = 42;
```

mysql-cdc processes this in three steps:

1. **Read the database log.** mysql-replication reads row data and transaction markers into raw binlog events.
2. **Build change notifications.** mysql-canal identifies the source, table, key and recorded fields, then delivers them after commit.
3. **Deliver the notifications.** Pikachu writes one JSON record per line by default. Library users can handle committed transactions themselves.

In MINIMAL mode, this update produces:

```json
{
  "source": "east",
  "schema": "app",
  "table": "users",
  "operation": "update",
  "pk": { "type": "unsigned", "value": 42 },
  "changed": ["name", "age"]
}
```

This means: **record 42 in east's app.users table was updated, and the log recorded the name and age fields.** The notification does not include non-key values such as Bob or NULL. Use replication's row-image reader to inspect raw values.

JSON is expanded here for readability; the CLI writes each record on one line.

## Fields in a change notification

These fields have the same meanings in MINIMAL and FULL.

| Field | Meaning | Example |
| --- | --- | --- |
| `source` | Configured source name, distinguishing MySQL instances | `east` |
| `schema` | Database name | `app` |
| `table` | Table name | `users` |
| `operation` | Insert, update or delete | `insert`, `update`, `delete` |
| `pk` | Key identifying the record; the new value if an update changes it | Key 42 |
| `previous_pk` | Previous key, supplied only when the key changes | 42 when the key changes from 42 to 43 |
| `changed` | Fields recorded in the after image, listed in table-column order | `name`, `age` |

Keys carry a type and a value. For example, `{"type":"unsigned","value":42}` is an unsigned integer, `{"type":"text","value":"user-42"}` is text, and `{"type":"decimal","value":"123.40"}` is an exact decimal preserving its scale. Signed integers use `signed`.

**`changed` is not the result of comparing old and new values.** It lists fields MySQL recorded in the after image. A recorded value can equal its previous value.

## What MINIMAL and FULL record

A row image is the part of a record saved in the log. `before` means data before the operation; `after` means data after it.

For the same update, the raw log contents can be understood as follows. This is an explanation of the data, not an additional output format:

| Data | MINIMAL before | MINIMAL after | FULL before | FULL after |
| --- | --- | --- | --- | --- |
| id | 42 | Not recorded | 42 | 42 |
| name | Not recorded | Bob | Alice | Bob |
| age | Not recorded | NULL | 20 | NULL |
| status | Not recorded | Not recorded | active | active |

For the single-key tables supported by CDC, MINIMAL uses the key to identify the record before an update, while the after image contains only the recorded columns. FULL includes all columns in each image.

The **notification fields stay the same**. The difference for this update is:

| Content | MINIMAL | FULL |
| --- | --- | --- |
| `operation` | `update` | `update` |
| `pk` | Unsigned integer 42 | Unsigned integer 42 |
| `changed` | `name`, `age` | `id`, `name`, `age`, `status` |
| Full row values in the notification | No | No |

In FULL, `status` appears in `changed` even though it is still active, because the complete after image records it.

Choose the mode explicitly in configuration. The consumer checks upstream settings and input shape. It does not change MySQL settings or reconstruct values missing from MINIMAL data. Raw replication does not require primary keys and its MINIMAL reader also accepts complete images; the key-only before-image requirement above belongs to canal's CDC policy.

### Missing data and NULL are different

| Case | Meaning | Is this a database NULL? |
| --- | --- | --- |
| A field is not recorded | This log entry does not provide its value | No |
| A field is recorded as NULL | The log explicitly records SQL NULL | Yes |
| An entire image is absent | This operation has no before or after record | It is not a row of NULL values |

In the MINIMAL example, `status` is not recorded, while the after value of `age` is NULL. Those are different cases.

## Inserts, updates and deletes

| Operation | Before data | After data | Notification `changed` |
| --- | --- | --- | --- |
| `insert` | Absent | Present | Fields actually recorded in the after image |
| `update` | Present | Present | Fields actually recorded in the after image |
| `delete` | Present | Absent | Empty list |

A present image need not be a complete row. Its columns depend on the policy and actual event. INSERT also uses the fields actually recorded; the consumer does not assume every column is present.

Deleting record 42 produces:

```json
{
  "source": "east",
  "schema": "app",
  "table": "users",
  "operation": "delete",
  "pk": { "type": "unsigned", "value": 42 },
  "changed": []
}
```

Changing a key from 42 to 43 supplies both values. This MINIMAL example changes only the key:

```json
{
  "source": "east",
  "schema": "app",
  "table": "users",
  "operation": "update",
  "pk": { "type": "unsigned", "value": 43 },
  "previous_pk": { "type": "unsigned", "value": 42 },
  "changed": ["id"]
}
```

## What a raw BinlogEvent contains

Raw events describe the MySQL log itself. Alongside row changes, they include transaction boundaries, file rotations and format information. One SQL statement can produce multiple events; one row event can contain multiple records.

Every raw event has three common parts, shared by both modes:

| Part | Meaning |
| --- | --- |
| Original data, `raw_data` | Complete event bytes, including the actual checksum information |
| Header, `header` | Time, event kind, producer ID, length, log position and flags |
| Content, `event` | Row changes, table information, a commit marker or another event body |

The common header fields describe the log, rather than business columns:

| Field | Meaning | What to keep in mind |
| --- | --- | --- |
| `timestamp` | Producer-recorded time in Unix seconds | Not the time the consumer received it |
| `event_type` | Event kind | A row change and a commit marker are different events |
| `server_id` | Producer-recorded server ID | Different from the configured source name |
| `event_size` | Complete event size in bytes | Includes the header and actual trailer/checksum |
| `log_pos` | Recorded log position, normally the next event position | Not an acknowledged consumer checkpoint |
| `flags` | Protocol flags | Used for parsing and diagnostics |

Common event kinds have these roles:

| Event | Purpose | Main contents |
| --- | --- | --- |
| Format description, FDE | Explain how to read a log file | Producer version, event layouts, checksum mode |
| Table map, TABLE_MAP | Identify the table and column layout for following rows | Temporary table ID, database/table, column types |
| Row changes, ROWS | Record inserts, updates and deletes | Table ID, operation, before/after data |
| Statement, QUERY | Record SQL or transaction boundaries | SQL and default database |
| Commit, XID | Confirm a transaction committed | Transaction identifier |
| Rotation, ROTATE | Move to another log file | Next filename and starting position |
| GTID / heartbeat | Describe transaction identity or maintain the connection | Transaction identity, metadata or filename |

MINIMAL/FULL differences are in row-image contents. They do not change the common header or add/remove these event kinds. Replication can return DDL in a statement event; canal stops on unsupported statements.

## Raw event contents in MINIMAL and FULL

This section expands the generic row events provided by replication. `BinlogEvent<'a, Minimal>` and `BinlogEvent<'a, Full>` are distinct compile-time types. The mode propagates through `EventData::Rows`, `RowsEvent<M>` and `Row<M>` to the image type; it is not a runtime mode field attached to one event type.

### Contents shared by row events

| Content | Access | Meaning in both modes |
| --- | --- | --- |
| Complete log bytes | `event.raw_data` | Original header, body and actual checksum |
| Common header | `event.header` | Time, event code, server ID, size, position and flags listed above |
| Row payload | `EventData::Rows(rows)` in `event.event` | `RowsEvent<'a, M>`, retaining the same mode |
| Temporary table ID | `rows.table_id` | Associates the preceding source-local TABLE_MAP |
| Row flags and operation | `rows.flags`, `rows.kind` | `RowsKind::Write`, `Update` or `Delete` |
| Encoded images | `rows.encoded_images` | Borrowed extra-data, column bitmaps and value bytes, before row iteration |
| Historical table definition | `rows.table()?`, `rows.columns()?` | Database, table and all column definitions, bound automatically by the parser |
| Decoded operations | `rows.rows()?` | Iterates `Result<Row<'a, M>>`; each `Row.operation` holds its applicable images |

The examples use the same `UPDATE app.users SET name = 'Bob', age = NULL WHERE id = 42`. Column indices are `0:id`, `1:name`, `2:age`, `3:status`; name and status are VARCHAR and age is nullable INT. Assume TABLE_MAP includes column-name metadata so names can be shown. Values in the trees are displayed for readability; these trees are neither `dump()` output nor another JSON format.

### MINIMAL

The event is `BinlogEvent<'_, Minimal>`. Each decoded row has an operation of type `RowOperation<SparseImage<'_>>`:

```text
BinlogEvent<Minimal>
└── event: EventData::Rows(RowsEvent<Minimal>)
    ├── table(): app.users
    ├── columns(): [id, name, age, status]
    └── rows(): Row<Minimal>
        └── operation: RowOperation::Update
            ├── before: SparseImage
            │   ├── column_indices(): [0]
            │   └── fields(): id=42
            └── after: SparseImage
                ├── column_indices(): [1, 2]
                └── fields(): name=Bob, age=NULL
```

`SparseImage` preserves the logged fields. `columns()` still describes all four columns and `column_count()` remains 4, even when only two fields are present. `column_indices()` contains absolute table ordinals, not compacted field positions.

| Access for this update | Result |
| --- | --- |
| `before.field(0)?` | `Some(Field)` with integer value 42 |
| `before.field(1)?` | `None`; the old name was not logged |
| `after.field(0)?` | `None`; the new id was not logged |
| `after.field(1)?` | `Some(Field)` with `RowValue::Bytes(b"Bob")` |
| `after.field(2)?` | `Some(Field)` with `RowValue::Null` |
| `after.field(3)?` | `None`; the new status was not logged |
| `after.field(4)` | `Err(Error::Invalid(_))`; outside the table's columns |

`None` means a valid column is absent from this image. Its definition remains available through `columns()[index]`; no NULL value is filled in. The raw MINIMAL reader also accepts complete images, so the table describes this particular log entry rather than a fixed set of missing columns for every MINIMAL event.

### FULL

The event is `BinlogEvent<'_, Full>`. Each decoded row has an operation of type `RowOperation<CompleteImage<'_>>`:

```text
BinlogEvent<Full>
└── event: EventData::Rows(RowsEvent<Full>)
    ├── table(): app.users
    ├── columns(): [id, name, age, status]
    └── rows(): Row<Full>
        └── operation: RowOperation::Update
            ├── before: CompleteImage
            │   └── fields(): id=42, name=Alice, age=20, status=active
            └── after: CompleteImage
                └── fields(): id=42, name=Bob, age=NULL, status=active
```

Every existing `CompleteImage` must contain all four columns. `field(index)` returns `Result<Field>`; an in-range field cannot return `None` because it was not logged:

| Access for this update | Result |
| --- | --- |
| `before.field(1)?` | `Field` with `RowValue::Bytes(b"Alice")` |
| `after.field(0)?` | `Field` with integer value 42 |
| `after.field(1)?` | `Field` with `RowValue::Bytes(b"Bob")` |
| `after.field(2)?` | `Field` with `RowValue::Null` |
| `after.field(3)?` | `Field` with `RowValue::Bytes(b"active")`, even though unchanged |
| `after.field(4)` | `Err(Error::Invalid(_))`; outside the table's columns |

FULL rejects an incomplete image with `Error::Unsupported`. SQL NULL remains the value of a present field, rather than a missing field.

### Operations and image types

| Operation | `Row<Minimal>.operation` | `Row<Full>.operation` |
| --- | --- | --- |
| INSERT | `Insert { after: SparseImage }` | `Insert { after: CompleteImage }` |
| UPDATE | `Update { before: SparseImage, after: SparseImage }` | `Update { before: CompleteImage, after: CompleteImage }` |
| DELETE | `Delete { before: SparseImage }` | `Delete { before: CompleteImage }` |

The table omits the `RowOperation::` prefix. INSERT has no before image, DELETE has no after image, and UPDATE requires both. Enum variants express these shapes directly. MINIMAL INSERT follows the actual column bitmap; do not assume it always carries a complete row.

### What each present field contains

Both modes return `Field { column: &ColumnInfo, value: RowValue }`:

| Content | Field or method | Meaning |
| --- | --- | --- |
| Column ordinal | `column.index` | Zero-based absolute table index |
| Name | `column.name` | `Option<Box<str>>`; `None` when not logged |
| Type and layout | `column.definition` | `wire_type()`, `column_type()`, `nullable()`, `metadata()` |
| Numeric signedness | `column.unsigned` | Optional metadata; absent signedness is not guessed |
| Character collation | `column.collation_id` | Optional collation ID |
| Key metadata | `column.primary_key`, `primary_key_prefix` | Optional membership and prefix length; raw reading still requires no key |
| Logged value | `value` | `RowValue::Null`, `Integer`, `Bytes`, `Decimal` and other variants |

For example, the after-image name field provides ordinal 1, its available name and type definition, and borrowed `RowValue::Bytes(b"Bob")`. `RowValue::Integer` retains the wire width; use `signed()` / `unsigned()` according to known signedness. Text stays as borrowed bytes, and DECIMAL preserves precision and scale.

**FULL means complete images, not complete metadata.** Names, signedness, collations and key attributes come from the log in both modes. Missing optional attributes stay `None`; no current-schema queries fill them in. Events share historical column definitions and values borrow the original buffer. `into_owned()` / `view()` retain the mode, definitions and actual checksum.

`EventData::RowsStatementEnd` is a separate statement-end sentinel with no columns or images. Control events such as FDE, QUERY, XID and ROTATE keep the same contents in both modes.

### What canal delivers after conversion

Replication exposes the raw values above, such as `name=Bob` and `age=NULL`. Canal's `Change` / `OwnedChange` still deliver only the table, operation, exact key, optional old key and `changed` column indices. They do not carry `SparseImage`, `CompleteImage` or full row values. For this update, MINIMAL `changed` is `[1, 2]` and FULL is `[0, 1, 2, 3]`; the CLI maps these indices to column names.

See the [Rust event API](event-api.md#read-concrete-operations-for-each-mode) for code matching `EventData::Rows`, reading fields and handling borrowing lifetimes.

## When delivery happens, and what failure means

With canal or Pikachu, applications receive changes after transaction commit. Rollback discards the transaction; excluded rows are not output. Library transaction handlers still receive fully filtered commits with an empty change list.

A checkpoint is saved after handlers succeed. If a handler has already produced side effects but the process exits before saving the checkpoint, restart can deliver the same transaction again. Consumers must tolerate duplicates. Flushing CLI stdout does not acknowledge downstream durable storage.

Events from one source follow log order, and another source cannot interrupt a transaction's delivery. Separate MySQL instances have no global time ordering.

When reading raw BinlogEvent records directly, seeing a row change does not mean the transaction committed. Use canal's transaction consumption interface when delivery must wait for commit.

## Supported data and stopping conditions

CDC requires a single-column primary key: integers, exact DECIMAL, or CHAR/VARCHAR using utf8, utf8mb3, utf8mb4, ascii or latin1. Composite, missing or unsupported keys fail explicitly. Consumers of large integers should use a JSON parser that preserves their precision.

Schema-changing SQL (DDL), unsupported statements/layouts, corrupt or truncated logs, missing keys and metadata mismatches stop consumption. Unacknowledged transactions do not advance disk checkpoints. NULL in a NOT NULL column also fails, including non-key columns.

DDL does not automatically refresh schema and continue. Recovery interprets historical logs with the schema saved in the checkpoint. Do not delete checkpoints to skip unprocessed data.

## Read next

- [Configuration and operation](configuration.md): choose a mode, prepare MySQL, recover and replay.
- [How the system works](architecture.md): layers, transactions and multiple sources.
- [Rust event API reference](event-api.md): complete fields, generic interfaces, accessors and borrowing lifetimes.
