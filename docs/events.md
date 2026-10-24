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
