# Rust event API reference

English | [简体中文](event-api.zh-CN.md)

[Documentation index](README.md)

Use this reference when integrating Rust APIs or working on protocol code. Start with the [event guide](events.md) for meanings, examples and mode comparisons; this page lists the corresponding types, interfaces and lifetimes.

## Event types and boundaries

| Type | Layer | Purpose |
| --- | --- | --- |
| `BinlogEvent<'a, M>` | replication | Original bytes, common header and protocol body |
| `Event<'a>` / `EventData<'a, M>` | replication | Shared decoding trait / actual body enum |
| `Change<'a>` / `OwnedChange` | canal | Normalized row change with exact keys and field indices |
| `CommittedTransaction<'a>` | canal | Complete transaction delivered after COMMIT |

`Event` is a trait with no public fields. It is separate from canal's normalized change types. Ordinary SELECT reads produce no CDC changes; a protocol QUERY carries SQL bytes and can contain DDL.

`dump` prints labeled fields, one per line. For example, an FDE prints `Version: 4`, `Server version: 8.4.6`, and `Checksum algorithm: CRC32`, followed by a blank line. The checksum algorithm comes from the validated frame trailer; body-only decoding leaves it `UNKNOWN` unless supplied by the caller. This context survives `into_owned()` and `view()`.

The structured parser supports FDE, ROTATE, QUERY, TABLE_MAP, ROWS v1/v2, XID, regular/anonymous/tagged GTID, Previous GTIDs and v1 heartbeat. Optional GTID metadata uses `Option` to distinguish absence from zero. Tagged GTID serialization v2 has a separate decoder. Other bodies, including heartbeat v2, XA, transaction payloads and unknown event codes, are explicitly `EventData::Raw`; this is not a claim of decoded support. SQL and schema bytes remain borrowed, and DDL is delivered as a QUERY event.

## Public event fields and image modes

The Rust type is `BinlogEvent<'a, M>`. `Event<'a>` is a shared, object-safe **trait**, with no fields; `EventData<'a, M>` is the enum stored in `BinlogEvent.event`. Match that enum to access a concrete protocol body. `Minimal` and `Full` distinguish ROWS policies and image types; they do not add or remove envelope or control-event fields.

### Common envelope and header

These are all public fields of `BinlogEvent<'a, M>`, for `M = Minimal` or `Full`:

| Field | Type | Meaning |
| --- | --- | --- |
| `raw_data` | `&'a [u8]` | Original complete event: common header, body and actual trailer/checksum, including the FDE algorithm byte; excludes file magic and network packet framing |
| `header` | `EventHeader` | The common 19-byte binlog v4 header below |
| `event` | `EventData<'a, M>` | Structured protocol body; ROWS carries the same `M` |

The internal `body` field is not public. `event.frame().body` exposes the borrowed body without the common header or framing trailer. `OwnedBinlogEvent<M>` has no public fields: use `raw_data()` and `view()`. `into_owned()` / `view()` preserve `M` and the original event bytes; a view borrows the owned buffer.

| Public `EventHeader` field | Type | Meaning |
| --- | --- | --- |
| `timestamp` | `u32` | Producer's event timestamp, in Unix seconds |
| `event_type` | `u8` | Wire event code; use named constants from `event_type` |
| `server_id` | `u32` | Server ID recorded by the producer |
| `event_size` | `u32` | Complete event length, including header and any trailer/checksum |
| `log_pos` | `u32` | Position recorded in the event header, normally the next event position; not an acknowledged CDC checkpoint, and some control events may use zero |
| `flags` | `u16` | Common-header flags; unknown bits are preserved |

`Event<'a>` exposes only `decode(&mut self, data: &'a [u8]) -> Result<()>` and `dump(&self, writer: &mut dyn Write) -> Result<()>`, plus the `Debug` supertrait. `EventHeader::decode` expects exactly 19 bytes; body decoders receive the body only. The trait itself supplies no `header`, `pk`, `before` or `after` fields. `dump` is diagnostic output.

### Protocol-body fields shared by both modes

`EventData` is non-exhaustive, so external matches need a fallback arm. Each variant contains the following public fields of its concrete body; only the `Rows` payload carries `M`:

| Variant / payload | Public fields, identical in MINIMAL and FULL |
| --- | --- |
| `FormatDescription` / `FormatDescriptionEvent<'a>` | `binlog_version: u16`, `server_version: &'a str`, `create_timestamp: u32`, `header_length: u8`, `post_header_lengths: &'a [u8]`, `checksum_algorithm: ChecksumAlgorithm` |
| `Rotate` / `RotateEvent<'a>` | `position: u64`, `next_file: &'a [u8]` |
| `Query` / `QueryEvent<'a>` | `thread_id: u32`, `execution_time: u32`, `error_code: u16`, `status_vars`, `schema`, `query` (each `&'a [u8]`) |
| `TableMap` / `TableMapEvent<'a>` | `table_id: u64`, `flags: u16`, `schema`, `table`, `column_data` (each `&'a [u8]`) |
| `Rows` / `RowsEvent<'a, M>` | `table_id: u64`, `flags: u16`, `kind: RowsKind`, `encoded_images: &'a [u8]` |
| `RowsStatementEnd` / `RowsStatementEndEvent<'a>` | `table_id()`, `flags()`, `kind()`; no columns or row images |
| `Xid` / `XidEvent` | `xid: u64` |
| `Gtid` and `AnonymousGtid` / `GtidEvent` | `flags: u8`, `sid: [u8; 16]`, `gno: i64`, plus the optional metadata listed below |
| `TaggedGtid` / `TaggedGtidEvent<'a>` | `gtid: GtidEvent`, `tag: &'a str`, `unknown_fields: &'a [u8]` |
| `PreviousGtids` / `PreviousGtidsEvent<'a>` | `sid_count: u64`, `encoded_sids: &'a [u8]` |
| `Heartbeat` / `HeartbeatEvent<'a>` | `log_file: &'a [u8]` |
| `Raw` / `RawEvent<'a>` | `data: &'a [u8]` for unknown or deliberately undecoded bodies |

The optional `GtidEvent` fields are `last_committed` / `sequence_number` (`Option<i64>`), `immediate_commit_timestamp` / `original_commit_timestamp` / `transaction_length` / `commit_group_ticket` (`Option<u64>`), and `immediate_server_version` / `original_server_version` (`Option<u32>`). Their presence depends on producer metadata and parser options, not the row-image mode. `ParserConfig` may also leave GTID bodies as `Raw` when structured decoding is disabled.

`TableMapEvent::columns()` exposes `TableColumns { count: usize, types, metadata, nullable, optional_metadata }`, with the last four fields borrowed byte slices. `PreviousGtidsEvent::sids()` exposes `SidIntervals { sid, intervals }`, both borrowed byte slices. These accessors are also shared by both modes. The `RowsEvent.flags` value is a ROWS-body flag set, separate from `BinlogEvent.header.flags`.

### Fields and accessors selected by the generic mode

Select `Full` or `Minimal` before reading. The same marker types are re-exported by canal. The parser, reader, event envelope, ROWS body and images retain `M`; mode selection uses static trait dispatch, without a runtime mode enum or per-row mode checks.

| API / property | `Minimal` | `Full` |
| --- | --- | --- |
| Envelope / body | `BinlogEvent<'a, Minimal>` / `EventData<'a, Minimal>` | `BinlogEvent<'a, Full>` / `EventData<'a, Full>` |
| `RowsEvent<M>::rows()` | `Result<Rows<'a, Minimal>>` | `Result<Rows<'a, Full>>` |
| Iterator item | `Result<Row<'a, Minimal>>` | `Result<Row<'a, Full>>` |
| `Row.operation` | `RowOperation<SparseImage<'a>>` | `RowOperation<CompleteImage<'a>>` |
| `image.field(index)` | `Result<Option<Field<'_, 'a>>>`; absent is `None` | `Result<Field<'_, 'a>>`; every in-range column exists |
| `image.fields()` | Present fields in table-column order | All fields in table-column order |
| `image.columns()` | All table column definitions, including absent fields | All table column definitions |
| Out-of-range index | `Error::Invalid` | `Error::Invalid` |

`RowOperation<I>` has `Insert { after: I }`, `Update { before: I, after: I }`, and `Delete { before: I }` variants. Required images cannot be missing. `before()` / `after()` borrow the applicable image; `into_images()` consumes the operation and returns the corresponding optional pair.

A present SQL NULL is `Field { value: RowValue::Null, .. }`. Under MINIMAL, `field(i)? == None` means a valid table column was not logged. Missing columns are never filled with NULL, defaults or values fetched from the current server. MINIMAL accepts complete bitmaps too; bytes cannot establish the producer's configured image policy.

`RowsEvent<M>::table()` and `columns()` return the bound historical definition in a `Result`. The parser owns a source-local TABLE_MAP registry and replaces a plan on every remap, even when the ID and column count are unchanged. It clears the registry on FDE, ROTATE, QUERY, XID and the ROWS statement-end flag. The final ordinary row event retains its bound plan after clearing. Missing TABLE_MAP and mismatched column counts are errors. Choose `M` explicitly in the constructor; a reader cannot change its mode.

`EventData::RowsStatementEnd` represents the special statement-end sentinel. Its payload exposes `table_id()`, `flags()` and `kind()`, with no table definition or row iterator. It clears mappings without requiring a TABLE_MAP. Both modes reject partial-JSON row events with `Error::Unsupported`; raw frame/view APIs remain available.

### Column metadata and borrowed field values

`TableDefinition` owns `schema: Box<[u8]>`, `table: Box<[u8]>`, shared `columns: Arc<[ColumnInfo]>`, and the original `optional_metadata: Box<[u8]>` TLVs. Each `ColumnInfo` contains:

| Field | Type | Meaning |
| --- | --- | --- |
| `index` | `u16` | Absolute zero-based table ordinal |
| `name` | `Option<Box<str>>` | UTF-8 column name when logged |
| `definition` | `ColumnDefinition` | Validated wire type, resolved type, nullability and encoding parameters |
| `unsigned` | `Option<bool>` | Logged numeric signedness; `None` means unknown |
| `collation_id` | `Option<u32>` | Logged character collation ID |
| `primary_key` | `Option<bool>` | Logged key membership; absent key metadata stays unknown |
| `primary_key_prefix` | `Option<usize>` | Character prefix length; zero means the entire key column |

`Field<'meta, 'event>` contains `column: &'meta ColumnInfo` and `value: RowValue<'event>`. Definitions are shared across events/images with `Arc`; row bytes remain borrowed. The metadata borrow cannot outlive the image, and a borrowed value cannot outlive the event buffer. `into_owned()` retains one raw buffer plus its shared TABLE_MAP plan; `view()` rebuilds borrowed fields without server queries or self-references and preserves the actual checksum.

Column metadata is independent of row-image completeness. `binlog_row_image=FULL` does not guarantee names; `binlog_row_metadata=FULL` logs additional attributes on servers supporting that setting. Missing attributes remain `None`. Malformed names, signedness, charset or key metadata return errors. Unknown TLVs remain available in `optional_metadata`; ENUM/SET labels and other attributes are not normalized by this API. See the [MySQL metadata setting](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html#sysvar_binlog_row_metadata).

Integers retain their wire width; choose `signed()` or `unsigned()` using known metadata. DECIMAL remains exact with precision and scale. Text/BLOB values remain borrowed bytes; temporal, JSON, geometry and vector values remain encoded bytes. No character-set conversion or JSON object materialization is implied.

## Read events with fields

```rust,no_run
use mysql_replication::{EventData, Full, Result, file::BinlogFile, parser::ParserConfig};
use std::{fs::File, io::BufReader};

fn inspect() -> Result<()> {
    let mut reader = BinlogFile::<_, Full>::new(
        BufReader::new(File::open("binlog.000001")?),
        ParserConfig::default(),
    )?;
    while let Some(event) = reader.next_event()? {
        if let EventData::Rows(rows) = event.event {
            for row in rows.rows()? {
                let (before, after) = row?.operation.into_images();
                for image in before.into_iter().chain(after) {
                    for field in image.fields() {
                        let field = field?;
                        println!(
                            "{} {:?}: {:?}",
                            field.column.index, field.column.name, field.value
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
```

The same reader with `Minimal` yields `SparseImage`. Mode-specific field access is checked by the compiler:

```rust,no_run
use mysql_replication::{
    Result,
    events::{CompleteImage, Field, SparseImage},
};

fn full(image: &CompleteImage<'_>) -> Result<()> {
    let field: Field<'_, '_> = image.field(0)?;
    println!("{:?}: {:?}", field.column.name, field.value);
    Ok(())
}

fn minimal(image: &SparseImage<'_>) -> Result<()> {
    let field: Option<Field<'_, '_>> = image.field(0)?;
    if let Some(field) = field {
        println!("{:?}: {:?}", field.column.name, field.value);
    }
    Ok(())
}
```

Each ROWS iterator compiles its sparse image plans once and shares column indices across rows, including tables wider than 64 columns. `field(index)` scans present fields up to the requested index; prefer `fields()` for sequential access. Row boundaries, lengths, NULL constraints and packed DECIMAL groups are validated before yielding each operation. A late error is yielded once and terminates the iterator; previously yielded rows remain visible. There is no transaction buffering in replication.

### Read concrete operations for each mode

This code reads the four-column table (`0:id`, `1:name`, `2:age`, `3:status`) from the [event contents example](events.md#minimal), explicitly accepting different `BinlogEvent` types. MINIMAL field access returns `Option<Field>`; FULL returns `Field`. Function parameters fix the mode without per-row mode checks.

```rust,no_run
use mysql_replication::{
    BinlogEvent, EventData, Full, Minimal, Result,
    events::{Field, RowOperation, RowValue},
};

fn read_minimal(event: &BinlogEvent<'_, Minimal>) -> Result<()> {
    if let EventData::Rows(rows) = &event.event {
        for row in rows.rows()? {
            if let RowOperation::Update { before, after } = row?.operation {
                let name: Option<Field<'_, '_>> = after.field(1)?;
                println!("before id: {:?}, after name: {:?}", before.field(0)?, name);
                match after.field(2)? {
                    None => println!("age was not logged"),
                    Some(Field {
                        value: RowValue::Null,
                        ..
                    }) => println!("age is SQL NULL"),
                    Some(field) => println!("age: {:?}", field.value),
                }
                println!("after status: {:?}", after.field(3)?);
            }
        }
    }
    Ok(())
}

fn read_full(event: &BinlogEvent<'_, Full>) -> Result<()> {
    if let EventData::Rows(rows) = &event.event {
        for row in rows.rows()? {
            if let RowOperation::Update { before, after } = row?.operation {
                let name: Field<'_, '_> = after.field(1)?;
                println!(
                    "before name: {:?}, after name: {:?}",
                    before.field(1)?,
                    name
                );
                println!("after age: {:?}", after.field(2)?.value);
                println!("after status: {:?}", after.field(3)?.value);
            }
        }
    }
    Ok(())
}
```

For that MINIMAL update, after name is present, age is SQL NULL and after status is `None`. In FULL, after name, age and status are all present fields, with `RowValue::Null` for age. Only UPDATE contains both `before` and `after`; see the [operation comparison](events.md#operations-and-image-types) for INSERT and DELETE.

## Raw transport and explicit protocol layouts

`file::FrameReader<R>::new(reader)` exposes validated `next_frame()` results without a field policy. `stream::ReplicationConnection` exposes raw network `next_frame()` / borrowed `next_view()`. Custom adapters use these transport interfaces and own their structural parser. Typed `BinlogFile<R, M>` and `ReplicationStream<M>` expose only `next_event()`, and require an explicit `M`.

For a standalone layout already validated by the caller, `RowsEvent::<M>::with_layout(kind, post_header, v2)` plus `Event::decode(body)` parses the ROWS envelope. `RowsDecoder::<M>::new(table_map)?.rows(&event)` decodes it using an explicitly supplied TABLE_MAP. This uses the same `Row<M>`, `Rows<M>`, `Field` and image types as automatic parser binding, without alternate event representations or cross-mode conversion. Raw protocol decoding requires no primary key. Canal keeps its own adapters and historical SQL schema, reusing borrowed field decoding for normalized images.

```sh
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001 --row-image full
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001 --row-image minimal
```

The single row example chooses a generic instantiation once at its command-line boundary. Row decoding itself uses static dispatch.

## Normalized canal event fields

`Change` is a borrowed conversion-callback view; `into_owned()` produces an `OwnedChange` that can be retained across events. Both have the same field meanings:

| Field | `Change<'a>` | `OwnedChange` | Meaning |
| --- | --- | --- | --- |
| `table` | `&'a Arc<TableMeta>` | `Arc<TableMeta>` | Historical schema/table and ordered column metadata |
| `operation` | `Operation` | `Operation` | `Insert`, `Update` or `Delete` |
| `pk` | `PrimaryKey<'a>` | `PrimaryKey<'static>` | Exact key; the new key when an UPDATE changes it |
| `previous_pk` | `Option<PrimaryKey<'a>>` | `Option<PrimaryKey<'static>>` | Old key when the key changes |
| `changed` | `&'a Arc<[u16]>` | `Arc<[u16]>` | Shared absolute column indices, in table order |
| `fields` | `RowOperation<FieldImage<'a>>` | `RowOperation<OwnedFieldImage>` | Logged before/after values and historical column definitions |

`PrimaryKey` variants are `Signed(i64)`, `Unsigned(u64)`, `Text(Cow<'a, str>)` and `Decimal(String)`. Decimal keys never pass through floating point. `OwnedChange::changed_names()` resolves indices using `table`.

| `CommittedTransaction<'a>` field | Type | Meaning |
| --- | --- | --- |
| `source_id` | `&'a str` | Stable configured source identity |
| `checkpoint` | `&'a State` | Commit boundary saved only after all handlers succeed |
| `changes` | `&'a [OwnedChange]` | Changes in binlog order; empty for a fully filtered commit |

`Canal::new(AppConfig)` creates the multi-source consumer. Each source configures its replication row-image mode independently; adapters construct `Decoder<Full>` or `Decoder<Minimal>` at the FDE boundary and decode rows through static dispatch.

`State` carries the historical schema, file/position and source UUID/ID/plugin. Transaction data is borrowed for the callback; clone only what must outlive it. `EventConverter` callbacks can run before COMMIT. Use `Canal` or `Processor` for committed delivery.

MINIMAL and FULL use these same normalized types and fields. MINIMAL exposes only logged fields; FULL exposes every column in each applicable image. The generic decoder validates policy before row callbacks; the normalized multi-source callback needs no runtime mode test.

See the [event guide](events.md) for JSON output, supported data types, commit delivery and failure behavior.

### Inspect canal fields

`FieldImage` and `OwnedFieldImage` provide `table()`, `columns()`, `column_indices()`, `fields()` and `field(index)`. `Field` has `index: u16`, `column: &ColumnMeta` and `value: RowValue`. Indices are absolute historical ordinals; `field(index)` returns `Result<Option<Field>>`, with `None` only for unlogged columns and an error outside the table. SQL NULL is `RowValue::Null`. INSERT carries only after, UPDATE both and DELETE only before, encoded by `RowOperation` variants.

Conversion `Change` borrows event values. `into_owned()` copies only each image's compact NULL bitmap and encoded values, sharing immutable layouts and historical columns. Committed callbacks receive `OwnedChange` and access values borrowing that change; clone only when retaining it beyond the callback. Images remain correct after input buffers are reused.

Raw `RowsDecoder<M>::rows_from_images(kind, images)` accepts already normalized `RowImages` from a structural adapter with the same mode checks and typed operations as `rows(&event)`. Raw `RowImage::into_owned()` returns an `OwnedRowImage`; `view()` and `fields()` read retained bytes without rebuilding full event buffers. `retained_bytes()` estimates image bytes plus shared layout cost, excluding shared table definitions.

See [Canal / CLI JSON](events.md#canal--cli-json) for the serialization contract and both image policies.

```rust,no_run
use mysql_canal::{OwnedChange, Result};

fn inspect(change: &OwnedChange) -> Result<()> {
    for (side, image) in [
        ("before", change.fields.before()),
        ("after", change.fields.after()),
    ] {
        if let Some(image) = image {
            for field in image.fields() {
                let field = field?;
                println!(
                    "{} [{}] {} {}: {:?}",
                    side, field.index, field.column.name, field.column.column_type, field.value
                );
            }
        }
    }
    Ok(())
}
```

### Embed canal in another product

Include canal's `AppConfig` directly and call `app.resolve_paths(parent_toml_path)` after deserialization for validation and relative paths. `AppConfig::read` reads only standalone canal files beginning at `[[sources]]`.

## Replication connection and parser APIs

In Rust, `Config::replication` uses `mysql_replication::config::Config`, re-exported by canal as `config::ReplicationConfig`; `config::RowImageMode` also re-exports the replication-owned type. `SourceConfig` contains only `plugin` and `adapter`.

`config::Config` owns reusable connection/subscription settings and supports `config::RowImageMode::{Minimal, Full}`. It defaults to MINIMAL, validates the required upstream setting without modifying MySQL, and builds low-level connection/stream configurations. Canal includes this exact type in its required `[sources.mysql-replication]` section. Defaults are 10 seconds for `connect_timeout_secs` and 90 for `read_timeout_secs`; zero timeouts are rejected. `tls_ca` is optional; an embedding application resolves relative paths. The binlog file and position are supplied by the consumer at runtime.

```rust,no_run
use mysql_replication::{
    Full, Result,
    config::{Config, RowImageMode},
    stream::ReplicationStream,
};

fn main() -> Result<()> {
    let mut settings = Config::new("MYSQL_URL", 123);
    settings.row_image = RowImageMode::Full;
    let mut stream = ReplicationStream::<Full>::connect(
        &settings.connection_config()?,
        settings.replication_config("binlog.000001", 4)?,
    )?;
    while let Some(event) = stream.next_event()? {
        println!("{}", event.header.log_pos);
    }
    Ok(())
}
```

The low-level APIs remain available for explicit parser selection and optional table preflight:

```rust,no_run
use mysql_replication::{
    Result,
    primary_key::validate_primary_keys,
    stream::{ConnectionConfig, ReplicationConfig, ReplicationConnection, connect},
};

fn main() -> Result<()> {
    let connection = ConnectionConfig::new("MYSQL_URL");
    let mut conn = connect(&connection)?;
    validate_primary_keys(&mut conn, [("app", "users"), ("app", "orders")])?;
    let config = ReplicationConfig::new(123, "binlog.000001", 4);
    let mut stream = ReplicationConnection::from_connection(conn, config)?;
    while let Some(view) = stream.next_view()? {
        // Borrowed body: no second raw-frame copy or row-value decoding.
        println!("{} {}", view.header.event_type, view.body.len());
    }
    Ok(())
}
```

Connection configuration controls credentials via an environment variable, TLS CA and timeouts. Replication configuration controls server ID, file/position, blocking mode, required upstream row image and parser options. Row-image requirements are verified, never changed globally. MySQL sends the available binlog bytes; there is no per-column network projection.

`primary_key::validate_primary_keys(&mut conn, tables)` performs optional INFORMATION_SCHEMA preflight before the caller constructs a transport. Selected tables need a supported single-column PRIMARY KEY; missing/composite keys and unsupported types or character sets fail explicitly. The list controls validation, not filtering. Raw transport and row decoding themselves require no key. Canal owns its separate startup eligibility check.

- `next_event()` returns typed protocol events and the complete raw bytes.
- `next_frame()` validates bounds and checksums, retaining raw bytes while leaving body interpretation to custom adapters.
- Network `next_view()` validates the driver's event and borrows its body directly, avoiding full raw-frame assembly for consumers that do not need it.
- `ParserConfig { decode_gtids: false, decode_previous_gtids: false }` retains those bodies as raw events.
- Typed parsers bind TABLE_MAP and validate image bitmaps at event time; row values remain borrowed and are validated during iteration.
- `TableMapEvent::columns()` exposes wire metadata; typed parsers compile its definitions before returning a TABLE_MAP.

## Protocol and transport limits

The structured parser selects advertised layouts from every file's FDE and accepts the registered MySQL series 5.7, 8.0–8.4, 9.0 and 9.7. Producer versions must have numeric `major.minor.patch` components, optionally followed by a nonempty `-` or `+` suffix (such as `5.7.44-log` or `8.0.45-0ubuntu0.24.04.1`). Malformed versions and MariaDB identifiers are rejected, using the same numeric parsing rules as CDC. Unknown series/layouts fail explicitly. `next_frame()` permits custom body parsers. Raw transport and typed readers are separate interfaces. Feed every FDE and TABLE_MAP to a custom parser when using raw transport.

Offline files must retain their initial FDE. CRC32/NONE are supported; truncated tails and oversized events fail, with a 64 MiB bound. Network transport uses `mysql`'s driver, which pre-parses FDE/TABLE_MAP/ROTATE and allocates before this library can check event size. Custom framing may require another transport. Full raw network events preserve the driver's original header fields, body, algorithm and stored checksum; they never call the driver's checksum-recalculating serializer. `next_view()` avoids this assembly entirely. Disconnects propagate errors without silent reconnects.

## Error boundaries

| Canal error | Trigger |
| --- | --- |
| `Error::Schema` | NULL in a NOT NULL non-key column, or schema mismatch |
| `Error::MissingPrimaryKey` | Missing or NULL primary key |
| `Error::Invalid` | Invalid input such as a malformed producer version |
| `Error::Unsupported` | Unsupported vendor, FDE layout or type |
| `Error::Statement` | QUERY other than BEGIN/COMMIT/ROLLBACK, including DDL |

Errors stop consumption; unacknowledged transactions must not advance disk checkpoints. Replication returns its separate `mysql_replication::Error` type. Regression entry points for these boundaries are in the [testing guide](testing.md).

## Non-key column support

Non-key field images support integers, FLOAT/DOUBLE, DECIMAL, BIT, date/time types with fractional seconds, CHAR/VARCHAR/BINARY/VARBINARY, ENUM/SET, BLOB/TEXT, JSON, spatial types, and VECTOR. Their values are exposed as borrowed `RowValue` views; binary JSON, temporal, geometry and vector contents remain encoded bytes without application object allocation. NULL bits are still checked against the saved schema: NULL in a NOT NULL column fails before transaction delivery or checkpoint persistence. Generated columns, invisible columns, the old DECIMAL wire type, and typed arrays produce explicit errors instead of guessed layouts.
