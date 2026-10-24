# Event reference

English | [简体中文](events.zh-CN.md)

[Documentation index](README.md)

This reference covers raw protocol events, row images and normalized canal CDC events. See [configuration](configuration.md) for mode selection/switching and [architecture](architecture.md) for implementation flow.

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

These are all public fields of `BinlogEvent<'a, M>`, for `M = Unspecified`, `Minimal` or `Full`:

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
| `Xid` / `XidEvent` | `xid: u64` |
| `Gtid` and `AnonymousGtid` / `GtidEvent` | `flags: u8`, `sid: [u8; 16]`, `gno: i64`, plus the optional metadata listed below |
| `TaggedGtid` / `TaggedGtidEvent<'a>` | `gtid: GtidEvent`, `tag: &'a str`, `unknown_fields: &'a [u8]` |
| `PreviousGtids` / `PreviousGtidsEvent<'a>` | `sid_count: u64`, `encoded_sids: &'a [u8]` |
| `Heartbeat` / `HeartbeatEvent<'a>` | `log_file: &'a [u8]` |
| `Raw` / `RawEvent<'a>` | `data: &'a [u8]` for unknown or deliberately undecoded bodies |

The optional `GtidEvent` fields are `last_committed` / `sequence_number` (`Option<i64>`), `immediate_commit_timestamp` / `original_commit_timestamp` / `transaction_length` / `commit_group_ticket` (`Option<u64>`), and `immediate_server_version` / `original_server_version` (`Option<u32>`). Their presence depends on producer metadata and parser options, not the row-image mode. `ParserConfig` may also leave GTID bodies as `Raw` when structured decoding is disabled.

`TableMapEvent::columns()` exposes `TableColumns { count: usize, types, metadata, nullable, optional_metadata }`, with the last four fields borrowed byte slices. `PreviousGtidsEvent::sids()` exposes `SidIntervals { sid, intervals }`, both borrowed byte slices. These accessors are also shared by both modes. The `RowsEvent.flags` value is a ROWS-body flag set, separate from `BinlogEvent.header.flags`.

### Fields and accessors that differ by mode

The four public `RowsEvent` fields above retain the same names and wire meaning. The mode difference appears when decoding images and rows:

| API / property | `Minimal` | `Full` |
| --- | --- | --- |
| Envelope / body types | `BinlogEvent<'a, Minimal>` / `EventData<'a, Minimal>` | `BinlogEvent<'a, Full>` / `EventData<'a, Full>` |
| `RowsEvent<M>::rows(&RowsDecoder)` | `Result<TypedRows<'a, Minimal>>` | `Result<TypedRows<'a, Full>>` |
| Iterator item | `Result<TypedRow<'a, Minimal>>` | `Result<TypedRow<'a, Full>>` |
| Public `TypedRow.before` | `Option<SparseImage<'a>>` | `Option<CompleteImage<'a>>` |
| Public `TypedRow.after` | `Option<SparseImage<'a>>` | `Option<CompleteImage<'a>>` |
| Image shape | Present columns only; complete bitmaps are also accepted by raw replication | Every column must exist in each existing image; sparse input is rejected, not filled in |
| `image.field(index)` | `Result<Option<RowValue<'a>>>`; `None` for an absent or out-of-range column | `Result<RowValue<'a>>`; every in-range column exists, out-of-range access is an error |
| Column indices | `image.column_indices()` gives present absolute zero-based indices | All table indices are present; use `image.as_ref().column_indices()` if needed |

`TypedRowsEvent<M>` has no public fields; use `table_id()`, `flags()`, `kind()`, `column_count()` and `rows()`.

Both image types have `column_count()` (total table columns) and `fields()` (an iterator over present fields). Their internal buffers/plans are private. `RowField` has `column_index: u16` and `value: RowValue<'a>` in both modes. Column names, SQL signedness and character-set interpretation are not supplied as mode-specific row fields.

`RowsEvent::images()` returns the shared encoded view `RowImages { extra_data: &'a [u8], column_count: usize, before_columns: &'a [u8], after_columns: &'a [u8], rows: &'a [u8] }`. For WRITE, `before_columns` names the **first wire bitmap**, which describes the after image; for DELETE both bitmap fields reference the before bitmap, and UPDATE has separate before/after bitmaps. These names do not create a before image for an INSERT. Full-mode parsing checks complete bitmaps before returning normal ROWS envelopes; statement-end sentinels carry no images. Field values are validated during row iteration.

The default `Unspecified` policy keeps the unclassified/raw API: `RowsEvent::rows` returns `Rows<'a>` with `Row { before: Option<RowImage<'a>>, after: Option<RowImage<'a>> }`. It does not establish a MINIMAL/FULL producer setting. Raw views from `next_frame()` / `next_view()` are likewise unclassified.

| Operation (`RowsKind`) | `before` in either mode | `after` in either mode |
| --- | --- | --- |
| INSERT (`Write`) | `None` | `Some(image)` |
| UPDATE (`Update`) | `Some(image)` | `Some(image)` |
| DELETE (`Delete`) | `Some(image)` | `None` |

Three cases remain distinct: `row.after` being `None` means the operation has no after image; `SparseImage::field(i)?` returning `None` means the column is not represented (or is out of range); `Some(RowValue::Null)` means a present column holds SQL NULL. Full-image access returns `RowValue::Null` directly for a present SQL NULL.

```rust,no_run
use mysql_replication::{Full, Minimal, Result, events::{RowValue, TypedRow}};

fn inspect_minimal(row: TypedRow<'_, Minimal>) -> Result<()> {
    if let Some(after) = row.after {
        match after.field(0)? {
            None => println!("column 0 is absent"),
            Some(RowValue::Null) => println!("column 0 is SQL NULL"),
            Some(value) => println!("column 0: {value:?}"),
        }
    }
    Ok(())
}

fn inspect_full(row: TypedRow<'_, Full>) -> Result<()> {
    if let Some(after) = row.after {
        let value: RowValue<'_> = after.field(0)?;
        println!("column 0: {value:?}");
    }
    Ok(())
}
```

These types are raw replication records. Canal's `Change` / `OwnedChange` instead contain `table`, `operation`, `pk`, `previous_pk` and `changed`; `CommittedTransaction` contains `source_id`, `checkpoint` and `changes`. Those public fields stay the same across modes. FULL changes which after-image column indices are included in `changed`; it does not add full-row values to CDC output. Canal also imposes its key-only MINIMAL before-image policy for selected single-key tables, while raw `Minimal` accepts sparse or complete images without requiring a primary key.

## Decoding raw row images

The event framework lives in `events`: `BinlogEvent` is the common container, `EventHeader` the common header, and `EventData` the typed body enum. Container ownership, control events, TABLE_MAP and ROWS have separate implementation modules; existing public import paths remain unchanged. `RowsEvent` describes the wire event and `RowsDecoder` compiles the column layout independently from CDC policy.

```rust,no_run
use mysql_replication::{Result, events::{RowValue, RowsDecoder, RowsEvent, TableMapEvent}};

fn inspect(table: &TableMapEvent<'_>, event: &RowsEvent<'_>) -> Result<()> {
    let decoder = RowsDecoder::new(table)?;
    for row in event.rows(&decoder)? {
        let row = row?;
        for (label, image) in [("before", row.before), ("after", row.after)] {
            if let Some(image) = image {
                for field in image.fields() {
                    let field = field?;
                    match field.value {
                        RowValue::Null => println!("{label}[{}] = NULL", field.column_index),
                        RowValue::Decimal(value) => println!("{label}[{}] = {value}", field.column_index),
                        value => println!("{label}[{}] = {value:?}", field.column_index),
                    }
                }
            }
        }
    }
    Ok(())
}
```

Cache one decoder per active TABLE_MAP within each source. Replace it when the table ID is remapped and clear mappings at file/statement boundaries. The decoder checks table ID and column count, but cannot detect a stale mapping with the same ID and column count. It owns the compiled layouts, so the input TABLE_MAP may be released. Each ROWS iterator compiles its sparse image plans once and shares column indices across rows; row payloads are borrowed without allocating a full row vector. Row boundaries, field lengths, NULL constraints and packed DECIMAL groups are validated before each operation is yielded. A decoding error is yielded once and terminates that iterator; earlier rows may already have been consumed. This API provides no transaction buffering.

- WRITE has only `after`, DELETE only `before`, and UPDATE has paired images. A missing image is `None`. Within an image, `fields()` yields only present columns with absolute zero-based indices. `RowValue::Null` means a present SQL NULL; an absent column is not filled in or inferred. Compact NULL bitmaps support sparse images and tables wider than 64 columns.
- Integers retain their actual wire width in `IntegerValue`. Call `signed()` or `unsigned()` using known SQL metadata; required TABLE_MAP metadata does not identify signedness. `DecimalValue` preserves precision and scale, validates packed digit groups, and formats exactly, including 65-digit values. FLOAT/DOUBLE, BIT, ENUM indices, SET masks and YEAR have explicit value variants.
- CHAR/VARCHAR and BLOB values are borrowed `Bytes`, excluding length prefixes. Character sets are not guessed. Temporal, JSON, geometry and vector values are borrowed `Encoded` bytes; `decoder.columns()` exposes their type and encoding metadata. This API does not convert them into dates, JSON objects or vectors. Legacy DECIMAL, typed arrays and unknown column types are explicitly unsupported.
- Invalid lengths, incomplete UPDATE pairs, invalid metadata and NULL in NOT NULL columns return raw protocol errors. Raw row decoding does not require a primary key; the separate selected-table preflight applies key restrictions when requested. CDC-specific decoding errors belong to `mysql-canal`. The CDC path continues skipping non-key values and does not call this decoder.

Run the complete offline inspection example with a binlog file that includes its FDE and TABLE_MAP events:

```sh
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001
```

## Choose image types with generics

`RowsEvent` remains the common borrowed wire event. `TypedRowsDecoder<M>` binds a TABLE_MAP plan to an image policy and returns `TypedRowsEvent<'a, M>` / `TypedRow<'a, M>`:

| Mode | Image type | `field(index)` result |
| --- | --- | --- |
| `Minimal` | `SparseImage<'a>` | `Result<Option<RowValue<'a>>>`; absent is `None`, SQL NULL is `Some(RowValue::Null)` |
| `Full` | `CompleteImage<'a>` | `Result<RowValue<'a>>`; every in-range column exists, SQL NULL is `RowValue::Null` |

```rust,no_run
use mysql_replication::{Result, events::{
    Full, Minimal, RowValue, RowsEvent, TableMapEvent, TypedRowsDecoder,
}};

fn inspect(table: &TableMapEvent<'_>, event: &RowsEvent<'_>) -> Result<()> {
    let full = TypedRowsDecoder::<Full>::new(table)?;
    for row in full.rows(event)? {
        if let Some(after) = row?.after {
            let value: RowValue<'_> = after.field(0)?;
            println!("{value:?}");
        }
    }
    let minimal = TypedRowsDecoder::<Minimal>::new(table)?;
    for row in minimal.rows(event)? {
        if let Some(after) = row?.after {
            let value: Option<RowValue<'_>> = after.field(0)?;
            println!("{value:?}");
        }
    }
    Ok(())
}
```

`TypedRowsDecoder::<Full>::event` rejects an incomplete bitmap in either existing image before iteration with `Error::Unsupported`. Values and complete UPDATE pairs are validated before yielding each row; malformed input yields one error and stops. WRITE has only `after`, DELETE only `before`, and UPDATE both. The generic changes the image type, not whether an operation has a before or after image. `CompleteImage::try_from(raw_image)` checks the same completeness invariant.

`column_count()` reports the total TABLE_MAP column count, including absent columns. `fields()` streams present fields with absolute column indices and is preferred for sequential access. `field(index)` scans up to that column without allocating; FULL returns `Error::Invalid` for an out-of-range index, MINIMAL returns `None`. Bytes remain borrowed and rows share the compiled column-index plan. Typed events and rows can outlive their decoder, but not the input bytes. `AsRef<RowImage>` permits common generic code; no full-row vector is created.

Use `raw_event.with_mode::<Full>(&raw_decoder)` to reuse an existing `RowsDecoder`. `TypedRowsDecoder::<Full>::from(raw_decoder)` also reuses its plan. Modes are sealed to preserve the completeness check. MINIMAL accepts complete bitmaps too: an INSERT or a table without a primary key can have all columns even under MINIMAL logging. The bytes cannot establish the producer's setting; online callers separately set `required_row_image` to check that setting.

For a runtime parameter, select the generic implementation once at the boundary with `match mode { ... run::<Minimal>(), ... run::<Full>() }`. The example defaults to MINIMAL and accepts both values:

```sh
cargo run -p mysql-replication --example decode_typed_rows -- /path/to/binlog.000001 --row-image full
cargo run -p mysql-replication --example decode_typed_rows -- /path/to/binlog.000001 --row-image minimal
```

## Normalized canal event fields

`Change` is a borrowed conversion-callback view; `into_owned()` produces an `OwnedChange` that can be retained across events. Both have the same field meanings:

| Field | `Change<'a>` | `OwnedChange` | Meaning |
| --- | --- | --- | --- |
| `table` | `&'a Arc<TableMeta>` | `Arc<TableMeta>` | Historical schema/table and ordered column metadata |
| `operation` | `Operation` | `Operation` | `Insert`, `Update` or `Delete` |
| `pk` | `PrimaryKey<'a>` | `PrimaryKey<'static>` | Exact key; the new key when an UPDATE changes it |
| `previous_pk` | `Option<PrimaryKey<'a>>` | `Option<PrimaryKey<'static>>` | Old key when the key changes |
| `changed` | `&'a Arc<[u16]>` | `Arc<[u16]>` | Shared absolute column indices, in table order |

`PrimaryKey` variants are `Signed(i64)`, `Unsigned(u64)`, `Text(Cow<'a, str>)` and `Decimal(String)`. Decimal keys never pass through floating point. `OwnedChange::changed_names()` resolves indices using `table`.

| `CommittedTransaction<'a>` field | Type | Meaning |
| --- | --- | --- |
| `source_id` | `&'a str` | Stable configured source identity |
| `checkpoint` | `&'a State` | Commit boundary saved only after all handlers succeed |
| `changes` | `&'a [OwnedChange]` | Changes in binlog order; empty for a fully filtered commit |

`State` carries the historical schema, file/position and source UUID/ID/plugin. Transaction data is borrowed for the callback; clone only what must outlive it. `EventConverter` callbacks can run before COMMIT. Use `Cdc` or `Processor` for committed delivery.

MINIMAL and FULL use these same normalized types and fields. Their difference is the after-image indices recorded in `changed`, described below; neither mode adds full row values.

## Output semantics

Output contains one JSON object per line. Events from each source follow binlog order. Sources share the output at transaction boundaries, so rows from another source cannot interrupt a transaction. Example UPDATE:

```json
{"source":"east","schema":"app","table":"users","operation":"update","pk":{"type":"unsigned","value":42},"changed":["name","age"]}
```

`source` is the stable source ID from configuration, distinguishing identical database/table/key combinations on different instances. `operation` is `insert`, `update`, or `delete`. When a primary key changes, `pk` holds the new value and `previous_pk` holds the old value. For INSERT, `changed` lists columns present in the after image. For DELETE, it is empty. For UPDATE, it lists columns in the after-image bitmap, including columns written as NULL and modified primary keys.

**In FULL mode, INSERT/UPDATE `changed` contains every after-image column, including unchanged values and the unchanged primary key. No before/after value comparison is performed.** The CDC output still contains no full row values.

**A "changed column" means a column recorded in the binlog after image; it does not guarantee that its value differs.** MINIMAL does not retain old non-key values, and columns specified by SQL may appear even when their values stay the same. MINIMAL logs alone cannot support a value-by-value comparison. See the [MySQL binlog_row_image documentation](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html) and [confirmed MySQL issue #83418](https://bugs.mysql.com/bug.php?id=83418).

Supported primary keys:

- TINYINT, SMALLINT, MEDIUMINT, INT, and BIGINT, including UNSIGNED. Values are decoded at their actual byte widths with correct sign extension, without conversion through floating point.
- DECIMAL, preserving scale: `{"type":"decimal","value":"-12345678.90"}`.
- CHAR and VARCHAR using utf8, utf8mb3, utf8mb4, ascii, or latin1. UTF-8 keys borrow the event buffer in low-level `Decoder` callbacks and are copied only when retained as owned transaction data. CHAR padding is trimmed from the end.

Composite keys, missing primary keys, FLOAT/DOUBLE keys, binary keys, and unsupported key character sets are rejected. JSON can encode u64 numbers, but ordinary JavaScript parsing loses precision for large integers. Consumers should use a parser that preserves large integers or convert them to strings in a custom sink.

Non-key columns can be skipped for integers, FLOAT/DOUBLE, DECIMAL, BIT, date/time types with fractional seconds, CHAR/VARCHAR/BINARY/VARBINARY, ENUM/SET, BLOB/TEXT, JSON, spatial types, and VECTOR. Their values are not decoded, so JSON, string, and BLOB contents do not allocate value objects. NULL bits are still checked against the saved schema: NULL in a NOT NULL column fails before transaction delivery or checkpoint persistence. Generated columns, invisible columns, the old DECIMAL wire type, and typed arrays produce explicit errors instead of guessed layouts.

## Transactions and limitations

Changes become visible only after XID/COMMIT. ROLLBACK discards buffered changes. After output succeeds and flushes, state is atomically replaced and both the file and parent directory are synchronized. A file lock prevents concurrent consumers of the same state. Output or persistence failures stop consumption. A crash between output and checkpoint persistence can cause duplicate delivery, so consumers must be idempotent. Flushing stdout only writes to the OS or pipe: **it does not acknowledge downstream durability, guarantee exactly-once delivery, or guarantee no loss on downstream failure.**

Any QUERY_EVENT other than BEGIN/COMMIT/ROLLBACK stops consumption, including DDL for another database. This conservative rule accounts for cross-database SQL, RENAME, and TRUNCATE, which cannot be safely filtered by the default database. DDL does not refresh the schema and continue automatically. Schema migrations require explicit handling of the schema version and the position after DDL, after consuming up to the DDL boundary. Starting a new state at the current position is appropriate only after confirming the previous backlog is fully handled; deleting state to skip a backlog is not a migration strategy.

XA, partial JSON updates, compressed transactions, MariaDB extensions, and unknown non-ignorable events are rejected. Heartbeats, GTID, ROTATE, format descriptions, and ordinary ignorable events are handled. Recovery uses file/position rather than GTID failover. Disconnections return errors; a process manager can restart consumption from saved state. The consumer does not reconnect silently within the process. Layout checks refer to the [MySQL 8.4 format description source](https://raw.githubusercontent.com/mysql/mysql-server/mysql-8.4.6/libs/mysql/binlog/event/control_events.cpp).

## Error boundaries

| Canal error | Trigger |
| --- | --- |
| `Error::Schema` | NULL in a NOT NULL non-key column, or schema mismatch |
| `Error::MissingPrimaryKey` | Missing or NULL primary key |
| `Error::Invalid` | Invalid input such as a malformed producer version |
| `Error::Unsupported` | Unsupported vendor, FDE layout or type |
| `Error::Statement` | QUERY other than BEGIN/COMMIT/ROLLBACK, including DDL |

Errors stop consumption; unacknowledged transactions must not advance disk checkpoints. Replication returns its separate `mysql_replication::Error` type. Regression entry points for these boundaries are in the [testing guide](testing.md).
