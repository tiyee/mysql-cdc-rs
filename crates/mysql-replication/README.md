# mysql-replication

English | [简体中文](README.zh-CN.md)

The bottom library layer: synchronous MySQL binlog subscription, protocol parsing and opt-in decoding of borrowed insert/update/delete row images. CDC conversion, whitelist filtering, historical SQL schema, transaction delivery and persistent checkpoints belong to the `mysql-canal` consumer library (`mysql_canal`), on which the `pikachu` CLI is built.

Raw parsing returns `BinlogEvent` / `EventData`; row decoding uses TABLE_MAP and `RowsDecoder` to expose sparse `Row { before, after }` images. It does not produce canal's `Change` or `CommittedTransaction`. Ordinary SELECT reads do not produce CDC changes; the protocol QUERY event holds SQL bytes, not query results. See the [architecture guide](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.md) for both decoding paths.

For a single dependency covering raw replication and CDC, use `mysql-cdc-rs` and import this API through `mysql_cdc_rs::replication`.

```toml
[dependencies]
mysql-replication = "1.0.0"
```

```rust,no_run
use mysql_replication::{Event, Result, file::BinlogFile};
use std::{fs::File, io::BufReader};

fn main() -> Result<()> {
    let mut reader = BinlogFile::new(BufReader::new(File::open("binlog.000001")?))?;
    while let Some(event) = reader.next_event()? {
        println!("{} {}", event.header.server_id, event.header.log_pos);
        event.event.dump(&mut std::io::stderr())?;
        // Clone the complete raw buffer only when retaining this event.
        let owned = event.into_owned()?;
        let view = owned.view()?;
        assert_eq!(view.raw_data, owned.raw_data());
    }
    Ok(())
}
```

`Event<'a>` has `decode(&mut self, &'a [u8]) -> Result<()>` and `dump(&self, &mut dyn Write) -> Result<()>`. The common `EventHeader` and concrete body types implement the same object-safe interface. Header decoding accepts exactly 19 bytes. Body decoding excludes the common header and checksum. `dump` writes human-readable diagnostics and propagates writer failures; it is neither binary serialization nor CDC output.

`dump` prints labeled fields, one per line. For example, an FDE prints `Version: 4`, `Server version: 8.4.6`, and `Checksum algorithm: CRC32`, followed by a blank line. The checksum algorithm comes from the validated frame trailer; body-only decoding leaves it `UNKNOWN` unless supplied by the caller. This context survives `into_owned()` and `view()`.

`BinlogEvent` associates `raw_data`, the complete header and typed `EventData`. Raw data contains the original common header, body and actual trailer, including the FDE algorithm byte; it excludes the file magic and network packet framing. Reader results borrow its reusable buffer until the next read. `into_owned()` retains one raw buffer; `view()` borrows it without self references or unsafe code. `EventData` is an enum, so consumers access concrete fields through matching rather than downcasting.

The structured parser supports FDE, ROTATE, QUERY, TABLE_MAP, ROWS v1/v2, XID, regular/anonymous/tagged GTID, Previous GTIDs and v1 heartbeat. Optional GTID metadata uses `Option` to distinguish absence from zero. Tagged GTID serialization v2 has a separate decoder. Other bodies, including heartbeat v2, XA, transaction payloads and unknown event codes, are explicitly `EventData::Raw`; this is not a claim of decoded support. SQL and schema bytes remain borrowed, and DDL is delivered as a QUERY event.

## Configuration and selective work

```rust,no_run
use mysql_replication::{Result, stream::{ConnectionConfig, ReplicationConfig, ReplicationStream, RowImage}, parser::ParserConfig};

fn main() -> Result<()> {
    let connection = ConnectionConfig::new("MYSQL_URL");
    let mut config = ReplicationConfig::new(123, "binlog.000001", 4);
    config.required_row_image = Some(RowImage::Minimal);
    config.parser = ParserConfig::minimal();
    let mut stream = ReplicationStream::connect_with_tables(
        &connection, config, [("app", "users"), ("app", "orders")],
    )?;
    while let Some(view) = stream.next_view()? {
        // Borrowed body: no second raw-frame copy or row-value decoding.
        println!("{} {}", view.header.event_type, view.body.len());
    }
    Ok(())
}
```

Connection configuration controls credentials via an environment variable, TLS CA and timeouts. Replication configuration controls server ID, file/position, blocking mode, required upstream row image and parser options. Row-image requirements are verified, never changed globally. MySQL sends the available binlog bytes; there is no per-column network projection.

`ReplicationStream::connect_with_tables` and `from_connection_with_tables` validate the caller's selected tables through INFORMATION_SCHEMA before sending a binlog request. Every selected table must have a single-column PRIMARY KEY: TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT (signed or unsigned), exact DECIMAL, or CHAR/VARCHAR using utf8, utf8mb3, utf8mb4, ascii or latin1. Missing tables/keys, composite keys, UNIQUE-only indexes, FLOAT/DOUBLE, binary/date keys and unsupported character sets return an error with the database/table and, when available, column name. Empty selections are rejected. This list controls validation only, not event filtering. The existing `connect` and `from_connection` methods remain available for raw replication without this check. Call `primary_key::validate_primary_keys(&mut conn, tables)` to run the same check independently without starting replication. The MySQL CDC source in `mysql-canal` uses this validation automatically, including on checkpoint recovery.

- `next_event()` returns typed protocol events and the complete raw bytes.
- `next_frame()` validates bounds and checksums, retaining raw bytes while leaving body interpretation to custom adapters.
- Network `next_view()` validates the driver's event and borrows its body directly, avoiding full raw-frame assembly for consumers that do not need it.
- `ParserConfig::minimal()` skips GTID metadata and Previous GTIDs decoding. They remain explicit raw bodies with their original event codes.
- ROWS image parsing is deferred by default; `RowsEvent::images()` reads extra data and sparse bitmaps on demand. `RowsDecode::ValidateImages` validates those immediately. These parser modes do not decode field values; the separate `RowsDecoder` API below is opt-in.
- `TableMapEvent::columns()` validates column envelopes on demand, after table selection.

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

The structured parser selects advertised layouts from every file's FDE and accepts the registered MySQL series 5.7, 8.0–8.4, 9.0 and 9.7. Producer versions must have numeric `major.minor.patch` components, optionally followed by a nonempty `-` or `+` suffix (such as `5.7.44-log` or `8.0.45-0ubuntu0.24.04.1`). Malformed versions and MariaDB identifiers are rejected, using the same numeric parsing rules as CDC. Unknown series/layouts fail explicitly. `next_frame()` permits custom body parsers. When switching between frame/view and structured-event methods, feed encountered FDE frames into your own `BinlogParser`; the built-in structured parser only observes `next_event()` calls.

Offline files must retain their initial FDE. CRC32/NONE are supported; truncated tails and oversized events fail, with a 64 MiB bound. Network transport uses `mysql`'s driver, which pre-parses FDE/TABLE_MAP/ROTATE and allocates before this library can check event size. Custom framing may require another transport. Full raw network events preserve the driver's original header fields, body, algorithm and stored checksum; they never call the driver's checksum-recalculating serializer. `next_view()` avoids this assembly entirely. Disconnects propagate errors without silent reconnects.

```sh
cargo test -p mysql-replication --locked
cargo test -p mysql-canal --locked
```

Synthetic protocol tests verify headers, raw-byte preservation, CRC32/NONE, optional/tagged GTID metadata, lazy ROWS parsing, decoded before/after images, wide sparse/NULL bitmaps, signed/unsigned integer boundaries, exact DECIMAL, malformed fields and owned lifetimes. Real-server CDC compatibility tests and version adapters live in `mysql-canal`; see the [project README](https://github.com/tiyee/mysql-cdc-rs#readme).
