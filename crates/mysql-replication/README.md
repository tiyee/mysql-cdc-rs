# mysql-replication

English | [简体中文](README.zh-CN.md)

Synchronous MySQL replication transport and borrowed binlog events. CDC conversion, whitelists, historical SQL schema, transaction delivery and persistent checkpoints belong to the `mysql-canal` consumer library (`mysql_canal`).

For a single dependency covering raw replication and CDC, use `mysql-cdc-rs` and import this API through `mysql_cdc_rs::replication`.

```toml
[dependencies]
mysql-replication = "0.0.4"
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
    let mut stream = ReplicationStream::connect(&connection, config)?;
    while let Some(view) = stream.next_view()? {
        // Borrowed body: no second raw-frame copy or row-value decoding.
        println!("{} {}", view.header.event_type, view.body.len());
    }
    Ok(())
}
```

Connection configuration controls credentials via an environment variable, TLS CA and timeouts. Replication configuration controls server ID, file/position, blocking mode, required upstream row image and parser options. Row-image requirements are verified, never changed globally. The library does not query application tables or require primary keys. MySQL sends the available binlog bytes; there is no per-column network projection.

- `next_event()` returns typed protocol events and the complete raw bytes.
- `next_frame()` validates bounds and checksums, retaining raw bytes while leaving body interpretation to custom adapters.
- Network `next_view()` validates the driver's event and borrows its body directly, avoiding full raw-frame assembly for consumers that do not need it.
- `ParserConfig::minimal()` skips GTID metadata and Previous GTIDs decoding. They remain explicit raw bodies with their original event codes.
- ROWS image parsing is deferred by default; `RowsEvent::images()` reads extra data and sparse bitmaps on demand. `RowsDecode::ValidateImages` validates those immediately. No mode materializes full row values.
- `TableMapEvent::columns()` validates column envelopes on demand, after table selection.

The structured parser selects advertised layouts from every file's FDE and accepts the registered MySQL series 5.7, 8.0–8.4, 9.0 and 9.7. Unknown series/layouts fail explicitly. `next_frame()` permits custom body parsers. When switching between frame/view and structured-event methods, feed encountered FDE frames into your own `BinlogParser`; the built-in structured parser only observes `next_event()` calls.

Offline files must retain their initial FDE. CRC32/NONE are supported; truncated tails and oversized events fail, with a 64 MiB bound. Network transport uses `mysql`'s driver, which pre-parses FDE/TABLE_MAP/ROTATE and allocates before this library can check event size. Custom framing may require another transport. Full raw network events preserve the driver's original header fields, body, algorithm and stored checksum; they never call the driver's checksum-recalculating serializer. `next_view()` avoids this assembly entirely. Disconnects propagate errors without silent reconnects.

```sh
cargo test -p mysql-replication --locked
cargo test -p mysql-canal --locked
```

Synthetic protocol tests verify headers, raw-byte preservation, CRC32/NONE, optional/tagged GTID metadata, lazy ROWS parsing, wide sparse bitmaps and owned lifetimes. Real-server CDC compatibility tests and version adapters live in `mysql-canal`; see the [project README](../../README.md).
