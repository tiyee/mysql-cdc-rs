# mysql-replication

English | [简体中文](README.zh-CN.md)

The bottom library in mysql-cdc: synchronous MySQL binlog subscription, protocol parsing and `BinlogEvent` generation. Raw insert/update/delete images can be decoded on demand; normalized CDC events, transaction delivery and checkpoints belong to `mysql-canal`.

## Read a local binlog

```toml
[dependencies]
mysql-replication = "2.0.0"
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

`next_event()` returns original bytes, the common header and an `EventData` body, borrowed until the next read. Use `into_owned()` only when retaining an event. `Event::dump` prints field diagnostics. Files must retain their initial FDE.

## Subscribe to MySQL

```rust,no_run
use mysql_replication::{Result, config::{Config, RowImageMode}, stream::ReplicationStream};

fn main() -> Result<()> {
    let mut settings = Config::new("MYSQL_URL", 123);
    settings.row_image = RowImageMode::Full;
    let mut stream = ReplicationStream::connect(
        &settings.connection_config()?,
        settings.replication_config("binlog.000001", 4)?,
    )?;
    while let Some(event) = stream.next_event()? {
        println!("{}", event.header.log_pos);
    }
    Ok(())
}
```

Credentials come from `MYSQL_URL`. `row_image` validates the upstream setting without changing MySQL; callers supply the file and position. `next_frame()` returns a validated raw frame, while network `next_view()` borrows the driver body directly.

## Fix the mode at construction

```rust,no_run
use mysql_replication::{Full, Result, file::BinlogFile, parser::ParserConfig};
use std::{fs::File, io::BufReader};

fn main() -> Result<()> {
    let mut reader = BinlogFile::<_, Full>::for_mode(
        BufReader::new(File::open("binlog.000001")?),
        ParserConfig::default(),
    )?;
    while let Some(event) = reader.next_event()? {
        let owned = event.into_owned()?;
        let _view: mysql_replication::BinlogEvent<'_, Full> = owned.view()?;
    }
    Ok(())
}
```

`Minimal` / `Full` propagate through parser, reader, `BinlogEvent` and `RowsEvent`. `ReplicationStream::<Full>::for_mode(connection, config)` validates type/config agreement before connecting. `next_event()` returns mode-checked events; raw `next_frame()` / `next_view()` remain unclassified. Default constructors use `Unspecified`.

## Decode row images

Build a `RowsDecoder` from TABLE_MAP and iterate before/after images through `RowsEvent::rows(&decoder)`. `Minimal` uses `SparseImage`; `Full` uses `CompleteImage` and rejects incomplete images. Raw decoding requires no primary key and provides no transaction buffering. Callers maintain, replace and clear TABLE_MAP plans.

```sh
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001
cargo run -p mysql-replication --example decode_typed_rows -- /path/to/binlog.000001 --row-image full
```

Run examples from the workspace root. See the [event reference](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.md) for field definitions, NULL/absent-column distinctions and mode-specific accessors.

## Reference

- [Connection settings, parser options and preflight](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/configuration.md)
- [Layer boundaries and event flow](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.md)
- [Development and extension guide](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/development.md)
- [Protocol and real-server testing](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/testing.md)
