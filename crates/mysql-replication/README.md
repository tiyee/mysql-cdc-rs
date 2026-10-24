# mysql-replication

English | [简体中文](README.zh-CN.md)

The bottom library in mysql-cdc: synchronous MySQL binlog subscription, protocol parsing and `BinlogEvent` generation. Raw insert/update/delete images can be decoded on demand; normalized CDC events, transaction delivery and checkpoints belong to `mysql-canal`.

## Read a local binlog

To integrate from local source, use the dependency below; replace `/path/to/mysql-cdc-rs` with your checkout path.

```toml
[dependencies]
mysql-replication = { path = "/path/to/mysql-cdc-rs/crates/mysql-replication" }
```

```rust,no_run
use mysql_replication::{Event, Minimal, Result, file::BinlogFile, parser::ParserConfig};
use std::{fs::File, io::BufReader};

fn main() -> Result<()> {
    let mut reader = BinlogFile::<_, Minimal>::new(
        BufReader::new(File::open("binlog.000001")?),
        ParserConfig::default(),
    )?;
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

Credentials come from `MYSQL_URL`. `row_image` validates the upstream setting without changing MySQL; callers supply the file and position. `next_frame()` returns a validated raw frame, while network `next_view()` borrows the driver body directly.

## Fix the mode at construction

```rust,no_run
use mysql_replication::{EventData, Full, Result, file::BinlogFile, parser::ParserConfig};
use std::{fs::File, io::BufReader};

fn main() -> Result<()> {
    let mut reader = BinlogFile::<_, Full>::new(
        BufReader::new(File::open("binlog.000001")?),
        ParserConfig::default(),
    )?;
    while let Some(event) = reader.next_event()? {
        if let EventData::Rows(rows) = &event.event {
            for row in rows.rows()? {
                let (before, after) = row?.operation.into_images();
                for image in before.into_iter().chain(after) {
                    for field in image.fields() {
                        let field = field?;
                        println!("{:?}: {:?}", field.column.name, field.value);
                    }
                }
            }
        }
        let owned = event.into_owned()?;
        let _view: mysql_replication::BinlogEvent<'_, Full> = owned.view()?;
    }
    Ok(())
}
```

`Minimal` / `Full` propagate through parser, reader, `BinlogEvent` and `RowsEvent`; every typed constructor requires an explicit mode. `ReplicationStream::<Full>::connect(connection, config)` validates type/config agreement before connecting. Use `FrameReader` for raw files and `ReplicationConnection` for raw network frames/views; typed readers expose only `next_event()`.

## Decode row images

See the concrete [MINIMAL](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.md#minimal) / [FULL](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.md#full) contents for the new events: the same UPDATE expanded into event types, before/after images, column definitions, values, and absent/NULL access results.

`BinlogFile<_, M>` / `ReplicationStream<M>` automatically bind shared column definitions from TABLE_MAP, so `RowsEvent<M>::rows()` needs no external decoder. The same `Full` / `Minimal` markers select types through static dispatch, without runtime mode checks in row loops. `Row.operation` uses `RowOperation::Insert { after }`, `Update { before, after }`, or `Delete { before }`, preventing missing required images.

`Minimal` yields `SparseImage`, whose `field(index)` returns `Result<Option<Field>>`; a valid but unlogged column is `None`. `Full` yields `CompleteImage`, returns `Result<Field>` and rejects incomplete images. Both reject out-of-range indices. Each `Field` provides `column: &ColumnInfo` and a borrowed `value: RowValue`; SQL NULL is `Field.value = RowValue::Null`. `fields()` iterates present fields, while `columns()` exposes every table column definition.

Definitions include ordinals, available names, wire types, nullability, signedness, collations and key metadata. Metadata availability is independent of image completeness: `binlog_row_image=FULL` does not guarantee names. Missing optional attributes stay `None`; decoding never queries the current server schema. `into_owned()` / `view()` preserve historical column plans and the actual checksum. Parser mappings are source-local, replaced on remaps and cleared at file/statement boundaries. The sentinel has its own `EventData::RowsStatementEnd` variant. Select the mode before the first read.

Custom protocol parsers use `file::FrameReader` / `stream::ReplicationConnection`. Explicit `RowsDecoder<M>` decoding shares the same generic events and images as automatic binding. Raw decoding requires no primary key or transaction buffering; canal retains its own adapters and lightweight change output.

```sh
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001 --row-image full
```

Run examples from the workspace root. See the [event reference](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.md) for field definitions, NULL/absent-column distinctions and mode-specific accessors.

## Reference

- [Rust fields and API reference](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/event-api.md)
- [Connection settings, parser options and preflight](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/configuration.md)
- [Layer boundaries and event flow](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.md)
- [Development and extension guide](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/development.md)
- [Protocol and real-server testing](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/testing.md)
