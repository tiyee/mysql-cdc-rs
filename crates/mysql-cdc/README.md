# mysql-cdc-rs

English | [简体中文](README.zh-CN.md)

One dependency for MySQL CDC and replication. This facade re-exports `mysql-canal` and `mysql-replication`, preserving their types, lifetimes and behavior.

The processing layers are `pikachu → mysql-canal → mysql-replication`; this facade provides access to the two libraries without adding a processing stage. Use `replication` to subscribe to binlogs, parse protocol events and optionally inspect sparse raw row images with `events::RowsDecoder`. Use `Cdc` for whitelist-based conversion, committed transaction handlers and checkpoint recovery. See the [architecture guide](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.md) for the event flow and boundaries.

```toml
[dependencies]
mysql-cdc-rs = "2.0.0"
```

The Rust import path is `mysql_cdc_rs`. CDC types and modules are available directly at the crate root and through `canal`; raw binlog events and replication transport are available through `replication`. Root `Error` and `Result` are the CDC types; raw protocol operations use `replication::Error` and `replication::Result`.

```rust,no_run
use mysql_cdc_rs::{Cdc, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut cdc = Cdc::new(AppConfig::read(Path::new("cdc.toml"))?)?;
    cdc.add_handle(|transaction: CommittedTransaction<'_>| {
        for change in transaction.changes {
            println!("{} {:?} {:?}", transaction.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    cdc.run()
}
```

Protocol events can be passed directly to CDC conversion without wrapping or copying them:

```rust
use mysql_cdc_rs::{
    Change, Result,
    canal::engine::converter::EventConverter,
    replication::BinlogEvent,
};

fn convert(converter: &mut EventConverter, event: &BinlogEvent<'_>) -> Result<()> {
    converter.convert_binlog(event, &mut |change: Change<'_>| {
        println!("{:?} {:?}", change.operation, change.pk);
        Ok(())
    })?;
    Ok(())
}
```

`EventConverter` emits row changes while processing ROWS, before transaction commit, and returns an `EventAction` describing control boundaries. The example above only demonstrates conversion. Applications delivering committed transactions should use `Cdc` or feed every event through `Processor`; direct conversion callbacks do not buffer transactions, discard rolled-back changes or persist checkpoints. Feed the file's FDE before converting its row events.

For offline raw events, import `mysql_cdc_rs::replication::file::BinlogFile`. For network replication, import `mysql_cdc_rs::replication::stream::ReplicationStream`. See [listen.rs](examples/listen.rs) and [dump_file.rs](examples/dump_file.rs) for complete examples. From the workspace root:

```sh
cargo run -p mysql-cdc-rs --example cdc_listen -- cdc.toml
cargo run -p mysql-cdc-rs --example replication_dump_file -- /path/to/binlog.000001
```

CDC uses named sources with independent schemas, whitelists and checkpoints. It delivers only committed changes containing the source, database/table, operation, exact primary key and changed column names. Rollback discards buffered changes; failures stop consumption, and a crash before checkpoint persistence may cause replay. There is no default output handler in the library. The `pikachu` package provides the `pikachu-cdc` CLI.

The lower-level crates remain available as separate dependencies. See the [project guide](https://github.com/tiyee/mysql-cdc-rs#readme), [CDC library](https://docs.rs/mysql-canal) and [replication library](https://docs.rs/mysql-replication) for configuration, output contracts, version support and limitations.
