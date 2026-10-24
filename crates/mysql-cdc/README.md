# mysql-cdc-rs

English | [简体中文](README.zh-CN.md)

One dependency for MySQL CDC and replication. This facade re-exports `mysql-canal` and `mysql-replication`, preserving their types, lifetimes and behavior.

The processing layers are `pikachu → mysql-canal → mysql-replication`; this facade provides access to the two libraries without adding a processing stage. Use `replication` to subscribe to binlogs, parse protocol events and optionally inspect sparse raw row images with `events::RowsDecoder<M>`. Use `canal::Canal` for whitelist-based conversion, committed transaction handlers and checkpoint recovery. See the [architecture guide](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.md) for the event flow and boundaries.

To integrate from local source, use the dependency below; replace `/path/to/mysql-cdc-rs` with your checkout path.

```toml
[dependencies]
mysql-cdc-rs = { path = "/path/to/mysql-cdc-rs/crates/mysql-cdc" }
```

The Rust import path is `mysql_cdc_rs`. The crate root only re-exports `canal` and `replication`. Use `canal::Canal` for the consumer lifecycle, `canal::config::AppConfig` for its configuration and `canal::Error` / `canal::Result` for its errors. Raw readers, streams, configuration and errors belong to `replication`. The aggregate has no consumer instance, configuration or error type of its own.

`canal.toml` uses the [standalone canal configuration](https://github.com/tiyee/mysql-cdc-rs/blob/master/crates/mysql-canal/examples/config/multi-source.toml), beginning at `[[sources]]`, with `[sources.mysql-replication]` under each source.

```rust,no_run
use mysql_cdc_rs::canal::{Canal, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut canal = Canal::new(AppConfig::read(Path::new("canal.toml"))?)?;
    canal.add_handle(|transaction: CommittedTransaction<'_>| {
        for change in transaction.changes {
            println!(
                "{} {:?} {:?}",
                transaction.source_id, change.operation, change.pk
            );
        }
        Ok(())
    });
    canal.run()
}
```

Protocol events can be passed directly to CDC conversion without wrapping or copying them:

Raw events use `replication::BinlogEvent<Full>` / `BinlogEvent<Minimal>` to select image types statically, sharing marker types with canal. Generic readers bind TABLE_MAP automatically; image `fields()` provides column definitions and borrowed values. MINIMAL distinguishes absent fields from SQL NULL, while FULL requires every column in each existing image. Missing optional metadata such as column names stays unknown; CDC changes do not acquire complete row values.

```rust
use mysql_cdc_rs::{
    canal::{Change, Result, engine::converter::EventConverter},
    replication::{BinlogEvent, BinlogMode},
};

fn convert<M: BinlogMode>(
    converter: &mut EventConverter,
    event: &BinlogEvent<'_, M>,
) -> Result<()> {
    converter.convert_binlog(event, &mut |change: Change<'_>| {
        println!("{:?} {:?}", change.operation, change.pk);
        Ok(())
    })?;
    Ok(())
}
```

`EventConverter` emits row changes while processing ROWS, before transaction commit, and returns an `EventAction` describing control boundaries. The example above only demonstrates conversion. Applications delivering committed transactions should use `Canal` or feed every event through `Processor`; direct conversion callbacks do not buffer transactions, discard rolled-back changes or persist checkpoints. Feed the file's FDE before converting its row events.

For offline raw events, import `mysql_cdc_rs::replication::file::BinlogFile`. For network replication, import `mysql_cdc_rs::replication::stream::ReplicationStream`. See [listen.rs](examples/listen.rs) and [dump_file.rs](examples/dump_file.rs) for complete examples. From the workspace root:

```sh
cargo run -p mysql-cdc-rs --example canal_listen -- canal.toml
cargo run -p mysql-cdc-rs --example replication_dump_file -- /path/to/binlog.000001
```

CDC uses named sources with independent schemas, whitelists and checkpoints. It delivers only committed changes containing the source, database/table, operation, exact primary key and changed column names. Rollback discards buffered changes; failures stop consumption, and a crash before checkpoint persistence may cause replay. There is no default output handler in the library. The `pikachu` package provides the `pikachu-cdc` CLI.

The lower-level crates remain available as separate dependencies. See the [project guide](https://github.com/tiyee/mysql-cdc-rs#readme), [CDC library](https://docs.rs/mysql-canal) and [replication library](https://docs.rs/mysql-replication) for configuration, output contracts, version support and limitations.

[Event reference](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.md) · [Development](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/development.md) · [Testing](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/testing.md)
