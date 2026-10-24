# mysql-canal

English | [简体中文](README.zh-CN.md)

The middle layer in mysql-cdc. Built on `mysql-replication`, it converts binlogs to normalized CDC changes and manages transactions, historical schema and checkpoints. `pikachu` supplies the CLI; this library has no default output handler.

## Listen for committed transactions

To integrate from local source, use the dependency below; replace `/path/to/mysql-cdc-rs` with your checkout path.

```toml
[dependencies]
mysql-canal = { path = "/path/to/mysql-cdc-rs/crates/mysql-canal" }
```

```rust,no_run
use mysql_canal::{Canal, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut canal = Canal::new(AppConfig::read(Path::new("canal.toml"))?)?;
    canal.add_handle(|event: CommittedTransaction<'_>| {
        for change in event.changes {
            println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    canal.run()
}
```

`canal.toml` uses the [standalone configuration example](examples/config/multi-source.toml), beginning at `[[sources]]`. Every source requires `[sources.mysql-replication]`. Canal adds source plugins, table selection, transaction budgets and checkpoints, reusing replication-owned connection and image settings. Pikachu embeds this same configuration under `[mysql-canal]`.

Each source owns its connection, schema and checkpoint. Initial consumption starts at the current position without a full snapshot; recovery decodes using saved historical schema.

## Events and handlers

`CommittedTransaction` contains `source_id`, the commit `checkpoint` and ordered `changes`. Each change contains its table, operation, exact key, optional previous key, changed column indices and `fields`. `fields.before()` / `fields.after()` expose applicable images; each image iterates logged fields with absolute indices, historical `ColumnMeta` and borrowed `RowValue`. Committed changes own compact image bytes and share column definitions. Field bytes count toward the transaction budget before copying. Logged integer signedness and primary-key metadata must agree with historical schema; conflicts stop consumption before rows are delivered.

Handlers run synchronously in registration order. All must succeed before checkpoint persistence. COMMIT/XID delivers the transaction; rollback discards it. Fully filtered commits deliver an empty slice. Failures can cause replay, so handlers must be idempotent. Register at least one handler before running; slow handlers apply backpressure.

`check()` validates the upstream and tables, `initialize()` creates initial checkpoints, and `run_once()` consumes currently available events.

## Source modes and typed decoding

`Canal::new(AppConfig)` consumes every configured source. Set `row_image = "full"` or `"minimal"` in each `[sources.mysql-replication]` section; one consumer can include both modes. The adapter constructs a `Decoder<Full>` or `Decoder<Minimal>` for each source and FDE. Row decoding uses static dispatch.

Direct decoder integrations choose the generic mode explicitly:

```rust
use mysql_canal::{Decoder, Full, Result, TableMeta};

fn full_decoder(tables: Vec<TableMeta>) -> Result<Decoder<Full>> {
    Decoder::<Full>::new(tables)
}
```

Use `Decoder::<Minimal>::new(tables)` for MINIMAL. Both modes deliver the same `Change` / `CommittedTransaction` API; MINIMAL retains only logged fields, while FULL retains every column in each applicable image; FULL `changed` includes every after-image column, even unchanged values. See the [listener example](examples/listen.rs).

## Offline replay

```sh
cargo run -p mysql-canal --example replay -- STATE.json /path/to/binlog.000001 full
```

`engine::Replay` replays from a historical checkpoint with an explicit `TransactionSink`, without updating state or connecting to MySQL. Run from the workspace root; see [replay.rs](examples/replay.rs).

## Reference

- [Rust fields and API reference](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/event-api.md)
- [Configuration, recovery and mode switching](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/configuration.md)
- [CDC fields and output contract](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.md)
- [Event flow and layer boundaries](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.md)
- [Custom source/version adapters](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/development.md)
- [Synthetic and real-server tests](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/testing.md)
