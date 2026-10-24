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
use mysql_canal::{Cdc, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut canal = Cdc::new(AppConfig::read(Path::new("canal.toml"))?)?;
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

`CommittedTransaction` contains `source_id`, the commit `checkpoint` and ordered `changes`. Each change contains its table, operation, exact key, optional previous key and column indices. Only key values are decoded; other values are skipped by encoded length.

Handlers run synchronously in registration order. All must succeed before checkpoint persistence. COMMIT/XID delivers the transaction; rollback discards it. Fully filtered commits deliver an empty slice. Failures can cause replay, so handlers must be idempotent. Register at least one handler before running; slow handlers apply backpressure.

`check()` validates the upstream and tables, `initialize()` creates initial checkpoints, and `run_once()` consumes currently available events.

## Fix the mode at construction

```rust,no_run
use mysql_canal::{Cdc, CommittedTransaction, Full, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut cdc = Cdc::<Full>::for_mode(AppConfig::read(Path::new("full.toml"))?)?;
    cdc.add_handle(|event: CommittedTransaction<'_>| {
        for change in event.changes {
            println!("{} {:?}", event.source_id, change.pk);
        }
        Ok(())
    });
    cdc.run()
}
```

Use `Cdc::<Minimal>::for_mode(config)` for MINIMAL. Every source must match the type, checked before connections or checkpoint initialization. Default `Cdc::new` permits per-source modes. Both modes use the same `Change` / `CommittedTransaction` fields; FULL `changed` includes every after-image column, even unchanged values. See the [typed example](examples/listen_typed.rs).

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
