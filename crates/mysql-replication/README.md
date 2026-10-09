# mysql-replication

English | [简体中文](README.zh-CN.md)

A reusable Rust MySQL CDC library with configuration-driven consumption and committed event handlers. The `pikachu-rs` package provides the `pikachu-cdc` CLI and registers its own default printing handle.

```toml
[dependencies]
mysql-replication = "0.0.2"
```

```rust,no_run
use mysql_replication::{Event, Replication, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let config = AppConfig::read(Path::new("cdc.toml"))?;
    let mut replication = Replication::new(config)?;
    replication.add_handle(|event: Event<'_>| {
        for change in event.changes {
            println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    replication.run()
}
```

Construct `AppConfig` in Rust or read the named `[[sources]]` TOML configuration; see `examples/config/multi-source.toml`. Call `add_handle` for each closure or `EventHandler` implementation. `run()` consumes continuously; `run_once()` consumes the currently available binlog. At least one handle is required; there is no default output. Use `check()` to validate source settings and `initialize()` to save initial checkpoints.

Each `Event` borrows the source ID, commit checkpoint, and ordered `OwnedChange` slice. Changes contain table metadata, operation, an exact primary key, optional previous key, and changed column indices/names. Full row values are not materialized. Handlers run synchronously in registration order, serialized across sources, only after COMMIT/XID; rolled-back and incomplete transactions produce no callbacks. Fully filtered commits deliver an empty slice. All handlers must succeed before checkpoint persistence. Failure stops consumption; earlier handler side effects may replay after restart. Slow handlers apply backpressure. Clone changes only when retaining them after the callback.

Sources keep independent connections, schema snapshots, transaction budgets, mappings, and checkpoints. Initial consumption starts at the current position without a data snapshot; recovery uses historical schema from the checkpoint. DDL stops consumption with an explicit error. Credentials are read from the configured environment variable. Requires Rust 1.89+, MySQL ROW + MINIMAL, and supported single-column numeric or string primary keys.

See `examples/listen.rs` for listeners and `examples/custom_version.rs` for adapter registration with `Replication::with_runtime`. Lower-level APIs remain under `config`, `engine`, `model`, `protocol::mysql`, `source`, and `sink`. Complete CLI configuration, compatibility, and output details are documented in the [repository README](https://github.com/tiyee/pikachu-rs#readme).
