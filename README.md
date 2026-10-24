# mysql-cdc

English | [简体中文](README.zh-CN.md)

mysql-cdc is a Rust MySQL CDC system. It consumes binlogs and delivers committed row changes for an exact database/table whitelist, with independent sources, MINIMAL / FULL image policies and checkpoint recovery.

CDC records contain source, database/table, operation, exact primary key and column names. Raw row images can be inspected separately through the replication library. Ordinary SELECT reads produce no CDC changes.

## System components

```text
pikachu (pikachu-cdc) ──> mysql-canal ──> mysql-replication
mysql-cdc-rs ──────────> mysql-canal + mysql-replication
```

| Package | Entry point | Responsibility |
| --- | --- | --- |
| [mysql-replication](crates/mysql-replication/README.md) | `mysql_replication` | Subscribe to and parse binlogs into raw events |
| [mysql-canal](crates/mysql-canal/README.md) | `mysql_canal` | Convert binlogs to normalized CDC events; manage transactions and checkpoints |
| [pikachu](crates/pikachu/README.md) | `pikachu-cdc` | CLI product built on canal, with JSON Lines output |
| [mysql-cdc-rs](crates/mysql-cdc/README.md) | `mysql_cdc_rs` | Unified library entry point re-exporting both libraries |

All four packages are at **2.0.0** and require Rust **1.89+**. Built-in adapters cover Oracle MySQL 5.7, 8.0–8.4, 9.0 and 9.7. See the [testing guide](docs/testing.md) for real-server verification coverage.

## CLI example

Prepare a MySQL instance with binlogs, InnoDB tables and `ROW + MINIMAL` enabled. Server settings and account grants are in the [configuration guide](docs/configuration.md). Save the following as `cdc.toml`, replacing the database/table names. For FULL sources, set `row_image = "full"` and match writer-session settings.

```toml
[mysql-canal]

[[mysql-canal.sources]]
id = "local"
state_file = "local-state.json"

[mysql-canal.sources.source]
plugin = "mysql"

[mysql-canal.sources.mysql-replication]
url_env = "MYSQL_URL"
server_id = 901001
row_image = "minimal"

[[mysql-canal.sources.tables]]
schema = "app"
table = "users"
```

Build and run from the workspace root:

```sh
cargo build --release --locked -p pikachu
export MYSQL_URL='mysql://cdc:password@127.0.0.1:3306'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

Pikachu requires a `mysql-canal` child; every canal source requires a `mysql-replication` child. Each layer defines only its own additions. Source IDs and state paths must be distinct. Paths resolve relative to the configuration file, and state parent directories must exist. See the [multi-source product configuration](crates/pikachu/examples/config/multi-source.toml).

| Command | Purpose |
| --- | --- |
| `pikachu-cdc check cdc.toml` | Validate configuration, server capabilities and table schemas |
| `pikachu-cdc init cdc.toml` | Save the current starting position; reject existing state |
| `pikachu-cdc run cdc.toml` | Consume from checkpoints; initialize missing state |
| `pikachu-cdc run cdc.toml --once` | Consume currently available events and exit |
| `pikachu-cdc replay local-state.json /path/to/binlog.000001` | Replay one file using historical schema without changing state |

Example output:

```json
{"source":"local","schema":"app","table":"users","operation":"update","pk":{"type":"unsigned","value":42},"changed":["name","age"]}
```

`changed` lists fields recorded in the after image; FULL includes fields whose values did not change. See the [event reference](docs/events.md) for fields, key types and before/after semantics.

## Library example

```toml
[dependencies]
mysql-cdc-rs = "2.0.0"
```

```rust,no_run
use mysql_cdc_rs::{Cdc, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let config = AppConfig::read(Path::new("canal.toml"))?;
    let mut cdc = Cdc::new(config)?;
    cdc.add_handle(|transaction: CommittedTransaction<'_>| {
        for change in transaction.changes {
            println!("{} {:?} {:?}", transaction.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    cdc.run()
}
```

`canal.toml` uses the [standalone canal configuration](crates/mysql-canal/examples/config/multi-source.toml), beginning at `[[sources]]`; the Pikachu product file has a different outer layer. CDC APIs are exported at the root and under `canal`; raw APIs are under `replication`. Register a handler before calling `run` or `run_once`.

Fix the mode at construction with `Cdc::<Minimal>::for_mode(config)` or `Cdc::<Full>::for_mode(config)`; every source must match the type. Replication also offers `BinlogFile::<_, Full>::for_mode(...)` and `ReplicationStream::<Full>::for_mode(...)`, carrying the mode through `BinlogEvent` and row images. See the [typed listener](crates/mysql-canal/examples/listen_typed.rs) and [raw row decoder](crates/mysql-replication/examples/decode_typed_rows.rs).

## Usage boundaries

- Initial consumption starts at the current committed position without a full snapshot. Recovery uses historical schema from the checkpoint.
- Complete transactions are delivered after COMMIT; rollback discards changes. Checkpoints are saved after every handler succeeds. Failures can cause replay, so handlers must be idempotent.
- Keys must be single-column integers, exact DECIMAL or CHAR/VARCHAR in supported character sets. DDL, unknown layouts and unsupported data stop consumption with an error.
- Each source owns its connection and checkpoint; there is no global ordering across sources.

## Documentation

- [Documentation index](docs/README.md)
- [Configuration, server setup and replay](docs/configuration.md)
- [Event fields, MINIMAL/FULL differences and output contract](docs/events.md)
- [Architecture and event flow](docs/architecture.md)
- [Development and extensions](docs/development.md)
- [Testing and real-server matrix](docs/testing.md)
- [CI, packaging and releases](docs/releasing.md) (Chinese)
