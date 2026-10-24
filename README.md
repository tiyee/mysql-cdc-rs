# mysql-cdc

English | [简体中文](README.zh-CN.md)

mysql-cdc is a Rust MySQL CDC system. It consumes binlogs and delivers committed row changes for an exact database/table whitelist, with independent sources, MINIMAL / FULL image policies and checkpoint recovery.

CDC records contain source, database/table, operation, exact primary key, changed column names and before/after fields with historical column metadata and logged values. Ordinary SELECT reads produce no CDC changes.

## System components

```text
pikachu (pikachu-cdc) ──> mysql-canal ──> mysql-replication
mysql-cdc-rs ──────────> mysql-canal + mysql-replication
```

| Component | How to use it | Responsibility |
| --- | --- | --- |
| [mysql-replication](crates/mysql-replication/README.md) | Rust library | Subscribe to and parse binlogs into raw events |
| [mysql-canal](crates/mysql-canal/README.md) | Rust library | Convert binlogs to normalized CDC events; manage transactions and checkpoints |
| [pikachu](crates/pikachu/README.md) | `pikachu-cdc` | CLI product built on canal, with JSON Lines output |
| [mysql-cdc-rs](crates/mysql-cdc/README.md) | Unified Rust library | Unified library entry point re-exporting both libraries |

All four package manifests specify **2.0.2** and require Rust **1.89+**. Built-in adapters cover Oracle MySQL 5.7, 8.0–8.4, 9.0 and 9.7. See the [testing guide](docs/testing.md) for real-server verification coverage.

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

## What an update produces

Suppose source `local` has `app.users` with an unsigned integer key. An application updates record 42 and commits:

```sql
UPDATE app.users SET name = 'Bob', age = 21 WHERE id = 42;
```

In MINIMAL mode, the notification identifies the record and fields recorded in the log:

```json
{
  "source": "local",
  "schema": "app",
  "table": "users",
  "operation": "update",
  "pk": {
    "type": "unsigned",
    "value": 42
  },
  "changed": [
    "name",
    "age"
  ],
  "fields": {
    "before": [
      {
        "index": 0,
        "column": {"name": "id", "data_type": "bigint", "column_type": "bigint unsigned", "nullable": false, "primary_key": true, "charset": null, "octet_length": null, "numeric_precision": 20, "numeric_scale": 0, "datetime_precision": null, "extra": ""},
        "value": 42
      }
    ],
    "after": [
      {
        "index": 1,
        "column": {"name": "name", "data_type": "varchar", "column_type": "varchar(32)", "nullable": false, "primary_key": false, "charset": "utf8mb4", "octet_length": 128, "numeric_precision": null, "numeric_scale": null, "datetime_precision": null, "extra": ""},
        "value": "Bob"
      },
      {
        "index": 2,
        "column": {"name": "age", "data_type": "int", "column_type": "int", "nullable": true, "primary_key": false, "charset": null, "octet_length": null, "numeric_precision": 10, "numeric_scale": 0, "datetime_precision": null, "extra": ""},
        "value": 21
      }
    ]
  }
}
```

`pk` identifies the record and `changed` lists the recorded column names. `fields.before` contains the logged key; `fields.after` contains Bob and 21 with their column metadata. In FULL, `changed` also includes unchanged fields present in the complete image. See the [event guide](docs/events.md) for the full comparison.

## Library example

To integrate from local source, use the dependency below; replace `/path/to/mysql-cdc-rs` with your checkout path.

```toml
[dependencies]
mysql-cdc-rs = { path = "/path/to/mysql-cdc-rs/crates/mysql-cdc" }
```

```rust,no_run
use mysql_cdc_rs::canal::{Canal, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let config = AppConfig::read(Path::new("canal.toml"))?;
    let mut canal = Canal::new(config)?;
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

`canal.toml` uses the [standalone canal configuration](crates/mysql-canal/examples/config/multi-source.toml), beginning at `[[sources]]`; the Pikachu product file has a different outer layer. `mysql-cdc-rs` only re-exports the two library modules: `canal::Canal` manages conversion, handlers and checkpoints; `replication` manages raw readers and streams. Each module owns its configuration and error types. Register a handler before calling `run` or `run_once`.

See the [Rust API reference](docs/event-api.md) for fixing mode-specific types and inspecting raw row values.

Raw replication supports statically selected images through `BinlogFile<_, Full>` / `BinlogFile<_, Minimal>` or `ReplicationStream<M>`. Row events bind TABLE_MAP automatically; `rows.rows()` yields typed operations whose fields provide column definitions and borrowed values. MINIMAL distinguishes absent fields from SQL NULL. Missing optional metadata such as column names remains unknown; see the [Rust API reference](docs/event-api.md).

## MINIMAL / FULL event contents

| Mode | Raw row event types | Image contents | Field access |
| --- | --- | --- | --- |
| MINIMAL | `BinlogEvent<Minimal>` / `RowOperation<SparseImage>` | Logged columns, definitions and values | `Result<Option<Field>>`; a valid unlogged column is `None` |
| FULL | `BinlogEvent<Full>` / `RowOperation<CompleteImage>` | All columns, definitions and values in each existing image | `Result<Field>`; incomplete images are rejected |

Compare the [MINIMAL JSON example](docs/events.md#minimal-json) and [FULL JSON example](docs/events.md#full-json) for one UPDATE, including JSON data, structure, before/after fields and access results. The [Rust example](docs/event-api.md#read-concrete-operations-for-each-mode) reads both static types. Canal also exposes logged values through `Change.fields` / `OwnedChange.fields`, with historical SQL column metadata and applicable before/after images.

## Usage boundaries

- Initial consumption starts at the current committed position without a full snapshot. Recovery uses historical schema from the checkpoint.
- Complete transactions are delivered after COMMIT; rollback discards changes. Checkpoints are saved after every handler succeeds. Failures can cause replay, so handlers must be idempotent.
- Keys must be single-column integers, exact DECIMAL or CHAR/VARCHAR in supported character sets. DDL, unknown layouts and unsupported data stop consumption with an error.
- Each source owns its connection and checkpoint; there is no global ordering across sources.

## Documentation

- [Documentation index](docs/README.md)
- [Configuration, server setup and replay](docs/configuration.md)
- [Events and MINIMAL/FULL examples](docs/events.md)
- [How the system works](docs/architecture.md)
- [Development and extensions](docs/development.md)
- [Testing and real-server matrix](docs/testing.md)
- [CI, packaging and releases](docs/releasing.md) (Chinese)
