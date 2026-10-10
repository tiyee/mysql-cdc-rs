# pikachu

English | [简体中文](README.zh-CN.md)

MySQL CDC command line powered by `mysql-canal`. The `pikachu` package provides the `pikachu-cdc` executable and its default JSON Lines transaction handler.

This is the top layer in `pikachu → mysql-canal → mysql-replication`. Replication handles subscription, protocol parsing and optional raw row decoding; canal handles table selection, selective row conversion, transaction buffering and checkpoints. The CLI maps commands to `Cdc` or offline `Processor` replay and delegates output to canal's `JsonLinesSink` through `PrintHandle`. See the [architecture guide](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.md).

Build and run from the workspace root:

```sh
cargo build --release --locked -p pikachu
cp crates/pikachu/examples/config/multi-source.toml cdc.toml
export MYSQL_EAST_URL='mysql://cdc:password@127.0.0.1:3306'
export MYSQL_WEST_URL='mysql://cdc:password@127.0.0.1:3307'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

Edit `[[sources.tables]]` to use the exact database/table names you want to monitor before running these commands. A single source still uses `[[sources]]`. Source IDs and state paths must be distinct, and state parent directories must exist. Paths in the configuration are relative to the TOML file.

| Command | Behavior |
| --- | --- |
| `pikachu-cdc check CONFIG.toml` | Validate configuration, server capabilities and table schemas without consuming events |
| `pikachu-cdc init CONFIG.toml` | Save initial schemas and current committed positions; reject existing state files |
| `pikachu-cdc run CONFIG.toml` | Initialize missing states and consume continuously from saved positions |
| `pikachu-cdc run CONFIG.toml --once` | Consume currently available events and exit |
| `pikachu-cdc replay STATE.json BINLOG_FILE` | Replay one local binlog without modifying state |
| `pikachu-cdc --help` | Print command usage |

The first `init` or `run` captures the current position under a brief global read lock; it does not take a full-data snapshot. Later runs use the saved historical schema and source identity. Keep the binlogs required by every source's checkpoint.

Every online MySQL connection checks the selected tables' current primary keys, including checkpoint recovery; missing or unsupported keys stop startup. Recovery still decodes historical events using the schema saved in the checkpoint. Local replay uses that saved schema without connecting to MySQL.

The [configuration example](examples/config/multi-source.toml) uses named sources with independent credentials, whitelists and checkpoints. Changes contain the source, database, table, operation, exact primary key and changed column names. Output appears only after transaction commit; rollback discards buffered changes. Output failures stop consumption, and a crash before checkpoint persistence may cause replay.

Replay a local binlog using a saved historical schema:

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001
```

The filename must match `state.file`, and the file must retain its initial FORMAT_DESCRIPTION. Replay begins at the saved event boundary and fails on truncation or an incomplete transaction. Each invocation reads one file for one source.

Consumption errors are written to stderr and produce a nonzero exit status; change records use stdout. The default handler flushes stdout before saving a checkpoint, but this is not downstream durable acknowledgement. Consumers must tolerate replay. Any QUERY other than BEGIN/COMMIT/ROLLBACK stops CDC with `Error::Statement`; DDL does not automatically refresh the schema and continue.

Once version 1.0.0 is available on crates.io, install with `cargo install pikachu --version 1.0.0 --locked`. The installed command is `pikachu-cdc`. For embedded CDC and raw replication through one dependency, use `mysql-cdc-rs` (`mysql_cdc_rs`); `mysql-canal` and `mysql-replication` remain available separately. See the [project guide](https://github.com/tiyee/mysql-cdc-rs#readme) for setup, the output contract, compatibility and limitations.

Run CLI regression tests with `cargo test --locked -p pikachu`.

Two ignored `mysql_live` tests exercise the built executable against a dedicated disposable MySQL server: check/init/run, complete JSON Lines, filtering, rollback, checkpoint recovery, DDL stops and primary-key startup failures for fresh and existing states. The shared MySQL matrix runs replication, CDC and CLI tests serially. Fixtures create/drop databases and remove temporary state; use the [project testing guide](https://github.com/tiyee/mysql-cdc-rs#testing) for setup and container cleanup.
