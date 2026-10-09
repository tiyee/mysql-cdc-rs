# pikachu

English | [简体中文](README.zh-CN.md)

MySQL CDC command line powered by `mysql-canal`. The `pikachu` package provides the `pikachu-cdc` executable and its default JSON Lines transaction handler.

Build and run from the workspace root:

```sh
cargo build --release --locked -p pikachu
cp crates/pikachu/examples/config/multi-source.toml cdc.toml
export MYSQL_EAST_URL='mysql://cdc:password@127.0.0.1:3306'
export MYSQL_WEST_URL='mysql://cdc:password@127.0.0.1:3307'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

The [configuration example](examples/config/multi-source.toml) uses named sources with independent credentials, whitelists and checkpoints. Changes contain the source, database, table, operation, exact primary key and changed column names. Output appears only after transaction commit; rollback discards buffered changes. Output failures stop consumption, and a crash before checkpoint persistence may cause replay.

Replay a local binlog using a saved historical schema:

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001
```

After publication, install with `cargo install pikachu`. For embedded CDC and raw replication through one dependency, use `mysql-cdc-rs` (`mysql_cdc_rs`); `mysql-canal` and `mysql-replication` remain available separately. See the [project guide](https://github.com/tiyee/mysql-cdc-rs#readme) for setup, the output contract, compatibility and limitations.

Run CLI regression tests with `cargo test --locked -p pikachu`.
