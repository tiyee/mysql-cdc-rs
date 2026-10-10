# mysql-canal

English | [简体中文](README.zh-CN.md)

MySQL CDC conversion and committed transaction delivery, built on `mysql-replication`. The `pikachu` package supplies the `pikachu-cdc` CLI and its default JSON Lines handler. This library has no default output.

This is the middle layer in `pikachu → mysql-canal → mysql-replication`. The lower library reads protocol bytes and can decode raw row images; canal interprets selected ROWS events using historical SQL schema and converts them into exact keys and changed-column indices. It uses its own selective `RowDecoder`, without first decoding all fields through replication's `RowsDecoder`. Ordinary SELECT reads are outside the CDC change model.

For a single dependency covering CDC and raw replication, use `mysql-cdc-rs` (`mysql_cdc_rs`); this library is re-exported at its root and under `canal`.

```toml
[dependencies]
mysql-canal = "2.0.0"
```

```rust,no_run
use mysql_canal::{Cdc, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut canal = Cdc::new(AppConfig::read(Path::new("cdc.toml"))?)?;
    canal.add_handle(|event: CommittedTransaction<'_>| {
        for change in event.changes {
            println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    canal.run()
}
```

`Cdc::new` accepts `AppConfig`, loaded from TOML or constructed in code. Use the named `[[sources]]` format in [the configuration example](examples/config/multi-source.toml). Every source has an explicit ID, plugin, credentials environment variable, whitelist and checkpoint path. Connections, historical schemas, table maps, transaction budgets and checkpoints are independent across sources; there is no global ordering. Initial consumption starts at the current committed position without a full snapshot; subsequent runs recover from the persisted historical schema and source identity.

`add_handle` accepts a `TransactionHandler` or closure. Callbacks receive a `CommittedTransaction` borrowing the source ID, checkpoint and ordered `OwnedChange` slice. COMMIT/XID delivers the transaction; rollback discards it. Filtered commits deliver an empty slice. Multiple handlers run in registration order, with whole transactions serialized across sources. Every handler must succeed before checkpoint persistence. Failures stop consumption, and a crash after output but before persistence can cause replay. Clone only data retained beyond the callback. Register at least one handler before `run()` or `run_once()`; slow handlers apply backpressure. `check()` validates configuration and server capabilities; `initialize()` creates initial checkpoints without consuming events.

Raw binlog events and CDC changes are separate types. `engine::converter::EventConverter` converts borrowed protocol views, including `mysql_replication::BinlogEvent`, into row changes and transaction actions. `engine::Processor` buffers those changes until commit and persists acknowledged checkpoints. QUERY containing DDL is valid at the protocol layer and stops CDC with `Error::Statement`; it never continues with an outdated schema.

The live source reads `ReplicationStream::next_view()`; offline replay adapts replication's `BinlogFile::next_frame()`. Both feed canal's `EventParser` and selective decoder. `EventConverter` emits `Change<'_>` before commit and returns `EventAction`; it provides no transaction delivery guarantee on its own. `Processor` retains `OwnedChange` values until COMMIT/XID, and `Cdc` delivers `CommittedTransaction` to handlers before saving the checkpoint. `convert_binlog` accepts a structured event by adapting its frame, not by consuming predecoded raw row values. See the [architecture and event flow](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.md).

The MySQL source passes its whitelist to `mysql-replication` for primary-key preflight validation on every connection, including checkpoint recovery, before any initial checkpoint is written or consumption starts. Missing, UNIQUE-only, composite or unsupported keys stop startup. Only selected tables are checked; recovery still decodes using the checkpoint's historical schema. Raw replication callers use `ReplicationStream::connect_with_tables` or `from_connection_with_tables` for the same check.

CDC output contains only the source, schema/table, operation, exact primary key, optional previous key and changed column names. It supports single-column signed/unsigned integers, exact DECIMAL and supported CHAR/VARCHAR keys. INSERT/UPDATE changed fields follow the after-image bitmap, including NULL and primary-key writes; DELETE has no changed fields. Unsupported keys, missing keys, truncated data and metadata mismatches fail explicitly. NULL in a NOT NULL column also fails, including non-key columns in sparse images, without delivering the transaction or advancing its checkpoint. The built-in source supports ROW + MINIMAL or FULL, selected per source; MINIMAL is the default. Table selection occurs before image parsing; only primary-key values are decoded, while other values are skipped by encoded length. UTF-8 keys and event views stay borrowed during conversion; field indices use shared `Arc<[u16]>`. The transport cannot reduce the columns already present in upstream binlog bytes.

Use `Cdc::with_runtime` to register `SourcePlugin` or `VersionAdapter` implementations. `EventParser` handles event layout differences; `RowDecoder` handles field encodings. Each FDE selects its producer's adapter again, including historical files from before an upgrade. Built-in adapters cover validated MySQL 5.7, 8.0–8.4, 9.0 and 9.7 series. The default ROTATE post-header must be exactly 8 bytes. Producer versions require numeric `major.minor.patch` components with optional nonempty `-` or `+` suffixes, and built-in adapters reject MariaDB identifiers. Unknown layouts, compressed transactions, XA, partial JSON and unregistered series are rejected. Real-server verification is separate from synthetic protocol tests. See [custom_version.rs](examples/custom_version.rs), [listen.rs](examples/listen.rs), and the [project guide](https://github.com/tiyee/mysql-cdc-rs#readme) for the CLI, output contract and compatibility matrix.

Validation errors preserve the commit boundary: NULL in a NOT NULL non-key column produces `Error::Schema`, a NULL primary key produces `Error::MissingPrimaryKey`, malformed producer versions produce `Error::Invalid`, and unsupported vendors or FDE layouts produce `Error::Unsupported`. The protocol regressions check error categories and unchanged disk checkpoints separately from real-server compatibility tests.

```sh
cargo test -p mysql-canal --locked
cargo bench -p mysql-canal --bench minimal_rows
cargo bench -p mysql-canal --bench event_adapter
```

Default tests cover synthetic protocol, conversion, transactions, checkpoints, multi-source isolation and adapters. The 12 ignored `mysql_live` scenarios include primary-key rejection through check/init and fresh/resumed consumption. Raw protocol preflight/row decoding and online CLI behavior are tested independently in `mysql-replication` and `pikachu`. The shared MySQL matrix runs all three layers serially. Live tests require a dedicated disposable MySQL server and create/delete test databases. Use the repository's `tests/infra/compose.yaml` and cleanup instructions in the project guide; never point these tests at production.

## Row image selection and migration

Set `row_image = "full"` under `[sources.source]` for a FULL source; use `minimal` or omit the field for MINIMAL. Programmatic `SourceConfig` literals in version 2 must include `row_image: config::RowImageMode::Minimal` or `Full`. The parameter verifies the upstream setting without modifying it. NOBLOB is not a CDC mode.

`Decoder::with_row_image(tables, mode)` and `EventConverter::with_row_image(tables, registry, adapter, mode)` select the policy explicitly. Offline/custom engines use `Processor::with_row_image(state, state_path, budget, registry, adapter, mode)`. Old constructors retain MINIMAL defaults. Custom adapters must implement `validate_server_with_row_image` and `create_decoder_with_row_image` to opt into FULL; the defaults delegate MINIMAL to existing overrides and reject FULL.

FULL validates complete row images and still decodes only primary keys. INSERT/UPDATE `changed` follows the after-image bitmap, so FULL includes unchanged columns; DELETE remains empty. Checkpoints retain v1 historical schemas and do not store mode. Select the historical file's mode for replay and coordinate producer/consumer mode changes at a drained commit boundary.
