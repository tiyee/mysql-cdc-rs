# Architecture and event flow

English | [简体中文](architecture.zh-CN.md)

The system is named **mysql-cdc**; `mysql-cdc-rs` is its facade package, not a fourth implementation layer. This guide describes the current implementation. See the [project README](../README.md) for configuration, output semantics and compatibility, and the [release guide](releasing.md) for CI and publishing.

## Package layers

Arrows indicate Cargo dependencies, from consumer to provider:

```text
pikachu (pikachu-cdc) ──> mysql-canal ──> mysql-replication
mysql-cdc-rs ──────────> mysql-canal
mysql-cdc-rs ─────────────────────────> mysql-replication
```

| Package / Rust import | Input | Responsibility and output |
| --- | --- | --- |
| `mysql-replication` / `mysql_replication` | MySQL replication connection or local binlog bytes | Validate frames, parse protocol events, optionally decode sparse raw insert/update/delete images |
| `mysql-canal` / `mysql_canal` | Borrowed event views, selected table schemas and source configuration | Select tables, decode exact keys and changed fields, buffer transactions, deliver committed changes and persist checkpoints |
| `pikachu` / executable `pikachu-cdc` | CLI arguments and TOML/state/binlog files | Dispatch commands to canal and register its default JSON Lines handler |
| `mysql-cdc-rs` / `mysql_cdc_rs` | Calls to the two library APIs | Re-export canal at the root and under `canal`, and replication under `replication`, preserving their types and lifetimes |

`mysql-cdc-rs` adds no processing stage. `pikachu` depends directly on `mysql-canal`. The root repository is a Cargo workspace, not another library package.

## Configuration composition

Replication owns `config::Config` and `RowImageMode`, including connection/TLS settings, consumer server ID, timeouts and MINIMAL/FULL selection. Canal includes that type as its mandatory `Config::replication` field, re-exported as `config::ReplicationConfig`. It does not duplicate those fields in `SourceConfig`; source plugin/adapter selection, table filters, transaction budget and checkpoint paths remain canal settings.

Pikachu composes a required `mysql-canal: AppConfig` child, whose named sources each compose a required `mysql-replication` child. Product settings contain only product additions (currently none are required); no layer redefines its child settings. Product file example:

```toml
[mysql-canal]

[[mysql-canal.sources]]
id = "east"
state_file = "east-state.json"

[mysql-canal.sources.source]
plugin = "mysql"

[mysql-canal.sources.mysql-replication]
url_env = "MYSQL_EAST_URL"
server_id = 901001
row_image = "full"
connect_timeout_secs = 10
read_timeout_secs = 90

[[mysql-canal.sources.tables]]
schema = "app"
table = "users"
```

A standalone canal file starts at `[[sources]]`, with `[sources.mysql-replication]` under each source; Pikachu embeds this tree under `mysql-canal` as above. Product and standalone examples live in `crates/pikachu/examples/config/` and `crates/mysql-canal/examples/config/`. Missing child configurations, unknown fields and fields in the wrong layer are rejected. `AppConfig::resolve_paths` supports embedding relative to the outer TOML file; `AppConfig::read` reads the standalone format. The selected replication mode also reaches canal's adapter and selective decoder, giving the transport and CDC path one source of truth. File/position come from the checkpoint. Raw typed `Minimal`/`Full` decoders remain separate, checking sparse versus complete images without CDC key policy.

The [public event-field reference](../crates/mysql-replication/README.md#public-event-fields-and-image-modes) distinguishes the `BinlogEvent` envelope, field-free `Event` trait and `EventData` bodies. It includes shared header/body fields, MINIMAL/FULL image types, accessor return types and absent-image/absent-column/SQL-NULL semantics.

## Replication: bytes to protocol events and raw rows

The transport entry point is [`ReplicationStream`](../crates/mysql-replication/src/stream.rs); offline files use [`BinlogFile`](../crates/mysql-replication/src/file.rs).

| Reader API | Result | Parsing boundary |
| --- | --- | --- |
| `next_event()` | `BinlogEvent<'_>` | Original raw bytes, common header and typed `EventData`; ROWS image parsing is deferred by default |
| `next_frame()` | `BinlogFrame<'_>` | Validated complete raw frame; body interpretation remains with the caller |
| Network `next_view()` | `mysql_replication::EventView<'_>` | Validated driver header/body view, avoiding a second complete raw-frame assembly |

[`BinlogParser`](../crates/mysql-replication/src/parser.rs) uses FORMAT_DESCRIPTION (FDE) to select each file's layout. Typed events include QUERY, TABLE_MAP, ROWS and transaction/control events. A QUERY is a protocol event containing SQL bytes; its name does not mean a SELECT result or a CDC read operation. Ordinary SELECT reads do not produce insert/update/delete CDC changes. Protocol parsing can return DDL as QUERY; stopping CDC at that boundary is canal's responsibility.

Raw row decoding is opt-in:

```text
TABLE_MAP ──> RowsDecoder::new ──> compiled column layout
ROWS ───────> RowsEvent::rows ──> Row { before, after }
                                      └─> RowImage::fields ──> RowField / RowValue
```

[`RowsKind`](../crates/mysql-replication/src/events/rows.rs) distinguishes `Write`, `Update` and `Delete`. WRITE has an after image; UPDATE pairs before/after images; DELETE has a before image. The [raw decoder](../crates/mysql-replication/src/events/rows/decoder.rs) exposes present fields with absolute zero-based column indices. Missing columns remain absent, and a present SQL NULL is `RowValue::Null`. MINIMAL images cannot reconstruct a complete old/new row.

TABLE_MAP provides wire layouts, but the raw decoder does not infer SQL signedness, column names or character sets. Callers choose signed/unsigned integer interpretation from their SQL metadata. Strings remain bytes; temporal, JSON, geometry and vector values remain encoded bytes. Exact DECIMAL values retain precision and scale. Raw decoding does not require a primary key or manage transactions, output handlers or checkpoints. See the [decoder example](../crates/mysql-replication/examples/decode_rows.rs).

Callers maintain TABLE_MAP plans independently per source, replace remapped IDs, and clear plans at file/statement boundaries. Raw row iteration can yield rows before a later error and provides no commit visibility guarantee.

The separate [`primary_key`](../crates/mysql-replication/src/primary_key.rs) helper validates selected tables through INFORMATION_SCHEMA. `connect_with_tables` / `from_connection_with_tables` opt into this preflight; their table list does not filter the replication stream. Plain `connect` / `from_connection` remain available without preflight. These existing optional APIs remain available for raw-library callers. Canal owns a separate source-level preflight that reuses its CDC key decoder's type rules; its normal stream uses `from_connection` after that check. The raw decoder remains independent of primary-key policy.

## Canal: protocol views to committed CDC changes

The normal CDC path does not pass through the raw `RowsDecoder`:

```text
Online:  ReplicationStream::next_view ──> canal EventView
Offline: replication BinlogFile::next_frame ──> canal EventView
                                                     │
                                                     v
                        EventConverter (per source)
                        ├─ FDE: select VersionAdapter, EventParser and RowDecoder
                        ├─ TABLE_MAP: match whitelist and compile table plan
                        ├─ ROWS: emit borrowed Change values
                        └─ control events: return EventAction
                                                     │
                                                     v
                        Processor + TransactionBuffer
                        ├─ retain OwnedChange until commit
                        ├─ rollback: discard buffered changes
                        └─ commit: TransactionSink
                                      └─> Cdc handlers ──> save checkpoint
```

[`EventConverter`](../crates/mysql-canal/src/engine/converter.rs) selects adapters from each FDE's producer version. The [`EventParser`](../crates/mysql-canal/src/protocol/mysql/events/parser.rs) normalizes version-specific event layouts. The [`RowDecoder`](../crates/mysql-canal/src/protocol/mysql/version.rs) interface and built-in [`Decoder`](../crates/mysql-canal/src/protocol/mysql/decoder.rs) apply historical SQL schema and exact table selection. Excluded tables are skipped before row-image parsing. Selected rows decode only the key and skip non-key values by encoded length, while still checking NULL constraints and byte boundaries.

`Change<'_>` contains table metadata, operation, exact primary key, optional previous key and shared changed-column indices. INSERT/UPDATE indices follow the after-image bitmap; DELETE has no changed fields. An after-image field does not necessarily differ from its old value. Source identity is supplied by the transaction; the JSON sink resolves column indices to names. CDC does not expose full row objects or non-key field values.

Conversion alone is not committed delivery. `EventConverter::convert_binlog` accepts a structured `BinlogEvent`, converts its frame to the CDC view and uses canal's adapters; it does not consume predecoded raw `RowValue` objects. Its callbacks can run before COMMIT and do not buffer or discard rolled-back rows. Use [`Processor`](../crates/mysql-canal/src/engine/processor.rs) or [`Cdc`](../crates/mysql-canal/src/cdc.rs) for transaction delivery.

`Processor` turns borrowed changes into owned transaction data only when retaining them across events. COMMIT/XID passes a complete transaction to `TransactionSink`. `Cdc`'s sink invokes registered `TransactionHandler`s in order with a borrowing `CommittedTransaction`; all must succeed before the checkpoint is saved. Fully filtered commits still deliver an empty change slice. Rollback discards the buffer and may save the rollback position. DDL and other unsupported statements stop CDC with `Error::Statement`; errors and incomplete transactions do not persist an unacknowledged commit. Output followed by a crash before checkpoint persistence can replay, so handlers must tolerate duplicates.

## Startup, recovery and source isolation

[`Runtime`](../crates/mysql-canal/src/engine/runtime.rs) coordinates named `[[sources]]`. Each source owns a connection/session, historical schema, TABLE_MAP plans, transaction budget, checkpoint and state-file lock.

1. Validate configuration and connect all sources. The built-in [`MysqlSource`](../crates/mysql-canal/src/source/mysql/mod.rs) checks server capabilities and selected tables' current primary keys, including on recovery. This preflight runs before runtime initialization writes initial checkpoints; the canal source checks selected keys again before requesting binlog bytes.
2. Acquire state-file locks. For saved checkpoints, verify source UUID, ID, plugin and whitelist. Load the historical schema from that checkpoint, without replacing it with current INFORMATION_SCHEMA metadata.
3. For a missing checkpoint, briefly acquire a global read lock and capture selected table schemas and the current committed position. Save the initial state. There is no full-data snapshot, and initialization across sources is not one consistent snapshot.
4. Start a worker per source and consume from its saved position. Re-select the producer adapter at each file's FDE. Deliver whole transactions through the shared sink, without interleaving their handlers across sources. There is no global event order across instances.
5. A worker failure requests peers to stop between events and propagates failure. Slow handlers apply backpressure; transaction budgets bound accounted buffers. There is no silent reconnect or unbounded event queue.

Current-primary-key preflight is a startup eligibility check; saved historical schema still determines how historical bytes are decoded. Schema changes are not automatically adopted at DDL boundaries.

## Pikachu and facade integration

[`pikachu-cdc`](../crates/pikachu/src/bin/pikachu-cdc.rs) maps `check`, `init`, `run` and `run --once` to `Cdc`. It registers [`PrintHandle`](../crates/pikachu/src/bin/print_handle/mod.rs) for consumption. That handler delegates to canal's `JsonLinesSink`, writes change records to stdout and flushes before checkpoint persistence; flushing is not downstream durable acknowledgement. Diagnostics go to stderr.

`replay` delegates to canal's [`engine::Replay`](../crates/mysql-canal/src/engine/replay.rs). Replay owns file-name and event-boundary checks, FDE bootstrapping, historical-event skipping and transaction delivery through `Processor`. It uses the supplied checkpoint's historical schema and an explicit `TransactionSink`, without updating the state file or querying the current server. Positions are checked before processing each event, including commits; later errors can still follow earlier successful commits. The CLI contains no event loop or protocol dispatch.

Use `mysql-replication` for protocol inspection, `mysql-canal` for embedded committed CDC, or [`mysql-cdc-rs`](../crates/mysql-cdc/src/lib.rs) for both through one dependency. The facade's root `Error`/`Result` are canal's; raw operations use `replication::Error`/`replication::Result`.

Generic entry points bind shared `Minimal` / `Full` types at construction. Replication carries `M` through parser, reader, `BinlogEvent`, `EventData`, `RowsEvent` and owned views. `Event` remains the shared object-safe decoding interface for common control bodies. Canal exposes `Cdc<M>` and `Decoder<M>`; fixed CDC instances reject source configuration conflicts before startup. Built-in adapters assemble generic decoders per FDE, while normalized changes and transaction delivery retain shared types. The default `Configured` CDC entry point permits mixed sources; dynamic adapter interfaces remain object safe.

## Image policies

`mysql_replication::config::RowImageMode`, re-exported by canal, is source-local under `[sources.mysql-replication]` and defaults to MINIMAL. Runtime passes it to online server checks and to Processor → EventConverter → VersionAdapter → Decoder; each FDE rebinds the decoder with the same policy. Offline replay supplies the mode explicitly. Raw `RowsEvent` and `RowsDecoder` remain bitmap-driven and independent of this CDC policy. FULL requires complete images but retains selective key decoding, borrowed UTF-8 keys and shared after-image column indices. Checkpoints retain their v1 format and historical schemas; the mode is a runtime option, not a property inferred from the current server or FDE.
