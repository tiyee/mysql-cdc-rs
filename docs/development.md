# Development guide

English | [简体中文](development.zh-CN.md)

[Documentation index](README.md)

Run commands from the repository root. Rust 1.89 or later is required; the root is a pure Cargo workspace. See the [system README](../README.md) for usage, [event reference](events.md) for contracts and [testing guide](testing.md) for verification.

## Project layout

```text
crates/mysql-cdc/
├── src/lib.rs           # Unified CDC and replication exports
└── examples/            # Single-dependency listeners and raw-event readers
crates/pikachu/
├── src/bin/             # pikachu-cdc and its default JSON Lines handle
├── examples/config/     # CLI configuration
└── tests/               # CLI replay tests
crates/mysql-canal/
├── src/config/          # Named sources and CDC policy
├── src/model/           # Exact primary keys, changes and committed transaction views
├── src/protocol/mysql/  # CDC adapters and selective primary-key decoding
├── src/source/          # Source plugins, schema discovery and initial positions
├── src/engine/          # Canal lifecycle, event conversion, replay and checkpoints
├── src/sink/            # Transaction handlers, sinks and JSON Lines output
├── examples/            # CDC listeners, adapters and configuration
├── benches/             # Selective decoding benchmarks
└── tests/               # CDC, conversion and dedicated MySQL tests
crates/mysql-replication/
├── src/events/          # Typed binlog events and opt-in sparse row decoding
├── src/frame.rs         # Raw bytes, bounds and checksums
├── src/parser.rs        # Per-file layouts and configurable lazy parsing
├── src/version.rs       # Shared numeric producer-version validation
├── src/file.rs          # Offline event/frame reader
├── src/stream.rs        # Replication transport and borrowed body views
├── src/primary_key.rs   # Opt-in selected-table primary-key preflight
└── tests/events.rs      # Protocol regression tests
tests/infra/             # Dedicated MySQL test containers
scripts/release/         # Release tag validation and tests
docs/                   # Configuration, events, architecture, development, tests and releases
.github/workflows/       # CI, MySQL compatibility and workspace publishing
```

Use `Canal` for configuration-driven CDC and `TransactionHandler` for committed transactions. `mysql_cdc_rs::replication::BinlogEvent` describes one protocol event; `mysql_cdc_rs::canal::CommittedTransaction` describes converted, committed changes. `engine::converter::EventConverter` explicitly converts binlog views into changes and transaction actions; `Processor` buffers the changes and delivers them only at commit. A QUERY containing DDL is valid in the replication layer; the CDC layer stops with `Error::Statement`.

CDC uses the replication library's `next_view()` network path to borrow the driver body directly, without assembling a second raw buffer. Offline replay uses `next_frame()`. Both paths delay ROWS image parsing until table selection, decode exact keys and validate logged field values, exposing before/after images with historical SQL metadata. Table layout metadata is still required to locate fields safely. The selected row-image mode is checked against the upstream setting; replication cannot request individual columns from an existing binlog. See the [replication API](../crates/mysql-replication/README.md) for raw events, parser configuration and transport limits.

For raw row inspection, `mysql_replication::events::RowsDecoder<M>` returns generic operations with borrowed images independently from the CDC path; see [row decoding](event-api.md#read-events-with-fields).

## Performance design

- TABLE_MAP performs exact whitelist lookup first. ROWS for excluded tables bypass bitmap and value parsing. Remapping a `table_id` replaces its previous plan.
- Each table precompiles field-length rules, the key index, and its key decoder. Integer decoding uses const generics for width and signedness. At runtime, each key calls the selected decoder once, without `Any`, downcasting, or type assertions.
- Identical TABLE_MAP contents for a schema reuse the execution plan. Each ROWS event builds its sparse image plan once. NULL bits use compact present-column ordinals.
- Each row scans present columns for key/constraint validation and borrowed field decoding. There is no per-row `Vec<RowValue>`; temporal, JSON, geometry and vector values retain their encoded representation.
- Non-key value slices borrow the input buffer. `changed` is a shared `Arc<[u16]>` per event; output maps indices to names without copying names per row.
- `table_id` lookup uses a sorted Vec and binary search. Bitmap iteration uses `trailing_zeros` and clears the lowest set bit one byte at a time, supporting up to 4096 columns.
- Core code forbids `unsafe`. Missing keys, truncated input, metadata mismatches, and output failures return `Result` errors.

Zero-copy applies to core row decoding: the network driver still receives and assembles events, and string keys retained across events require copies. Borrowed callbacks for numeric keys need no per-row heap allocation. The CLI buffers lightweight changes until commit. `max_transaction_bytes` bounds accounted transaction memory, including Vec capacity, key data, retained field bytes and conservative shared-layout/index estimates, before allocating owned images. It is not a hard process RSS limit. Exceeding the budget stops consumption without advancing the checkpoint.

```sh
cargo bench -p mysql-canal --bench minimal_rows
cargo bench -p mysql-canal --bench event_adapter
```

These benchmarks measure direct Decoder calls and the EventParser + `dyn RowDecoder` dispatch path, respectively, using MINIMAL UPDATE events with 1, 100, or 1000 rows. They exclude networking, JSON serialization, transaction retention, and disk synchronization, and do not promise end-to-end throughput.

The CLI flushes output, writes state, and fsyncs on every transaction. With many small transactions, persistence often costs more than decoding. For higher throughput, implement `TransactionSink` with batched durable storage and acknowledge the transaction before saving the checkpoint. Shared multi-source output takes one lock per transaction. Slow output applies backpressure without an unbounded message queue. Total transaction budgets are the sum of all source budgets, plus driver buffers and metadata. Source transport and version/row adapters dispatch per connection or event; sinks dispatch per transaction. Row callbacks keep borrowed types.

The library's `Decoder::<M>::new(Vec<TableMeta>)` accepts whitelisted schemas in column order, with `M` set to `Full` or `Minimal`. `table_map_event(TableMapEvent)` preprocesses mappings, and `rows_event(RowsEvent, callback)` delivers borrowed `Change` values. Inputs are normalized views from an FDE-bound `EventParser`. Use `Change::into_owned()` when retaining changes across events. `Canal` manages source lifecycles and transaction handlers, `Processor` handles commits and persistence, and `Runtime` schedules sources. See `crates/mysql-canal/tests/integration/mysql_live.rs` for the live path.

## Version adapters and plugins

Plugins are linked at compile time and registered at runtime through object-safe Rust interfaces. No dylibs are loaded at runtime. Numeric keys retain precise enum types, strings borrow event slices, and interfaces do not use `Any` or intermediate JSON values.

| Interface | Responsibility |
| --- | --- |
| `SourcePlugin` / `SourceSession` / `EventStream` | Connect, read schemas and starting positions, and deliver borrowed events; replace the driver or customize connection behavior |
| `VersionAdapter` | Match versions, validate capabilities and FDE layouts, choose position queries, and create event parsers and row decoders |
| `EventParser` | Normalize event structures into borrowed `ParsedEvent` views: `TableMapEvent`, `RowsEvent`, or control events |
| `RowDecoder` | Preprocess TABLE_MAP and decode ROWS from normalized views, emitting borrowed `Change<'_>` values |
| `TransactionSink` | Receive a source ID, committed file/position, and a batch of lightweight `OwnedChange` values; state is saved only after success |

`VersionRegistry` includes `mysql57`, `mysql80`, `mysql81`, `mysql82`, `mysql83`, `mysql84`, `mysql90`, and `mysql97`. Connections select an adapter using `SELECT VERSION()`. Each binlog file selects again from its FDE producer version, allowing recovery from logs written before a server upgrade. MySQL 5.7 and 8.0–8.3 use `SHOW MASTER STATUS`; 8.4, 9.0, and 9.7 use `SHOW BINARY LOG STATUS`.

Adapters check ROW and the selected MINIMAL/FULL settings, transaction compression and partial-JSON options, the common header, and relevant post-header lengths, including an exact 8-byte ROTATE post-header. Raw parsing and CDC use the same complete numeric `major.minor.patch` syntax, with optional nonempty `-` or `+` suffixes; built-in parsers and adapters reject MariaDB producer identifiers. Custom adapters may explicitly validate other layouts. The 9.7 adapter reuses strictly validated binlog v4 layouts; see the [MySQL 9.7.2 format description source](https://raw.githubusercontent.com/mysql/mysql-server/mysql-9.7.2/libs/mysql/binlog/event/control_events.cpp). Real-server compatibility remains subject to the CI matrix.

Unregistered series are rejected. A condition such as `major >= 5` is not sufficient to establish compatibility. Oracle MySQL has no 5.8 series; 5.7.8 belongs to 5.7. See the [official version reference](https://downloads.mysql.com/docs/mysqld-version-reference-en.pdf). Vendor versions require adapters based on their actual versions and log layouts.

To add a version, implement `VersionAdapter` and call `runtime.versions.register(Arc::new(adapter))`. Each FDE creates a file-specific parser through `create_event_parser`; `create_decoder` handles column metadata and value encoding. `crates/mysql-canal/examples/custom_version.rs` provides a compilable template. Registration alone does not establish compatibility.

When event structures differ, override `create_event_parser`. A custom `EventParser` produces borrowed `TableMapEvent` and `RowsEvent` views directly from the input, handling offsets, field order, endianness, and new event codes without rebuilding a canonical byte buffer. Control events may return `ParsedEvent::Control`. The converter handles the returned transaction or log action directly.

The default `MysqlEventParser` precompiles TABLE_MAP ID widths, ROWS v1/v2 headers, and QUERY/ROTATE post-header lengths from the FDE into a fixed dispatch table. A ROWS event with a zero post-header length in the FDE is rejected even if its code is known. Core row loops consume normalized views without reading version numbers or wire event types. Image parsers are selected at FDE/event time, and excluded tables are skipped before extra-data, bitmap, or value parsing.

Custom parsers use `RowsEvent::new(..., parse_images)` to provide a function returning borrowed `RowImages`. Reuse the default `Decoder` when MySQL value encodings match. Otherwise, override `create_decoder` and implement `RowDecoder::table_map_event` and `rows_event`. `RowDecoder` requires `table_map_event` and `rows_event`; all wire parsing belongs to the FDE-bound `EventParser`. `crates/mysql-canal/tests/integration/event_adapters.rs` uses synthetic vendor formats to cover four-byte IDs, extended control headers, reordered fields, alternate endianness, new ROWS codes, file transitions, and zero-copy UTF-8 keys. These fixtures do not represent real-server compatibility tests.

The transport owns common binlog v4 framing: the 19-byte header, FDE bootstrap, and CRC32/NONE. The current MySQL driver also preparses FDE/TABLE_MAP/ROTATE, so it may reject vendor layout changes before they reach the adapter. Such cases need a custom `SourcePlugin` as well. Registering a `VersionAdapter` alone cannot bypass driver layout restrictions. Default Oracle MySQL adapters continue to enforce validated layouts rather than assuming unknown FDE length changes are safe.

```rust,ignore
let mut runtime = mysql_cdc_rs::canal::engine::Runtime::default();
runtime.versions.register(std::sync::Arc::new(MyVersionAdapter))?;
// runtime.sources.register(Arc::new(MySourcePlugin))?;
runtime.run(config, false, sink)?;
```

Adapter selection is automatic by default. To override a built-in adapter for the same series, set `adapter = "your-adapter"` in that source's configuration. Overlapping matches without an explicit selection produce an ambiguity error; registration order does not silently override adapters. A pinned adapter also constrains historical FDE versions, so recovery across series upgrades generally uses automatic selection with all required adapters registered.

Custom adapters implement `validate_server(variables, consumer_id, row_image)` and `create_decoder(tables, format, row_image)`. The common v4 defaults support both policies; custom restrictions must be explicit. `RowDecoder` accepts normalized views only, and `EventParser` owns all structural/control classification.

## Making and verifying changes

Place protocol changes in replication, conversion/transaction/recovery changes in canal, and product arguments/default handlers in Pikachu. Check `autotests = false` and explicitly register new integration test targets; facade examples also require registration. Before the initial release, organize public APIs around the current design and keep examples and both documentation languages in sync. Keep all four packages at the same version with exact internal path dependency versions.

Run the local checks in the [testing guide](testing.md). Release scripts, CI and packaging procedures are covered by the [release guide](releasing.md) (Chinese).

## Implementation data flow

### Replication: bytes to protocol events and raw rows

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
ROWS ───────> RowsEvent::rows ──> Row<M> { operation }
                                      └─> RowImage::fields ──> Field / RowValue
```

[`RowsKind`](../crates/mysql-replication/src/events/rows.rs) distinguishes `Write`, `Update` and `Delete`. WRITE has an after image; UPDATE pairs before/after images; DELETE has a before image. The [raw decoder](../crates/mysql-replication/src/events/rows/decoder.rs) exposes present fields with absolute zero-based column indices. Missing columns remain absent, and a present SQL NULL is `RowValue::Null`. MINIMAL images cannot reconstruct a complete old/new row.

TABLE_MAP provides wire layouts, but the raw decoder does not guess missing SQL signedness, column names or character sets. Callers choose signed/unsigned integer interpretation from their SQL metadata. Strings remain bytes; temporal, JSON, geometry and vector values remain encoded bytes. Exact DECIMAL values retain precision and scale. Raw decoding does not require a primary key or manage transactions, output handlers or checkpoints. See the [decoder example](../crates/mysql-replication/examples/decode_rows.rs).

For `BinlogParser<Full>` / `BinlogParser<Minimal>`, a static mode strategy owns source-local TABLE_MAP state, binds each ROWS event to its historical plan, replaces remaps and clears file/statement boundaries. `RowsEvent<M>::rows()` returns `Row { operation: RowOperation<M::Image> }`; `fields()` exposes column metadata plus borrowed values. FULL/MINIMAL dispatch is static. Explicit `RowsDecoder<M>` consumers maintain their own mappings. Optional names, signedness, collations and primary-key metadata are decoded when present; unknown attributes remain in the original TLVs. Missing attributes are never inferred from the current schema. Raw iteration can yield rows before a later error and provides no commit visibility guarantee.

The separate `primary_key::validate_primary_keys` helper performs optional INFORMATION_SCHEMA preflight before a caller opens raw transport. It does not filter events. Canal owns its separate source preflight and opens `ReplicationConnection` only after validation. Raw decoding remains independent of primary-key policy.

### Canal: protocol views to committed CDC changes

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
                                      └─> Canal handlers ──> save checkpoint
```

[`EventConverter`](../crates/mysql-canal/src/engine/converter.rs) selects adapters from each FDE's producer version. The [`EventParser`](../crates/mysql-canal/src/protocol/mysql/events/parser.rs) normalizes version-specific event layouts. The [`RowDecoder`](../crates/mysql-canal/src/protocol/mysql/version.rs) interface and built-in [`Decoder`](../crates/mysql-canal/src/protocol/mysql/decoder.rs) apply historical SQL schema and exact table selection. Excluded tables are skipped before row-image parsing. Selected rows validate exact keys, NULL constraints, field encodings and byte boundaries, then attach borrowed field images using the same static `Decoder<M>` policy.

`Change<'_>` contains table metadata, operation, exact primary key, optional previous key, shared changed-column indices and `fields: RowOperation<FieldImage>`. INSERT/UPDATE indices follow the after-image bitmap; DELETE has no changed fields. An after-image field does not necessarily differ from its old value. Source identity is supplied by the transaction; the JSON sink resolves column indices to names. Each present field exposes its absolute index, historical `ColumnMeta` and `RowValue`. `Change::into_owned()` copies compact image bytes and shares immutable layouts; the transaction budget includes image bytes and conservatively charged layouts, checked before copying.

Conversion alone is not committed delivery. `EventConverter::convert_binlog` accepts a structured `BinlogEvent`, converts its frame to the CDC view and uses canal's adapters; it does not consume predecoded raw `RowValue` objects. Its callbacks can run before COMMIT and do not buffer or discard rolled-back rows. Use [`Processor`](../crates/mysql-canal/src/engine/processor.rs) or [`Canal`](../crates/mysql-canal/src/engine/canal.rs) for transaction delivery.

`Processor` turns borrowed changes into owned transaction data only when retaining them across events. COMMIT/XID passes a complete transaction to `TransactionSink`. `Canal`'s sink invokes registered `TransactionHandler`s in order with a borrowing `CommittedTransaction`; all must succeed before the checkpoint is saved. Fully filtered commits still deliver an empty change slice. Rollback discards the buffer and may save the rollback position. DDL and other unsupported statements stop CDC with `Error::Statement`; errors and incomplete transactions do not persist an unacknowledged commit. Output followed by a crash before checkpoint persistence can replay, so handlers must tolerate duplicates.

### Startup, recovery and source isolation

[`Runtime`](../crates/mysql-canal/src/engine/runtime.rs) coordinates named `[[sources]]`. Each source owns a connection/session, historical schema, TABLE_MAP plans, transaction budget, checkpoint and state-file lock.

1. Validate configuration and connect all sources. The built-in [`MysqlSource`](../crates/mysql-canal/src/source/mysql/mod.rs) checks server capabilities and selected tables' current primary keys, including on recovery. This preflight runs before runtime initialization writes initial checkpoints; the canal source checks selected keys again before requesting binlog bytes.
2. Acquire state-file locks. For saved checkpoints, verify source UUID, ID, plugin and whitelist. Load the historical schema from that checkpoint, without replacing it with current INFORMATION_SCHEMA metadata.
3. For a missing checkpoint, briefly acquire a global read lock and capture selected table schemas and the current committed position. Save the initial state. There is no full-data snapshot, and initialization across sources is not one consistent snapshot.
4. Start a worker per source and consume from its saved position. Re-select the producer adapter at each file's FDE. Deliver whole transactions through the shared sink, without interleaving their handlers across sources. There is no global event order across instances.
5. A worker failure requests peers to stop between events and propagates failure. Slow handlers apply backpressure; transaction budgets bound accounted buffers. There is no silent reconnect or unbounded event queue.

Current-primary-key preflight is a startup eligibility check; saved historical schema still determines how historical bytes are decoded. Schema changes are not automatically adopted at DDL boundaries.

### Pikachu and facade integration

[`pikachu-cdc`](../crates/pikachu/src/bin/pikachu-cdc.rs) maps `check`, `init`, `run` and `run --once` to `Canal`. It registers [`PrintHandle`](../crates/pikachu/src/bin/print_handle/mod.rs) for consumption. That handler delegates to canal's `JsonLinesSink`, writes change records to stdout and flushes before checkpoint persistence; flushing is not downstream durable acknowledgement. Diagnostics go to stderr.

`replay` delegates to canal's [`engine::Replay`](../crates/mysql-canal/src/engine/replay.rs). Replay owns file-name and event-boundary checks, FDE bootstrapping, historical-event skipping and transaction delivery through `Processor`. It uses the supplied checkpoint's historical schema and an explicit `TransactionSink`, without updating the state file or querying the current server. Positions are checked before processing each event, including commits; later errors can still follow earlier successful commits. The CLI contains no event loop or protocol dispatch.

Use `mysql-replication` for protocol inspection, `mysql-canal` for embedded committed CDC, or [`mysql-cdc-rs`](../crates/mysql-cdc/src/lib.rs) for both through one dependency. The aggregate only re-exports `canal` and `replication`; each module owns its construction, configuration and error types. Use `canal::Canal::new`, `canal::Error` / `canal::Result`, or the readers, streams and error types under `replication`.

Generic entry points bind shared `Minimal` / `Full` types at construction. Replication carries `M` through parser, reader, `BinlogEvent`, `EventData`, `RowsEvent` and owned views. `Event` remains the shared object-safe decoding interface for common control bodies. Canal exposes `Decoder<M>` for typed row decoding. `Canal::new(AppConfig)` orchestrates sources, each with its own replication image setting. Built-in adapters assemble `Decoder<Full>` or `Decoder<Minimal>` per source and FDE, while normalized changes and transaction delivery retain shared types. A single consumer can include both modes; dynamic adapter interfaces remain object safe.

### Image policies

`mysql_replication::config::RowImageMode`, re-exported by canal, is source-local under `[sources.mysql-replication]` and defaults to MINIMAL. Runtime passes it to online server checks and to Processor → EventConverter → VersionAdapter → Decoder<M>; each FDE rebinds the decoder with the same policy. Offline replay supplies the mode explicitly. Raw `RowsEvent` and `RowsDecoder` remain bitmap-driven and independent of this CDC policy. FULL requires complete images and exposes all their field values while retaining borrowed UTF-8 keys and shared after-image column indices. Checkpoints store historical schemas; the mode is a runtime option, not a property inferred from the current server or FDE.
