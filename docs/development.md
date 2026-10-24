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
├── src/cdc.rs           # Cdc and committed transaction handlers
├── src/config/          # Named sources and CDC policy
├── src/model/           # Exact primary keys and lightweight changes
├── src/protocol/mysql/  # CDC adapters and selective primary-key decoding
├── src/source/          # Source plugins, schema discovery and initial positions
├── src/engine/          # Event conversion, transactions, offline replay and checkpoints
├── src/sink/            # Transaction sinks and JSON Lines output
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

Use `Cdc` for configuration-driven CDC and `TransactionHandler` for committed transactions. `mysql_cdc_rs::replication::BinlogEvent` describes one protocol event; `mysql_cdc_rs::CommittedTransaction` describes converted, committed changes. `engine::converter::EventConverter` explicitly converts binlog views into changes and transaction actions; `Processor` buffers the changes and delivers them only at commit. A QUERY containing DDL is valid in the replication layer; the CDC layer stops with `Error::Statement`.

CDC uses the replication library's `next_view()` network path to borrow the driver body directly, without assembling a second raw buffer. Offline replay uses `next_frame()`. Both paths delay ROWS image parsing until table selection, decode only the primary key and skip other values by their encoded lengths. Table layout metadata is still required to locate fields safely. The selected row-image mode is checked against the upstream setting; replication cannot request individual columns from an existing binlog. See the [replication API](../crates/mysql-replication/README.md) for raw events, parser configuration and transport limits.

For opt-in raw row inspection, `mysql_replication::events::RowsDecoder` returns borrowed, sparse before/after images independently from the CDC path; see [row decoding](events.md#decoding-raw-row-images).

## Performance design

- TABLE_MAP performs exact whitelist lookup first. ROWS for excluded tables bypass bitmap and value parsing. Remapping a `table_id` replaces its previous plan.
- Each table precompiles field-length rules, the key index, and its key decoder. Integer decoding uses const generics for width and signedness. At runtime, each key calls the selected decoder once, without `Any`, downcasting, or type assertions.
- Identical TABLE_MAP contents for a schema reuse the execution plan. Each ROWS event builds its sparse image plan once. NULL bits use compact present-column ordinals.
- Each row visits only present columns and skips other values by their encoded lengths. There is no `Vec<RowValue>` or full-row deserialization.
- Non-key value slices borrow the input buffer. `changed` is a shared `Arc<[u16]>` per event; output maps indices to names without copying names per row.
- `table_id` lookup uses a sorted Vec and binary search. Bitmap iteration uses `trailing_zeros` and clears the lowest set bit one byte at a time, supporting up to 4096 columns.
- Core code forbids `unsafe`. Missing keys, truncated input, metadata mismatches, and output failures return `Result` errors.

Zero-copy applies to core row decoding: the network driver still receives and assembles events, and string keys retained across events require copies. Borrowed callbacks for numeric keys need no per-row heap allocation. The CLI buffers lightweight changes until commit. `max_transaction_bytes` bounds accounted transaction memory, including Vec capacity, key data, and conservative shared-index estimates. It is not a hard process RSS limit. Exceeding the budget stops consumption without advancing the checkpoint.

```sh
cargo bench -p mysql-canal --bench minimal_rows
cargo bench -p mysql-canal --bench event_adapter
```

These benchmarks measure direct Decoder calls and the EventParser + `dyn RowDecoder` dispatch path, respectively, using MINIMAL UPDATE events with 1, 100, or 1000 rows. They exclude networking, JSON serialization, transaction retention, and disk synchronization, and do not promise end-to-end throughput.

The CLI flushes output, writes state, and fsyncs on every transaction. With many small transactions, persistence often costs more than decoding. For higher throughput, implement `TransactionSink` with batched durable storage and acknowledge the transaction before saving the checkpoint. Shared multi-source output takes one lock per transaction. Slow output applies backpressure without an unbounded message queue. Total transaction budgets are the sum of all source budgets, plus driver buffers and metadata. Source transport and version/row adapters dispatch per connection or event; sinks dispatch per transaction. Row callbacks keep borrowed types.

The library's `Decoder::new(Vec<TableMeta>)` accepts whitelisted schemas in column order. `table_map(&[u8])` preprocesses mappings, and `rows(event_type, &[u8], callback)` delivers borrowed `Change` values. Inputs are event bodies with the 19-byte common header and checksum removed; the decoder does not assume full row images. Use `Change::into_owned()` when retaining changes beyond the input buffer's lifetime. Use `Processor` for commit handling, `source::mysql::MysqlSource` for real connections, and `Runtime` for multi-source execution. See `crates/mysql-canal/tests/integration/mysql_live.rs` for the live path.

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

When event structures differ, override `create_event_parser`. A custom `EventParser` produces borrowed `TableMapEvent` and `RowsEvent` views directly from the input, handling offsets, field order, endianness, and new event codes without rebuilding a canonical byte buffer. Control events may return `ParsedEvent::Control`. `classify` handles `Unclassified` events; QUERY/ROTATE/TABLE_MAP/ROWS already recognized by the parser do not call it again.

The default `MysqlEventParser` precompiles TABLE_MAP ID widths, ROWS v1/v2 headers, and QUERY/ROTATE post-header lengths from the FDE into a fixed dispatch table. A ROWS event with a zero post-header length in the FDE is rejected even if its code is known. Core row loops consume normalized views without reading version numbers or wire event types. Image parsers are selected at FDE/event time, and excluded tables are skipped before extra-data, bitmap, or value parsing.

Custom parsers use `RowsEvent::new(..., parse_images)` to provide a function returning borrowed `RowImages`. Reuse the default `Decoder` when MySQL value encodings match. Otherwise, override `create_decoder` and implement `RowDecoder::table_map_event` and `rows_event`. Raw-body methods handle standard layouts; default trait view methods reject nonstandard layouts explicitly. `crates/mysql-canal/tests/integration/event_adapters.rs` uses synthetic vendor formats to cover four-byte IDs, extended control headers, reordered fields, alternate endianness, new ROWS codes, file transitions, and zero-copy UTF-8 keys. These fixtures do not represent real-server compatibility tests.

The transport owns common binlog v4 framing: the 19-byte header, FDE bootstrap, and CRC32/NONE. The current MySQL driver also preparses FDE/TABLE_MAP/ROTATE, so it may reject vendor layout changes before they reach the adapter. Such cases need a custom `SourcePlugin` as well. Registering a `VersionAdapter` alone cannot bypass driver layout restrictions. Default Oracle MySQL adapters continue to enforce validated layouts rather than assuming unknown FDE length changes are safe.

```rust,ignore
let mut runtime = mysql_cdc_rs::engine::Runtime::default();
runtime.versions.register(std::sync::Arc::new(MyVersionAdapter))?;
// runtime.sources.register(Arc::new(MySourcePlugin))?;
runtime.run(config, false, sink)?;
```

Adapter selection is automatic by default. To override a built-in adapter for the same series, set `adapter = "your-adapter"` in that source's configuration. Overlapping matches without an explicit selection produce an ambiguity error; registration order does not silently override adapters. A pinned adapter also constrains historical FDE versions, so recovery across series upgrades generally uses automatic selection with all required adapters registered.

## Making and verifying changes

Place protocol changes in replication, conversion/transaction/recovery changes in canal, and product arguments/default handlers in Pikachu. Check `autotests = false` and explicitly register new integration test targets; facade examples also require registration. Assess semantic-version compatibility for public API changes and update examples and both documentation languages. Keep all four packages at the same version with exact internal path dependency versions.

Run the local checks in the [testing guide](testing.md). Release scripts, CI and packaging procedures are covered by the [release guide](releasing.md) (Chinese).
