# pikachu-rs

English | [简体中文](README.zh-CN.md)

A Rust library and CLI for MySQL change data capture. It keeps only **database names, table names, single-column primary keys, and changed column names**, without materializing full rows. Built-in `ROW + MINIMAL` adapters cover Oracle MySQL 5.7, 8.0, 8.1–8.4, 9.0, and 9.7, with concurrent consumption from multiple instances. Protocol and simulated-source tests are described below; real-server compatibility tests must be run separately.

## Project layout

```text
src/
├── bin/pikachu-cdc.rs     # CLI entry point
├── config/               # Configuration parsing and validation
├── model/                # Changes, primary key types, table and column metadata
├── protocol/mysql/       # Event constants, binlog framing, row decoding, version adapters
│   └── events/           # Event structure parsers and normalized borrowed views
├── source/               # Source plugin interfaces and registry
│   └── mysql/            # MySQL connections, schema queries, and event streams
├── engine/               # Multi-source scheduling, transactions, and checkpoints
├── sink/                 # Output interface and JSON Lines implementation
├── error.rs              # Public error types
└── lib.rs                # Library entry point and public exports
examples/
├── config/               # Multi-source TOML configuration
└── custom_version.rs     # Custom version adapter registration
tests/
├── integration/          # CLI, protocol, transaction, multi-source, and live tests
├── support/              # Protocol builders and real-MySQL fixtures
└── infra/compose.yaml    # Dedicated MySQL test environment
benches/                  # Core decoding benchmarks
scripts/release/          # Release tag validation and tests
docs/releasing.md         # CI matrix and crates.io publishing guide (Chinese)
.github/workflows/        # CI, MySQL compatibility matrix, and crate publishing
```

Integrate through `engine`, `model`, `protocol::mysql`, `source`, and `sink`. The initial `0.0.1` release uses a multi-source architecture with an explicit identity and independent checkpoint for every source.

## Usage

The public crate is named `pikachu-rs`; its Rust import path is `pikachu_rs`:

```toml
[dependencies]
pikachu-rs = "0.0.1"
```

The package also provides the `pikachu-cdc` CLI. Start with `examples/config/multi-source.toml`. Each `[[sources]]` entry has its own `id`, `state_file`, `source`, and `tables`:

```sh
cargo build --release --locked
cp examples/config/multi-source.toml cdc.toml
# Set each source's [[sources.tables]] to exact database and table names, respecting server case rules.
export MYSQL_EAST_URL='mysql://cdc:password@east.example:3306'
export MYSQL_WEST_URL='mysql://cdc:password@west.example:3306'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

All configurations use the `[[sources]]` list, including deployments with just one named source. Each entry must explicitly specify an `id` and the credentials environment variable in `sources.source.url_env`. Source IDs and state paths must be distinct, and state parent directories must already exist. Subscriptions to the same upstream instance UUID must also use distinct replication `server_id` values. Different instances may reuse replication IDs, although assigning them centrally is easier to operate.

Each source owns its thread, connection, schema, `table_id` mappings, transaction budget, and committed position. TABLE_MAP entries never cross source boundaries. There is no global transaction or global ordering across sources.

When a worker fails, the process asks other workers to stop between events and exits with a nonzero status. A blocked MySQL read may wait for its 90-second read timeout. After restart, each source resumes from its own committed checkpoint. Sources acquire initialization read locks individually, so initialization does not provide a consistent snapshot across instances. If initialization fails partway through, successfully created state files remain; a later `run` prepares the remaining sources.

Requires Rust 1.89 or later. `check` validates server settings, table engines, and primary keys; actual wire types and lengths are checked further when TABLE_MAP arrives. Relative configuration paths are resolved against the TOML file's directory. Credentials come from environment variables; encode special characters according to MySQL URL rules. Set `source.tls_ca` to a CA certificate to enable certificate and hostname verification.

The first `run` briefly executes `FLUSH TABLES WITH READ LOCK`, reads the whitelisted schemas and current committed position under the lock, then unlocks and starts consumption. **Consumption starts at the current position; there is no initial full-data snapshot.** You can also run `init cdc.toml` to capture the starting point and run the consumer later. `init` never overwrites an existing state file. `run cdc.toml --once` consumes currently available events and exits, which is useful for catch-up runs and testing.

Subsequent runs resume from the committed position in the state file. State must contain nonempty `source_uuid`, `source_id`, and `source_plugin` fields, together with the column order, primary key type, and character set valid at that position. Historical events are decoded using the saved schema, rather than the current INFORMATION_SCHEMA. The server UUID, source ID, source plugin, and whitelist must match the checkpoint. Binlogs after the starting position must remain available.

Example server settings; MySQL 5.7 does not have the last two variables, so omit them there:

```ini
[mysqld]
server-id=1
log-bin=binlog
binlog-format=ROW
binlog-row-image=MINIMAL
binlog-transaction-compression=OFF
binlog-row-value-options=
```

Writer connections must also use `ROW + MINIMAL`. Changing GLOBAL settings does not update SESSION settings on existing connections. The consumer checks global settings and rejects before images that do not meet MINIMAL expectations for whitelisted tables. The CDC `server_id` must be nonzero and differ from the upstream server and other replication consumers.

Example permissions; an administrator should replace the address, password, and database:

```sql
CREATE USER 'cdc'@'127.0.0.1' IDENTIFIED BY 'replace-me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'127.0.0.1';
GRANT SELECT ON app.* TO 'cdc'@'127.0.0.1';
-- Initialization uses a global read lock; RELOAD can be granted on MySQL 5.7/8.x.
GRANT RELOAD ON *.* TO 'cdc'@'127.0.0.1';
```

The global read lock briefly blocks writes and may wait for running statements. Initialize during an appropriate operational window. Normal checkpoint recovery does not acquire this lock. Whitelisted tables must use InnoDB so that changes from nontransactional tables are not treated as rollback-capable transactions.

## Output semantics

Output contains one JSON object per line. Events from each source follow binlog order. Sources share the output at transaction boundaries, so rows from another source cannot interrupt a transaction. Example UPDATE:

```json
{"source":"east","schema":"app","table":"users","operation":"update","pk":{"type":"unsigned","value":42},"changed":["name","age"]}
```

`source` is the stable source ID from configuration, distinguishing identical database/table/key combinations on different instances. `operation` is `insert`, `update`, or `delete`. When a primary key changes, `pk` holds the new value and `previous_pk` holds the old value. For INSERT, `changed` lists columns present in the after image. For DELETE, it is empty. For UPDATE, it lists columns in the after-image bitmap, including columns written as NULL and modified primary keys.

**A "changed column" means a column recorded in the binlog after image; it does not guarantee that its value differs.** MINIMAL does not retain old non-key values, and columns specified by SQL may appear even when their values stay the same. The log alone cannot support a value-by-value comparison. See the [MySQL binlog_row_image documentation](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html) and [confirmed MySQL issue #83418](https://bugs.mysql.com/bug.php?id=83418).

Supported primary keys:

- TINYINT, SMALLINT, MEDIUMINT, INT, and BIGINT, including UNSIGNED. Values are decoded at their actual byte widths with correct sign extension, without conversion through floating point.
- DECIMAL, preserving scale: `{"type":"decimal","value":"-12345678.90"}`.
- CHAR and VARCHAR using utf8, utf8mb3, utf8mb4, ascii, or latin1. UTF-8 keys borrow the event buffer in library callbacks and are copied only when retained as owned transaction data. CHAR padding is trimmed from the end.

Composite keys, missing primary keys, FLOAT/DOUBLE keys, binary keys, and unsupported key character sets are rejected. JSON can encode u64 numbers, but ordinary JavaScript parsing loses precision for large integers. Consumers should use a parser that preserves large integers or convert them to strings in a custom sink.

Non-key columns can be skipped for integers, FLOAT/DOUBLE, DECIMAL, BIT, date/time types with fractional seconds, CHAR/VARCHAR/BINARY/VARBINARY, ENUM/SET, BLOB/TEXT, JSON, spatial types, and VECTOR. Their values are not decoded, so JSON, string, and BLOB contents do not allocate value objects. Generated columns, invisible columns, the old DECIMAL wire type, and typed arrays produce explicit errors instead of guessed layouts.

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
cargo bench --bench minimal_rows
cargo bench --bench event_adapter
```

These benchmarks measure direct Decoder calls and the EventParser + `dyn RowDecoder` dispatch path, respectively, using MINIMAL UPDATE events with 1, 100, or 1000 rows. They exclude networking, JSON serialization, transaction retention, and disk synchronization, and do not promise end-to-end throughput.

The CLI flushes output, writes state, and fsyncs on every transaction. With many small transactions, persistence often costs more than decoding. For higher throughput, implement `TransactionSink` with batched durable storage and acknowledge the transaction before saving the checkpoint. Shared multi-source output takes one lock per transaction. Slow output applies backpressure without an unbounded message queue. Total transaction budgets are the sum of all source budgets, plus driver buffers and metadata. Source transport and version/row adapters dispatch per connection or event; sinks dispatch per transaction. Row callbacks keep borrowed types.

The library's `Decoder::new(Vec<TableMeta>)` accepts whitelisted schemas in column order. `table_map(&[u8])` preprocesses mappings, and `rows(event_type, &[u8], callback)` delivers borrowed `Change` values. Inputs are event bodies with the 19-byte common header and checksum removed; the decoder does not assume full row images. Use `Change::into_owned()` when retaining changes beyond the input buffer's lifetime. Use `Processor` for commit handling, `source::mysql::MysqlSource` for real connections, and `Runtime` for multi-source execution. See `tests/integration/mysql_live.rs` for the live path.

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

Adapters check ROW/MINIMAL settings, transaction compression and partial-JSON options, the common header, and relevant post-header lengths. The 9.7 adapter reuses strictly validated binlog v4 layouts; see the [MySQL 9.7.2 format description source](https://raw.githubusercontent.com/mysql/mysql-server/mysql-9.7.2/libs/mysql/binlog/event/control_events.cpp). Real-server compatibility remains subject to the CI matrix.

Unregistered series are rejected. A condition such as `major >= 5` is not sufficient to establish compatibility. Oracle MySQL has no 5.8 series; 5.7.8 belongs to 5.7. See the [official version reference](https://downloads.mysql.com/docs/mysqld-version-reference-en.pdf). Vendor versions require adapters based on their actual versions and log layouts.

To add a version, implement `VersionAdapter` and call `runtime.versions.register(Arc::new(adapter))`. Each FDE creates a file-specific parser through `create_event_parser`; `create_decoder` handles column metadata and value encoding. `examples/custom_version.rs` provides a compilable template. Registration alone does not establish compatibility.

When event structures differ, override `create_event_parser`. A custom `EventParser` produces borrowed `TableMapEvent` and `RowsEvent` views directly from the input, handling offsets, field order, endianness, and new event codes without rebuilding a canonical byte buffer. Control events may return `ParsedEvent::Control`. `classify` handles `Unclassified` events; QUERY/ROTATE/TABLE_MAP/ROWS already recognized by the parser do not call it again.

The default `MysqlEventParser` precompiles TABLE_MAP ID widths, ROWS v1/v2 headers, and QUERY/ROTATE post-header lengths from the FDE into a fixed dispatch table. A ROWS event with a zero post-header length in the FDE is rejected even if its code is known. Core row loops consume normalized views without reading version numbers or wire event types. Image parsers are selected at FDE/event time, and excluded tables are skipped before extra-data, bitmap, or value parsing.

Custom parsers use `RowsEvent::new(..., parse_images)` to provide a function returning borrowed `RowImages`. Reuse the default `Decoder` when MySQL value encodings match. Otherwise, override `create_decoder` and implement `RowDecoder::table_map_event` and `rows_event`. Raw-body methods handle standard layouts; default trait view methods reject nonstandard layouts explicitly. `tests/integration/event_adapters.rs` uses synthetic vendor formats to cover four-byte IDs, extended control headers, reordered fields, alternate endianness, new ROWS codes, file transitions, and zero-copy UTF-8 keys. These fixtures do not represent real-server compatibility tests.

The transport owns common binlog v4 framing: the 19-byte header, FDE bootstrap, and CRC32/NONE. The current MySQL driver also preparses FDE/TABLE_MAP/ROTATE, so it may reject vendor layout changes before they reach the adapter. Such cases need a custom `SourcePlugin` as well. Registering a `VersionAdapter` alone cannot bypass driver layout restrictions. Default Oracle MySQL adapters continue to enforce validated layouts rather than assuming unknown FDE length changes are safe.

```rust
let mut runtime = pikachu_rs::engine::Runtime::default();
runtime.versions.register(std::sync::Arc::new(MyVersionAdapter))?;
// runtime.sources.register(Arc::new(MySourcePlugin))?;
runtime.run(config, false, sink)?;
```

Adapter selection is automatic by default. To override a built-in adapter for the same series, set `adapter = "your-adapter"` in that source's configuration. Overlapping matches without an explicit selection produce an ambiguity error; registration order does not silently override adapters. A pinned adapter also constrains historical FDE versions, so recovery across series upgrades generally uses automatic selection with all required adapters registered.

## Transactions and limitations

Changes become visible only after XID/COMMIT. ROLLBACK discards buffered changes. After output succeeds and flushes, state is atomically replaced and both the file and parent directory are synchronized. A file lock prevents concurrent consumers of the same state. Output or persistence failures stop consumption. A crash between output and checkpoint persistence can cause duplicate delivery, so consumers must be idempotent. Flushing stdout only writes to the OS or pipe: **it does not acknowledge downstream durability, guarantee exactly-once delivery, or guarantee no loss on downstream failure.**

Any QUERY_EVENT other than BEGIN/COMMIT/ROLLBACK stops consumption, including DDL for another database. This conservative rule accounts for cross-database SQL, RENAME, and TRUNCATE, which cannot be safely filtered by the default database. DDL does not refresh the schema and continue automatically. Schema migrations require explicit handling of the schema version and the position after DDL, after consuming up to the DDL boundary. Starting a new state at the current position is appropriate only after confirming the previous backlog is fully handled; deleting state to skip a backlog is not a migration strategy.

XA, partial JSON updates, compressed transactions, MariaDB extensions, and unknown non-ignorable events are rejected. Heartbeats, GTID, ROTATE, format descriptions, and ordinary ignorable events are handled. Recovery uses file/position rather than GTID failover. Disconnections return errors; a process manager can restart consumption from saved state. The consumer does not reconnect silently within the process. Layout checks refer to the [MySQL 8.4 format description source](https://raw.githubusercontent.com/mysql/mysql-server/mysql-8.4.6/libs/mysql/binlog/event/control_events.cpp).

## Testing

```sh
cargo fmt --all -- --check
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
```

Default tests cover event adaptation and zero-copy behavior, source thread isolation, identical table IDs with different column orders, source identity binding, independent checkpoints, sink failures, version selection, historical-file adapter selection, custom parsers/decoders, and registry conflicts. Independently constructed protocol bytes cover sign extension, sparse NULL images, multiple rows, key changes, long CHAR values, mixed skip types, more than 64 columns, table ID reuse, truncation, CRC32/NONE, transaction visibility, checkpoint ordering, and file locks. These are synthetic fixtures, not mysqlbinlog exports.

GitHub Actions runs tests on Ubuntu with Rust 1.89.0 and stable, and macOS with stable, for PRs and pushes to main/master. It also runs formatting, Clippy, documentation, the MySQL matrix below, and crates.io package verification. Releases are triggered by `v<package.version>` tags and publish only after the full CI passes. Publishing uses GitHub OIDC Trusted Publishing: after the initial crate publication, configure this repository and `release.yml` as a trusted publisher on crates.io. No long-lived API token is stored. See the [CI and publishing guide](docs/releasing.md) (Chinese).

Run real-MySQL tests explicitly against a dedicated test server. The fixtures create and drop uniquely named databases:

```sh
MYSQL_IMAGE=mysql:8.4 docker compose -f tests/infra/compose.yaml up -d --wait
export CDC_TEST_MYSQL_URL='mysql://root:cdc-test-only@127.0.0.1:13306'
cargo test --test mysql_live --locked -- --ignored --nocapture --test-threads=1
docker compose -f tests/infra/compose.yaml down -v
```

`mysql_live` contains 11 independent scenarios reading actual server-generated binlogs through the production `SourcePlugin`, `VersionAdapter`, `Processor`, and sink path. Synthetic events do not replace real mutations. Assertions compare source, schema, table, operation, exact key types/values, `previous_pk`, and `changed`. Missing, extra, or duplicate changes fail the test.

| Scenario | Checks |
| --- | --- |
| Single-row INSERT / UPDATE / DELETE | Complete output, specified update columns, NULL writes, empty DELETE `changed`, and skipping JSON/time/DECIMAL/BIT/ENUM/SET/BLOB values |
| Multi-row mutations | Every inserted, updated, or deleted row; zero-match UPDATE/DELETE emits nothing |
| Integer keys | Signed/unsigned boundaries for all integer widths, NULL skipping, i64::MIN, and u64::MAX |
| Primary key changes | Old and new UTF-8 keys including emoji, CHAR, ASCII, latin1, DECIMAL with scale, and unsigned integers |
| Wide sparse images | 80 columns, a key in column 70, and NULL/non-NULL updates across the 64-bit boundary |
| Whitelists | Excluded tables, same-named tables in other databases, mixed transactions, and checkpoint advancement for fully filtered transactions |
| Live transaction consumption | Start a blocking consumer before writes; ROWS remains invisible and unacknowledged until XID/COMMIT |
| Rollback | Rolled-back INSERT/UPDATE/DELETE emits nothing; subsequent commits are still consumed |
| Checkpoint recovery | Reconnect from the persisted checkpoint without resending acknowledged events; new changes continue |
| Sink failure | Rejected commits preserve disk positions and allow full replay from the original checkpoint |
| DDL boundaries | ALTER TABLE stops consumption without decoding new-schema rows or acknowledging positions after DDL |

Run these scenarios serially: fixture creation and cleanup produce DDL, and the consumer stops on DDL in any database. Each fixture uses independent schemas, a replication ID, and temporary state, and attempts cleanup on completion or assertion failure.

Set `MYSQL_IMAGE` to `mysql:5.7.44`, `mysql:8.0`, `mysql:8.4`, or `mysql:9.7` to test each series. Remove the previous test volume before switching versions. `.github/workflows/mysql-compatibility.yml` runs all 11 scenarios for the historical 5.7/8.0 baselines and all current LTS series, 8.4 and 9.7. Explicit series tags follow patch updates; the moving `lts` tag is not used. CI checks the actual server series and logs its patch version and selected adapter. See the [official MySQL LTS description](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html). Add both a matrix entry and a version adapter when supporting a new LTS series. Real-server tests require a Docker/MySQL environment.

Offline replay requires a state/schema matching the starting position and a binlog retaining its initial FORMAT_DESCRIPTION:

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001
mysqlbinlog --hexdump --base64-output=DECODE-ROWS -vv /path/to/binlog.000001
```

Replay uses the explicit source ID from state, leaves state unchanged, and skips events before the starting position. The saved schema must match the historical log. Each command replays one file for one source; use the corresponding checkpoint for each source. CRC32 and NONE are supported, events are limited to 64 MiB, and truncated tails return errors. The filename must match the state's `file`. At the library level, a source's `Processor` can continue across successive files.
