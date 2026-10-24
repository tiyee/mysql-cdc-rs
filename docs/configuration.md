# Configuration and operation

English | [简体中文](configuration.zh-CN.md)

[Documentation index](README.md)

This guide covers configuration, server setup, recovery and replay. Start with the [system README](../README.md); see the [event reference](events.md) for fields and output semantics.

## Configuration layers

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

For direct canal integration, copy `crates/mysql-canal/examples/config/multi-source.toml` to `canal.toml`. That file starts at `[[sources]]` and requires `[sources.mysql-replication]`; the product wrapper above is used only by Pikachu.

The CLI registers its default `PrintHandle`, which writes JSON Lines to stdout. Its command name remains `pikachu-cdc`. The product file must contain `mysql-canal`, with `mysql-replication` nested under each canal source. Start with `crates/pikachu/examples/config/multi-source.toml`. Pikachu adds only product-specific settings at the root; currently none are required. Each `[[mysql-canal.sources]]` entry has its own `id`, `state_file`, `source`, and `tables`:

```sh
cargo build --release --locked -p pikachu
cp crates/pikachu/examples/config/multi-source.toml cdc.toml
# Set each source's [[mysql-canal.sources.tables]] to exact database and table names, respecting server case rules.
export MYSQL_EAST_URL='mysql://cdc:password@east.example:3306'
export MYSQL_WEST_URL='mysql://cdc:password@west.example:3306'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

| Command | Behavior |
| --- | --- |
| `pikachu-cdc check CONFIG.toml` | Validate sources, server capabilities and table schemas without consuming events |
| `pikachu-cdc init CONFIG.toml` | Save initial schemas and current positions without consuming events; reject existing state files |
| `pikachu-cdc run CONFIG.toml` | Initialize missing states, then consume continuously from each saved position |
| `pikachu-cdc run CONFIG.toml --once` | Consume currently available events and exit |
| `pikachu-cdc replay STATE.json BINLOG_FILE` | Replay one local file using the saved historical schema without modifying state |

## Configuration fields

Paths below are relative to each `sources` entry; Pikachu adds the `mysql-canal.` prefix. Each layer rejects unknown and flattened fields.

| Field | Requirement / default | Owner |
| --- | --- | --- |
| `id` | Required, nonempty and unique | Canal |
| `state_file` | Required, unique path with an existing parent directory | Canal |
| `max_transaction_bytes` | Default 67108864 (64 MiB), nonzero | Canal |
| `tables` | Nonempty list of exact database/table pairs, without duplicates | Canal |
| `source.plugin` | `[source]` required; plugin defaults to `mysql` | Canal |
| `source.adapter` | Optional; automatic server/FDE selection by default | Canal |
| `mysql-replication.url_env` | Required credentials environment-variable name | Replication |
| `mysql-replication.server_id` | Required nonzero replication ID | Replication |
| `mysql-replication.tls_ca` | Optional CA certificate path | Replication |
| `mysql-replication.row_image` | Default `minimal`; also accepts `full` | Replication |
| `mysql-replication.connect_timeout_secs` | Default 10, nonzero | Replication |
| `mysql-replication.read_timeout_secs` | Default 90, nonzero | Replication |

To embed canal in another product, deserialize its `AppConfig` directly and call `app.resolve_paths(parent_toml_path)` for validation and relative paths. `AppConfig::read` reads only standalone canal files. File/position come from runtime checkpoints, rather than static connection settings.

## Row images and mode switching

Set `row_image = "minimal"` or `row_image = "full"` under each `[mysql-canal.sources.mysql-replication]`; omitted values default to `minimal`. Sources may use different modes. Online commands accept repeatable `--row-image SOURCE_ID=minimal|full` overrides, including with `run --once`. Replay accepts `--row-image minimal|full` and defaults to `minimal`:

```sh
./target/release/pikachu-cdc check cdc.toml --row-image east=full
./target/release/pikachu-cdc run cdc.toml --once --row-image east=full --row-image west=minimal
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001 --row-image full
```

These options validate the input; they never change MySQL settings or request different bytes from an existing binlog. MINIMAL requires key-only UPDATE/DELETE before images for the supported single-key tables. FULL requires every column in each existing image. NOBLOB and automatic mode detection are not CDC options. Image bitmaps cannot always distinguish producer settings, for example on single-column tables.

The v1 checkpoint format remains unchanged and does not store the image mode. Replay must select the mode appropriate for the historical file. A current GLOBAL setting does not describe historical logs. To switch modes, pause writers, consume through a commit boundary with the old mode, stop the consumer, update the server and writer sessions, then restart with the new mode. Do not reinitialize checkpoints to skip unconsumed data. Mixed historical modes can fail validation.

## Sources, paths and recovery

Pikachu configurations use the `[[mysql-canal.sources]]` list, including deployments with just one named source. Each entry must explicitly specify an `id` and the credentials environment variable in `mysql-canal.sources.mysql-replication.url_env`. Source IDs and state paths must be distinct, and state parent directories must already exist. Subscriptions to the same upstream instance UUID must also use distinct replication `server_id` values. Different instances may reuse replication IDs, although assigning them centrally is easier to operate.

Each source owns its thread, connection, schema, `table_id` mappings, transaction budget, and committed position. TABLE_MAP entries never cross source boundaries. There is no global transaction or global ordering across sources.

When a worker fails, the process asks other workers to stop between events and exits with a nonzero status. A blocked MySQL read may wait for its configured read timeout (90 seconds by default). After restart, each source resumes from its own committed checkpoint. Sources acquire initialization read locks individually, so initialization does not provide a consistent snapshot across instances. If initialization fails partway through, successfully created state files remain; a later `run` prepares the remaining sources.

Requires Rust 1.89 or later. `check` validates server settings, table engines, and primary keys; actual wire types and lengths are checked further when TABLE_MAP arrives. Relative configuration paths are resolved against the TOML file's directory. Credentials come from environment variables; encode special characters according to MySQL URL rules. Set `mysql-canal.sources.mysql-replication.tls_ca` to a CA certificate to enable certificate and hostname verification.

Every MySQL source checks the current primary keys of its whitelist within `mysql-canal`, using the same key-type rules as CDC conversion before starting, including checkpoint recovery. Missing or unsupported keys stop startup before initial checkpoints are written or binlog consumption begins. Recovery still uses the saved historical schema to decode events. Raw replication callers can pass selected tables to `ReplicationStream::connect_with_tables` or `from_connection_with_tables`; see [replication APIs below](#replication-connection-and-parser-apis).

The first `run` briefly executes `FLUSH TABLES WITH READ LOCK`, reads the whitelisted schemas and current committed position under the lock, then unlocks and starts consumption. **Consumption starts at the current position; there is no initial full-data snapshot.** You can also run `init cdc.toml` to capture the starting point and run the consumer later. `init` never overwrites an existing state file. `run cdc.toml --once` consumes currently available events and exits, which is useful for catch-up runs and testing.

Subsequent runs resume from the committed position in the state file. State must contain nonempty `source_uuid`, `source_id`, and `source_plugin` fields, together with the column order, primary key type, and character set valid at that position. Historical events are decoded using the saved schema, rather than the current INFORMATION_SCHEMA. The server UUID, source ID, source plugin, and whitelist must match the checkpoint. Binlogs after the starting position must remain available.

## MySQL settings and grants

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

Writer connections must use ROW and the configured MINIMAL or FULL image mode. Changing GLOBAL settings does not update SESSION settings on existing connections. The consumer checks global settings and validates whitelisted row images against the selected policy. The CDC `server_id` must be nonzero and differ from the upstream server and other replication consumers.

Example permissions; an administrator should replace the address, password, and database:

```sql
CREATE USER 'cdc'@'127.0.0.1' IDENTIFIED BY 'replace-me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'127.0.0.1';
GRANT SELECT ON app.* TO 'cdc'@'127.0.0.1';
-- Initialization uses a global read lock; RELOAD can be granted on MySQL 5.7/8.x.
GRANT RELOAD ON *.* TO 'cdc'@'127.0.0.1';
```

The global read lock briefly blocks writes and may wait for running statements. Initialize during an appropriate operational window. Normal checkpoint recovery does not acquire this lock. Whitelisted tables must use InnoDB so that changes from nontransactional tables are not treated as rollback-capable transactions.

## Replication connection and parser APIs

`config::Config` owns reusable connection/subscription settings and supports `config::RowImageMode::{Minimal, Full}`. It defaults to MINIMAL, validates the required upstream setting without modifying MySQL, and builds the existing low-level connection/stream configurations. Canal includes this exact type in its required `[sources.mysql-replication]` section. Defaults are 10 seconds for `connect_timeout_secs` and 90 for `read_timeout_secs`; zero timeouts are rejected. `tls_ca` is optional; an embedding application resolves relative paths. The binlog file and position are supplied by the consumer at runtime.

```rust,no_run
use mysql_replication::{Result, config::{Config, RowImageMode}, stream::ReplicationStream};

fn main() -> Result<()> {
    let mut settings = Config::new("MYSQL_URL", 123);
    settings.row_image = RowImageMode::Full;
    let mut stream = ReplicationStream::connect(
        &settings.connection_config()?,
        settings.replication_config("binlog.000001", 4)?,
    )?;
    while let Some(event) = stream.next_event()? {
        println!("{}", event.header.log_pos);
    }
    Ok(())
}
```

The low-level APIs remain available for explicit parser selection and optional table preflight:

```rust,no_run
use mysql_replication::{Result, stream::{ConnectionConfig, ReplicationConfig, ReplicationStream, RowImage}, parser::ParserConfig};

fn main() -> Result<()> {
    let connection = ConnectionConfig::new("MYSQL_URL");
    let mut config = ReplicationConfig::new(123, "binlog.000001", 4);
    config.required_row_image = Some(RowImage::Minimal);
    config.parser = ParserConfig::minimal();
    let mut stream = ReplicationStream::connect_with_tables(
        &connection, config, [("app", "users"), ("app", "orders")],
    )?;
    while let Some(view) = stream.next_view()? {
        // Borrowed body: no second raw-frame copy or row-value decoding.
        println!("{} {}", view.header.event_type, view.body.len());
    }
    Ok(())
}
```

Connection configuration controls credentials via an environment variable, TLS CA and timeouts. Replication configuration controls server ID, file/position, blocking mode, required upstream row image and parser options. Row-image requirements are verified, never changed globally. MySQL sends the available binlog bytes; there is no per-column network projection.

`ReplicationStream::connect_with_tables` and `from_connection_with_tables` validate the caller's selected tables through INFORMATION_SCHEMA before sending a binlog request. Every selected table must have a single-column PRIMARY KEY: TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT (signed or unsigned), exact DECIMAL, or CHAR/VARCHAR using utf8, utf8mb3, utf8mb4, ascii or latin1. Missing tables/keys, composite keys, UNIQUE-only indexes, FLOAT/DOUBLE, binary/date keys and unsupported character sets return an error with the database/table and, when available, column name. Empty selections are rejected. This list controls validation only, not event filtering. The existing `connect` and `from_connection` methods remain available for raw replication without this check. Call `primary_key::validate_primary_keys(&mut conn, tables)` to run the same check independently without starting replication. These optional public helpers remain available; `mysql-canal` owns its CDC startup eligibility checks and does not depend on them. Raw event parsing and row decoding require no primary key.

- `next_event()` returns typed protocol events and the complete raw bytes.
- `next_frame()` validates bounds and checksums, retaining raw bytes while leaving body interpretation to custom adapters.
- Network `next_view()` validates the driver's event and borrows its body directly, avoiding full raw-frame assembly for consumers that do not need it.
- `ParserConfig::minimal()` skips GTID metadata and Previous GTIDs decoding. They remain explicit raw bodies with their original event codes.
- ROWS image parsing is deferred by default; `RowsEvent::images()` reads extra data and sparse bitmaps on demand. `RowsDecode::ValidateImages` validates those immediately. These parser modes do not decode field values; the separate `RowsDecoder` API in the [event reference](events.md) is opt-in.
- `TableMapEvent::columns()` validates column envelopes on demand, after table selection.

## Offline replay

Offline replay requires a state/schema matching the starting position and a binlog retaining its initial FORMAT_DESCRIPTION:

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001
mysqlbinlog --hexdump --base64-output=DECODE-ROWS -vv /path/to/binlog.000001
```

Replay uses the explicit source ID from state, leaves state unchanged, and skips events before the starting position. The saved schema must match the historical log. Each command replays one file for one source; use the corresponding checkpoint for each source. CRC32 and NONE are supported, events are limited to 64 MiB, and truncated tails return errors. The filename must match the state's `file`. At the library level, a source's `Processor` can continue across successive files.

`ParserConfig::minimal()` reduces metadata parsing; it does not require MINIMAL row images. Low-level `stream::ReplicationConfig.required_row_image = Some(RowImage::Full)` requires upstream FULL; `None` skips that setting check without changing existing bytes or image types.

## Protocol and transport limits

The structured parser selects advertised layouts from every file's FDE and accepts the registered MySQL series 5.7, 8.0–8.4, 9.0 and 9.7. Producer versions must have numeric `major.minor.patch` components, optionally followed by a nonempty `-` or `+` suffix (such as `5.7.44-log` or `8.0.45-0ubuntu0.24.04.1`). Malformed versions and MariaDB identifiers are rejected, using the same numeric parsing rules as CDC. Unknown series/layouts fail explicitly. `next_frame()` permits custom body parsers. When switching between frame/view and structured-event methods, feed encountered FDE frames into your own `BinlogParser`; the built-in structured parser only observes `next_event()` calls.

Offline files must retain their initial FDE. CRC32/NONE are supported; truncated tails and oversized events fail, with a 64 MiB bound. Network transport uses `mysql`'s driver, which pre-parses FDE/TABLE_MAP/ROTATE and allocates before this library can check event size. Custom framing may require another transport. Full raw network events preserve the driver's original header fields, body, algorithm and stored checksum; they never call the driver's checksum-recalculating serializer. `next_view()` avoids this assembly entirely. Disconnects propagate errors without silent reconnects.

## Migrating configuration to 2.0

Version 2 requires a `[mysql-canal.sources.mysql-replication]` section for every source. Move `url_env`, `server_id`, `tls_ca` and `row_image` out of `[mysql-canal.sources.source]`; that section now contains only `plugin` and `adapter`. Rust `Config` literals must include `replication: config::ReplicationConfig::new(url_env, server_id)`, then set its `row_image` to `RowImageMode::Full` when needed. `ReplicationConfig` and `RowImageMode` are re-exports of `mysql_replication::config` types. Old TOML layouts are rejected; `Decoder::new`, `EventConverter::new`, `Processor::new` / `with_registry` retain MINIMAL defaults. Explicit constructors use `with_row_image`. Custom version adapters must opt into FULL through `validate_server_with_row_image` and `create_decoder_with_row_image`; their defaults preserve MINIMAL and reject FULL.
