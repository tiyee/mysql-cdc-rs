# Configuration and operation

English | [简体中文](configuration.zh-CN.md)

[Documentation index](README.md)

Follow this guide to prepare MySQL, connect one source, check and run consumption, then recover or replay. Commands use a build from source.

## 1. Prepare MySQL

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

## 2. Configure one source

Save this as `cdc.toml`. It connects to the MySQL instance named by `MYSQL_EAST_URL`, selects only `app.users`, and saves progress in `east-state.json`. Replace the database/table names with yours.

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
row_image = "minimal"

[[mysql-canal.sources.tables]]
schema = "app"
table = "users"
```

### How to read the configuration

| Section | What it controls |
| --- | --- |
| `mysql-canal` | The change-consumption service used by Pikachu |
| `mysql-canal.sources` | One named source; configure separate entries for multiple instances |
| `sources.source` | Source plugin; use `mysql` for MySQL |
| `sources.mysql-replication` | Database connection, replication ID and row-image mode |
| `sources.tables` | Exact database/table pairs to subscribe to |

Pikachu contains canal, and each canal source contains replication settings. Each layer supplies only its own options. Unknown fields, missing lower-layer sections and misplaced settings fail validation.

### Common settings

Paths below are relative to each source, under `mysql-canal.sources` in a product file.

| Setting | What to enter | Default / constraint |
| --- | --- | --- |
| `id` | A stable name such as `east` | Required and unique; included in output |
| `state_file` | A progress file such as `east-state.json` | Different for each source; parent directory must exist |
| `tables` | Database/table pairs | Required, exact matches, no duplicates |
| `mysql-replication.url_env` | Environment variable holding the MySQL URL | Required; keep passwords out of the file |
| `mysql-replication.server_id` | This consumer’s replication ID, such as `901001` | Nonzero and different from the upstream and its other consumers |
| `mysql-replication.row_image` | `minimal` or `full` | Defaults to `minimal`; must match the log |
| `mysql-replication.tls_ca` | CA certificate for server verification | Optional; enables certificate and hostname checks |
| `mysql-replication.connect_timeout_secs` | Connection wait in seconds | Defaults to 10, nonzero |
| `mysql-replication.read_timeout_secs` | Read wait in seconds | Defaults to 90, nonzero |
| `max_transaction_bytes` | Buffered transaction memory budget for this source | Defaults to 64 MiB, nonzero; not a process RSS limit |
| `source.plugin` | Source type | Defaults to `mysql`; `[source]` is still required |
| `source.adapter` | A specific version adapter | Usually omitted for automatic server/log version selection |

Relative paths resolve against the configuration file. Encode special characters in connection URLs. Log filename/position come from checkpoints rather than static configuration.

## 3. Check and start consumption

```sh
cargo build --release --locked -p pikachu
export MYSQL_EAST_URL='mysql://cdc:password@127.0.0.1:3306'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

`check` validates connections, server settings and selected primary keys without consuming changes. `run` creates a starting checkpoint if needed and consumes continuously. Changes use stdout; diagnostics use stderr.

| Task | Command |
| --- | --- |
| Validate settings only | `pikachu-cdc check cdc.toml` |
| Save the current starting point for later | `pikachu-cdc init cdc.toml` |
| Consume continuously | `pikachu-cdc run cdc.toml` |
| Consume currently available events and exit | `pikachu-cdc run cdc.toml --once` |

Initial consumption starts at the **current committed position**, without backfilling existing data or taking a full snapshot. `init` does not overwrite checkpoints.

## 4. Recover, replay and add sources

Run `run` again to resume from each source's last acknowledged checkpoint. Do not rewrite progress files or delete them to skip a backlog. Recovery uses the historical schema saved at that position, and the upstream must retain the required logs. Each connection checks current primary keys, but historical logs are still interpreted using checkpoint schema.

Local replay does not connect to MySQL or change progress. Supply a matching checkpoint and one log file:

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001 --row-image minimal
```

The filename must match the checkpoint, and the file must retain its initial format description. Truncation or incomplete transactions fail. Each invocation replays one file for one source.

Add one `[[mysql-canal.sources]]` entry per instance, each with its own name, connection, tables and checkpoint. See the [multi-source example](../crates/pikachu/examples/config/multi-source.toml). Sources recover independently without global ordering or a cross-instance consistent starting snapshot. A source failure stops the run; blocked reads may wait for their timeout.

## Choose and switch modes

See the [event examples](events.md#what-minimal-and-full-record) for mode meanings. Configuration defaults to MINIMAL; set `row_image = "full"` for FULL sources. The mode is validated, not applied to MySQL or automatically detected from historical logs.

Temporary online overrides name the source; replay selects the file's mode directly:

```sh
./target/release/pikachu-cdc run cdc.toml --once --row-image east=full
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001 --row-image full
```

Before switching, pause writes, consume through a commit boundary using the current mode and stop the consumer. Update server and writer-session settings, then resume with the new mode. Checkpoints do not store the mode, so replay must select the historical file's mode explicitly. NOBLOB is not a CDC option.

## Use the library directly

A standalone canal [configuration](../crates/mysql-canal/examples/config/multi-source.toml) begins at `[[sources]]`, without the outer `mysql-canal` wrapper. See the [Rust API reference](event-api.md#replication-connection-and-parser-apis) for raw connections, parser interfaces and embedding configuration in another product.
