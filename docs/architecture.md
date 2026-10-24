# How the system works

English | [简体中文](architecture.zh-CN.md)

[Documentation index](README.md)

mysql-cdc turns changes recorded in MySQL logs into notifications an application can consume. Start with three questions: who reads the log, who decides when changes take effect, and who delivers the result?

## What each layer does

| Layer | Responsibility | Result |
| --- | --- | --- |
| mysql-replication | Connect to MySQL or read local binlogs and recognize log events | Raw events, including rows, transactions and log-control information |
| mysql-canal | Select tables, identify keys and recorded fields, wait for commit and track recovery positions | Normalized change notifications from committed transactions |
| Pikachu | Read product configuration, run commands and print results | One JSON record per line by default |

Dependencies run from consumer to provider: **Pikachu → mysql-canal → mysql-replication**. `mysql-cdc-rs` is a unified Rust library entry point for both libraries; it adds no processing stage. The complete system is named **mysql-cdc**.

The aggregate exposes only the `canal` and `replication` modules. `canal::Canal::new` owns configured consumption, handlers and checkpoints; replication readers and streams own raw event consumption. Configuration and errors stay with their owning module.

## How an update reaches an application

Suppose an application changes record 42 in `app.users` to the name Bob:

| Stage | What happens | Is a notification available? |
| --- | --- | --- |
| MySQL writes the log | Records table information, row changes and transaction boundaries | No |
| Replication reads it | Recognizes events and reads before/after data | Raw-event readers can see log contents |
| Canal organizes it | Selects the table, identifies the key and fields, and buffers the transaction | CDC consumers still wait |
| MySQL commits | The log contains a commit marker | Canal delivers the complete transaction |
| Handlers succeed | Application handling finishes, or the CLI writes and flushes output | A checkpoint is saved for recovery |

Rollback makes canal discard buffered changes. A transaction modifying ten rows delivers those ten changes in log order after commit; a library handler receives the whole transaction at once. Raw-event readers must track commit boundaries themselves.

See the [event guide](events.md) for actual data and MINIMAL/FULL examples.

## Why there are two kinds of event

Raw events answer “what did the log record?” They include format descriptions, encoded table information, row changes and transaction markers, useful for protocol inspection and raw-data analysis.

Normalized notifications answer “which source, table and record changed?” They keep the source, database/table, operation, key and recorded column names for application handling. FULL does not automatically add complete row values to notifications.

Canal reads key values selectively and skips other values, without first building full application row objects. Missing MINIMAL data is not filled in, and the current database is not queried to guess historical values.

## Why configuration is nested

| Layer | Settings it owns |
| --- | --- |
| mysql-replication | Credentials environment variable, replication ID, TLS, timeouts and row-image mode |
| mysql-canal | Selected tables, source names, checkpoints and transaction budgets; includes replication settings |
| Pikachu | Product settings; includes the complete canal configuration, with no extra required product options today |

Each product source therefore supplies its connection through `mysql-canal → sources → mysql-replication`. A standalone canal application omits the outer product layer. See [configuration and operation](configuration.md) for complete examples.

## First start and recovery

**First start** briefly acquires a MySQL global read lock, saves selected table schemas and the current committed position, then begins consumption. Earlier data is not backfilled, and there is no full snapshot. Choose an appropriate window for this brief effect on writes.

**Later starts** resume from checkpoints and interpret historical logs with the schema saved there. Source identity and selected tables are checked to prevent using one source's state for another. MySQL must retain the required logs.

**Offline replay** uses an existing checkpoint and matching log file without connecting to MySQL or changing the checkpoint. It is useful for examining logs and reproducing processing behavior.

## Multiple sources

Each source has its own connection, schema, transaction buffer and checkpoint. Record 42 on east and record 42 on west are different records; output `source` distinguishes them.

Each source follows its log order. Shared output finishes one transaction before writing another, without interleaving their rows. Separate instances have no single global event order, and initialization is not a cross-instance consistent snapshot.

A source failure stops the run and asks peers to stop between events. A blocked read may wait for its timeout. On restart, sources resume from their individual checkpoints.

## What consumers need to handle

| Situation | Result | Consumer action |
| --- | --- | --- |
| Commit handled successfully | Checkpoint saved | Process the next batch |
| Transaction rolls back | Changes discarded | No row notifications from that transaction |
| Exit after output but before checkpoint persistence | Restart may redeliver | Make processing safe to repeat |
| Schema change or unsupported log | Stop without passing unacknowledged data | Diagnose and explicitly handle schema and restart position |
| Processing slower than writes | Backlog grows; slow handlers block further delivery | Monitor lag and retain enough upstream logs |

Flushing CLI output means writing to stdout or a pipe, not downstream durable acknowledgement. Applications requiring durable delivery should confirm success only after saving their business result.

## Developer details

See [development](development.md#implementation-data-flow) for modules, parser/decoder interfaces, buffers and extension points, and the [API reference](event-api.md) for Rust fields and generics.
