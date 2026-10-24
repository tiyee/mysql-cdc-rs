# Testing guide

English | [简体中文](testing.zh-CN.md)

[Documentation index](README.md)

Run from the repository root. Ordinary tests use synthetic protocol data; live tests are ignored by default. Report synthetic and real-server results separately.

## Local checks

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked
```

## Synthetic tests

Default tests cover event adaptation and zero-copy behavior, source thread isolation, identical table IDs with different column orders, source identity binding, independent checkpoints, sink failures, version selection, historical-file adapter selection, custom parsers/decoders, and registry conflicts. Independently constructed protocol bytes cover sign extension, sparse NULL images, multiple rows, key changes, long CHAR values, mixed field encodings, more than 64 columns, table ID reuse, truncation, CRC32/NONE, transaction visibility, checkpoint ordering, and file locks. The registered `decode_rows` example tests execute default/explicit mode selection, exact operation output, sparse rejection, table remapping and boundary clearing, truncated updates, and output failures on every Rust CI target. Compile-fail doctests check mode separation and borrowed image lifetimes. These are synthetic fixtures, not mysqlbinlog exports.

CI configuration and publishing are described in the [release guide](releasing.md) (Chinese).

Field-bearing generic event regressions independently construct TABLE_MAP optional TLVs and ROWS bytes. They cover column names, MSB-first signedness, compact character collation indices, primary-key prefixes, unknown/malformed metadata, absent/NULL fields, historical definitions retained by owned views, remaps and source isolation. The raw-server row scenario also compares automatic generic `next_event()` decoding against the existing manual decoder.

Canal TABLE_MAP regressions also reject integer signedness and primary-key metadata conflicts in both image policies, verify matching/absent optional metadata and compact numeric ordinals, and assert no output or checkpoint acknowledgement on rejection.

Canal field regressions compare complete borrowed/owned JSON, historical column metadata, sparse absence versus SQL NULL, unsigned integers and exact DECIMAL, text/bytes and opaque values, input-buffer reuse, mixed-source modes, image memory budgets and serialization failure without checkpoint advancement. Live expectations compare before/after fields as well as keys and changed names; binary JSON is decoded only in its dedicated semantic assertion.

## Dedicated MySQL tests

Run real-MySQL tests explicitly against a dedicated test server. The fixtures create and drop uniquely named databases:

```sh
(
set -eu
cargo test -p mysql-replication -p mysql-canal -p pikachu --test mysql_live --locked --no-run
export CDC_TEST_ROW_IMAGE='minimal' # Use full to test FULL images.
cleanup_mysql_cdc_test() {
    docker compose -f tests/infra/compose.yaml down -v
}
trap cleanup_mysql_cdc_test EXIT
MYSQL_IMAGE=mysql:8.4 docker compose -f tests/infra/compose.yaml up -d --wait --wait-timeout 180
export CDC_TEST_MYSQL_URL='mysql://root:cdc-test-only@127.0.0.1:13306'
export CDC_TEST_MYSQL_SERIES='8.4'
cargo test -p mysql-replication --test mysql_live --locked -- --ignored --nocapture --test-threads=1
cargo test -p mysql-canal --test mysql_live --locked -- --ignored --nocapture --test-threads=1
cargo test -p pikachu --test mysql_live --locked -- --ignored --nocapture --test-threads=1
)
```

Always run `down -v` after this dedicated test run, including when startup or tests fail. When switching the image or `CDC_TEST_ROW_IMAGE`, update the series/mode accordingly and remove the old test volume first. Both the server and writer sessions use the selected mode.

Each processing layer has its own registered `mysql_live` test target. The MySQL matrix compiles all three before starting its disposable container, then runs them serially:

| Package | Scenarios | Contract |
| --- | --- | --- |
| `mysql-replication` | 4 | Primary-key preflight, upstream mode mismatch/unchecked selection, raw and typed sparse/full images, borrowed `next_view()` decoding, wide multirow operations, exact values, NULL and delivery of unselected tables |
| `mysql-canal` | 12 | CDC conversion, committed transactions, source startup validation and checkpoint recovery |
| `pikachu` | 3 | Online `check`/`init`/`run --once`, complete JSON Lines, filtering, rollback, resume without duplicates, DDL and preflight failure checkpoints, source mode overrides |

These 19 scenarios consume actual server-generated binlogs or query real metadata. Default workspace tests compile them but leave them ignored; synthetic events do not replace real mutations. Canal's 12 scenarios use the production `SourcePlugin`, `VersionAdapter`, `Processor`, and sink path. CDC assertions compare source, schema, table, operation, exact key types/values, `previous_pk`, and `changed`. Missing, extra, or duplicate changes fail the test.

| Scenario | Checks |
| --- | --- |
| Startup primary keys | CDC check/init and fresh/resumed consumption reject missing, UNIQUE-only, composite and unsupported keys; checkpoints remain unchanged and excluded tables do not affect validation |
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

Set `MYSQL_IMAGE` to `mysql:5.7.44`, `mysql:8.0`, `mysql:8.4`, or `mysql:9.7` to test each series. Remove the previous test volume before switching versions. `.github/workflows/mysql.yml` runs all 19 scenarios in both MINIMAL and FULL modes across the three packages for the historical 5.7/8.0 baselines and all current LTS series, 8.4 and 9.7. Explicit series tags follow patch updates; the moving `lts` tag is not used. CI checks the actual server series and logs its patch version and selected CDC adapter. See the [official MySQL LTS description](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html). Add both a matrix entry and a version adapter when supporting a new LTS series. Real-server tests require a Docker/MySQL environment.

## Focused verification

| Change area | Regression entry point |
| --- | --- |
| Raw protocol, CRC, borrowed lifetimes | `crates/mysql-replication/tests/events.rs` |
| Canal row decoding and layout adapters | `crates/mysql-canal/tests/integration/decoder.rs`, `event_adapters.rs` |
| Conversion, commits and checkpoints | `converter.rs`, `processor.rs` in the same directory |
| Multiple sources and replay | `multi_source.rs`, `replay.rs` in the same directory |
| CLI arguments and output | `crates/pikachu/tests/integration/cli.rs` |

```sh
cargo test -p mysql-replication --locked
cargo test -p mysql-canal --locked
cargo test -p pikachu --locked
python3 -m unittest discover -s scripts/release -p 'test_*.py'
```

Benchmarks and their measurement limits are in the [development guide](development.md#performance-design); package verification is in the [release guide](releasing.md) (Chinese).
