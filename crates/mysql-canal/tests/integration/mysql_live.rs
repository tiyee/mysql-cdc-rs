//! Real SQL -> replication stream -> row decoder -> JSON sink/checkpoint tests.
//! Run serially: each fixture creates/drops schemas and CDC deliberately rejects DDL.
#[path = "../support/mysql_live/mod.rs"]
mod live;

use live::{CapturedOutput, Fixture, expected};
use mysql::prelude::Queryable;
use mysql_canal::{
    Cdc, CommittedTransaction, Error, OwnedChange, Result,
    config::{AppConfig, Config, SourceConfig, SourceEntry, WhitelistTable},
    engine::checkpoint::State,
    protocol::mysql::event_type as event,
    sink::{JsonLinesSink, TransactionSink},
};
use serde_json::{Value, json};
use std::{sync::mpsc, thread, time::Duration};

const ITEMS: &[(&str, &str)] = &[("items", "id INT PRIMARY KEY, note VARCHAR(80)")];

fn key(id: i32) -> Value {
    json!({"type": "signed", "value": id})
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_primary_key_preflight_stops_fresh_and_resumed_consumption() {
    for (alter, reason) in [
        (
            "DROP PRIMARY KEY",
            "requires exactly one primary-key column; found 0",
        ),
        (
            "DROP PRIMARY KEY, ADD UNIQUE KEY (id)",
            "requires exactly one primary-key column; found 0",
        ),
        (
            "DROP PRIMARY KEY, MODIFY note VARCHAR(80) NOT NULL, ADD PRIMARY KEY (id,note)",
            "requires exactly one primary-key column; found 2",
        ),
        ("MODIFY id BINARY(4) NOT NULL", "primary-key type binary"),
        ("MODIFY id DOUBLE NOT NULL", "primary-key type double"),
        ("MODIFY id DATE NOT NULL", "primary-key type date"),
        (
            "MODIFY id VARCHAR(40) CHARACTER SET gbk NOT NULL",
            "primary-key charset Some(\"gbk\")",
        ),
    ] {
        let mut fixture = Fixture::new(ITEMS);
        fixture.sql("CREATE TABLE no_key_outside_whitelist (id INT) ENGINE=InnoDB");
        let schema = fixture.schema.clone();
        fixture.sql(&format!("ALTER TABLE items {alter}"));
        let before = std::fs::read(&fixture.config.state_file).unwrap();
        for resume in [true, false] {
            if !resume {
                std::fs::remove_file(&fixture.config.state_file).unwrap();
            }
            let new_cdc = || {
                Cdc::new(AppConfig {
                    sources: vec![SourceEntry {
                        id: live::SOURCE_ID.into(),
                        config: Config {
                            source: SourceConfig {
                                plugin: fixture.config.source.plugin.clone(),
                                adapter: fixture.config.source.adapter.clone(),
                            },
                            replication: fixture.config.replication.clone(),
                            state_file: fixture.config.state_file.clone(),
                            max_transaction_bytes: fixture.config.max_transaction_bytes,
                            tables: vec![WhitelistTable {
                                schema: schema.clone(),
                                table: "items".into(),
                            }],
                        },
                    }],
                })
                .unwrap()
            };
            assert!(
                matches!(new_cdc().check(), Err(Error::Sources(ref message))
                if message.contains(&schema) && message.contains(reason)),
                "check: {alter}"
            );
            assert!(
                matches!(new_cdc().initialize(), Err(Error::Sources(ref message))
                if message.contains(&schema) && message.contains(reason)),
                "initialize: {alter}"
            );
            let mut cdc = new_cdc();
            cdc.add_handle(|_: CommittedTransaction<'_>| {
                panic!("preflight failure must prevent all transaction callbacks");
            });
            assert!(
                matches!(cdc.run_once(), Err(Error::Sources(ref message))
                if message.contains(&schema) && message.contains(reason)),
                "{alter}"
            );
            if resume {
                assert_eq!(std::fs::read(&fixture.config.state_file).unwrap(), before);
            } else {
                assert!(!fixture.config.state_file.exists());
            }
            assert!(
                !fixture
                    .config
                    .state_file
                    .with_extension("json.lock")
                    .exists()
            );
        }
    }
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_insert_update_delete_assert_complete_changes() {
    let mut fixture = Fixture::new(&[(
        "users",
        "id BIGINT UNSIGNED PRIMARY KEY, name VARCHAR(100) CHARACTER SET utf8mb4, amount DECIMAL(12,4), flags BIT(9), kind ENUM('a','b'), tags SET('a','b'), body JSON, created_at DATETIME(6), raw_data BLOB",
    )]);
    fixture.sql("INSERT INTO users VALUES (18446744073709551615,'Alice',12345678.9000,b'100000001','b','a,b',JSON_OBJECT('v',1),NOW(6),X'010203')");
    fixture
        .sql("UPDATE users SET name=NULL, body=JSON_OBJECT('v',2) WHERE id=18446744073709551615");
    fixture.sql("DELETE FROM users WHERE id=18446744073709551615");
    let pk = json!({"type": "unsigned", "value": u64::MAX});
    let (changes, _) = fixture.collect(&fixture.initial);
    assert_eq!(
        changes,
        [
            expected(
                &fixture,
                "users",
                "insert",
                pk.clone(),
                &[
                    "id",
                    "name",
                    "amount",
                    "flags",
                    "kind",
                    "tags",
                    "body",
                    "created_at",
                    "raw_data"
                ]
            ),
            expected(&fixture, "users", "update", pk.clone(), &["name", "body"]),
            expected(&fixture, "users", "delete", pk, &[]),
        ]
    );
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_multi_row_insert_update_delete_and_zero_matches() {
    let mut fixture = Fixture::new(ITEMS);
    fixture.sql("INSERT INTO items VALUES (1,'a'),(2,'b'),(3,'c')");
    let (changes, checkpoint) = fixture.collect(&fixture.initial);
    live::assert_unordered(
        changes,
        (1..=3)
            .map(|id| expected(&fixture, "items", "insert", key(id), &["id", "note"]))
            .collect(),
    );
    fixture.sql("UPDATE items SET note=NULL WHERE id IN (1,3)");
    let (changes, checkpoint) = fixture.collect(&checkpoint);
    live::assert_unordered(
        changes,
        [1, 3]
            .map(|id| expected(&fixture, "items", "update", key(id), &["note"]))
            .to_vec(),
    );
    fixture.sql("DELETE FROM items WHERE id IN (1,2,3)");
    let (changes, checkpoint) = fixture.collect(&checkpoint);
    live::assert_unordered(
        changes,
        (1..=3)
            .map(|id| expected(&fixture, "items", "delete", key(id), &[]))
            .collect(),
    );
    fixture.sql("UPDATE items SET note='absent' WHERE id=999");
    fixture.sql("DELETE FROM items WHERE id=999");
    assert!(fixture.collect(&checkpoint).0.is_empty());
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_all_integer_key_widths_and_signed_unsigned_boundaries() {
    let cases = [
        ("tiny_signed", "TINYINT", json!(-128), json!(127)),
        ("small_signed", "SMALLINT", json!(-32768), json!(32767)),
        (
            "medium_signed",
            "MEDIUMINT",
            json!(-8388608),
            json!(8388607),
        ),
        ("int_signed", "INT", json!(i32::MIN), json!(i32::MAX)),
        ("big_signed", "BIGINT", json!(i64::MIN), json!(i64::MAX)),
        (
            "tiny_unsigned",
            "TINYINT UNSIGNED",
            json!(0),
            json!(u8::MAX),
        ),
        (
            "small_unsigned",
            "SMALLINT UNSIGNED",
            json!(0),
            json!(u16::MAX),
        ),
        (
            "medium_unsigned",
            "MEDIUMINT UNSIGNED",
            json!(0),
            json!(16777215),
        ),
        ("int_unsigned", "INT UNSIGNED", json!(0), json!(u32::MAX)),
        ("big_unsigned", "BIGINT UNSIGNED", json!(0), json!(u64::MAX)),
    ];
    let definitions: Vec<_> = cases
        .iter()
        .map(|(name, ty, _, _)| (*name, format!("note VARCHAR(80), id {ty} PRIMARY KEY")))
        .collect();
    let tables: Vec<_> = definitions
        .iter()
        .map(|(name, ddl)| (*name, ddl.as_str()))
        .collect();
    let mut fixture = Fixture::new(&tables);
    let mut wanted = Vec::new();
    for (table, ty, minimum, maximum) in cases {
        fixture.sql(&format!(
            "INSERT INTO {table} VALUES ('minimum',{minimum}),('maximum',{maximum})"
        ));
        fixture.sql(&format!("UPDATE {table} SET note=NULL WHERE id={minimum}"));
        fixture.sql(&format!("DELETE FROM {table} WHERE id={maximum}"));
        let kind = if ty.ends_with("UNSIGNED") {
            "unsigned"
        } else {
            "signed"
        };
        let min_pk = json!({"type": kind, "value": minimum});
        let max_pk = json!({"type": kind, "value": maximum});
        wanted.extend([
            expected(&fixture, table, "insert", min_pk.clone(), &["note", "id"]),
            expected(&fixture, table, "insert", max_pk.clone(), &["note", "id"]),
            expected(&fixture, table, "update", min_pk, &["note"]),
            expected(&fixture, table, "delete", max_pk, &[]),
        ]);
    }
    live::assert_unordered(fixture.collect(&fixture.initial).0, wanted);
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_text_decimal_and_numeric_primary_key_changes() {
    let cases = [
        (
            "utf8_keys",
            "VARCHAR(40) CHARACTER SET utf8mb4",
            "text",
            json!("你好🙂"),
            json!("新🙂键"),
        ),
        (
            "char_keys",
            "CHAR(20) CHARACTER SET utf8mb4",
            "text",
            json!("你好"),
            json!("新键"),
        ),
        (
            "ascii_keys",
            "VARCHAR(40) CHARACTER SET ascii",
            "text",
            json!("old"),
            json!("new"),
        ),
        (
            "latin_keys",
            "VARCHAR(40) CHARACTER SET latin1",
            "text",
            json!("café"),
            json!("élève"),
        ),
        (
            "decimal_keys",
            "DECIMAL(10,2)",
            "decimal",
            json!("-12345678.90"),
            json!("12.30"),
        ),
        (
            "numeric_keys",
            "BIGINT UNSIGNED",
            "unsigned",
            json!(u64::MAX),
            json!(7),
        ),
    ];
    let definitions: Vec<_> = cases
        .iter()
        .map(|(name, ty, _, _, _)| (*name, format!("id {ty} PRIMARY KEY, note VARCHAR(80)")))
        .collect();
    let tables: Vec<_> = definitions
        .iter()
        .map(|(name, ddl)| (*name, ddl.as_str()))
        .collect();
    let mut fixture = Fixture::new(&tables);
    let mut wanted = Vec::new();
    for (table, _, kind, old, new) in cases {
        let old_sql = old
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| old.to_string());
        let new_sql = new
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| new.to_string());
        fixture.sql(&format!("INSERT INTO {table} VALUES ('{old_sql}','old')"));
        fixture.sql(&format!(
            "UPDATE {table} SET id='{new_sql}', note=NULL WHERE id='{old_sql}'"
        ));
        fixture.sql(&format!("DELETE FROM {table} WHERE id='{new_sql}'"));
        let old_pk = json!({"type": kind, "value": old});
        let new_pk = json!({"type": kind, "value": new});
        let mut updated = expected(&fixture, table, "update", new_pk.clone(), &["id", "note"]);
        updated["previous_pk"] = old_pk.clone();
        wanted.extend([
            expected(&fixture, table, "insert", old_pk, &["id", "note"]),
            updated,
            expected(&fixture, table, "delete", new_pk, &[]),
        ]);
    }
    assert_eq!(fixture.collect(&fixture.initial).0, wanted);
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_wide_sparse_null_images_with_key_after_column_64() {
    let names: Vec<_> = (0..80)
        .map(|i| {
            if i == 69 {
                "id".to_owned()
            } else {
                format!("c{i}")
            }
        })
        .collect();
    let ddl = names
        .iter()
        .map(|name| {
            if name == "id" {
                "id BIGINT PRIMARY KEY".to_owned()
            } else {
                format!("{name} INT NULL")
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut fixture = Fixture::new(&[("wide", &ddl)]);
    fixture.sql("INSERT INTO wide (id,c0,c79) VALUES (-42,11,79)");
    fixture.sql("UPDATE wide SET c0=NULL, c65=65, c79=NULL WHERE id=-42");
    fixture.sql("DELETE FROM wide WHERE id=-42");
    assert_eq!(
        fixture.collect(&fixture.initial).0,
        [
            expected(&fixture, "wide", "insert", key(-42), &["c0", "id", "c79"]),
            expected(&fixture, "wide", "update", key(-42), &["c0", "c65", "c79"]),
            expected(&fixture, "wide", "delete", key(-42), &[]),
        ]
    );
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_whitelist_filters_tables_and_same_named_tables_in_other_schema() {
    let mut fixture = Fixture::new(ITEMS);
    let outside = fixture.excluded_schema.clone();
    fixture.sql("BEGIN");
    for table in [
        "items".to_owned(),
        "excluded".to_owned(),
        format!("`{outside}`.items"),
    ] {
        fixture.sql(&format!("INSERT INTO {table} VALUES (1,'a')"));
        fixture.sql(&format!("UPDATE {table} SET note=NULL WHERE id=1"));
        fixture.sql(&format!("DELETE FROM {table} WHERE id=1"));
    }
    fixture.sql("COMMIT");
    let (changes, checkpoint) = fixture.collect(&fixture.initial);
    assert_eq!(
        changes,
        [
            expected(&fixture, "items", "insert", key(1), &["id", "note"]),
            expected(&fixture, "items", "update", key(1), &["note"]),
            expected(&fixture, "items", "delete", key(1), &[]),
        ]
    );
    fixture.sql("INSERT INTO excluded VALUES (2,'filtered transaction')");
    let (changes, advanced) = fixture.collect(&checkpoint);
    assert!(changes.is_empty());
    assert!(advanced.position > checkpoint.position);
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_live_monitor_buffers_rows_until_transaction_commit() {
    let mut fixture = Fixture::new(ITEMS);
    let mut stream = fixture.stream(&fixture.initial, false);
    let mut processor = fixture.processor(&fixture.initial);
    let output = CapturedOutput::default();
    let observed = output.clone();
    let sink = JsonLinesSink::new(output);
    let initial = fixture.initial.clone();
    let state_path = fixture.config.state_file.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let in_worker = observed.clone();
    let worker = thread::spawn(move || {
        let mut row_events = 0;
        let mut ready = false;
        while let Some(raw) = stream.next_event().unwrap() {
            let kind = raw.event_type;
            let is_rows = matches!(
                kind,
                event::WRITE_ROWS_EVENT | event::UPDATE_ROWS_EVENT | event::DELETE_ROWS_EVENT
            );
            processor.process_to(raw, &sink).unwrap();
            if kind == event::FORMAT_DESCRIPTION_EVENT && !ready {
                ready_tx.send(()).unwrap();
                ready = true;
            }
            if is_rows {
                row_events += 1;
                assert!(
                    in_worker.is_empty(),
                    "row output became visible before XID/COMMIT"
                );
                assert_eq!(State::read(&state_path).unwrap().position, initial.position);
            }
            if kind == event::XID_EVENT && row_events > 0 {
                assert!(
                    row_events >= 3,
                    "must receive INSERT, UPDATE and DELETE row events"
                );
                processor.finish().unwrap();
                done_tx.send(()).unwrap();
                return;
            }
        }
        panic!("live stream ended before the transaction commit");
    });
    ready_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("CDC must process the FDE before writes begin");
    fixture.sql("BEGIN");
    fixture.sql("INSERT INTO items VALUES (1,'a'),(2,'b')");
    fixture.sql("UPDATE items SET note=NULL WHERE id=1");
    fixture.sql("DELETE FROM items WHERE id=2");
    assert!(observed.is_empty());
    assert_eq!(fixture.saved().position, fixture.initial.position);
    fixture.sql("COMMIT");
    done_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("CDC must observe the committed transaction");
    worker.join().unwrap();
    assert_eq!(
        observed.changes(),
        [
            expected(&fixture, "items", "insert", key(1), &["id", "note"]),
            expected(&fixture, "items", "insert", key(2), &["id", "note"]),
            expected(&fixture, "items", "update", key(1), &["note"]),
            expected(&fixture, "items", "delete", key(2), &[]),
        ]
    );
    assert!(fixture.saved().position > fixture.initial.position);
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_rollback_discards_insert_update_delete() {
    let mut fixture = Fixture::new(ITEMS);
    fixture.sql("INSERT INTO items VALUES (1,'original')");
    let (_, checkpoint) = fixture.collect(&fixture.initial);
    fixture.sql("BEGIN");
    fixture.sql("INSERT INTO items VALUES (2,'rolled back')");
    fixture.sql("UPDATE items SET note=NULL WHERE id=1");
    fixture.sql("DELETE FROM items WHERE id=1");
    fixture.sql("ROLLBACK");
    let (changes, checkpoint) = fixture.collect(&checkpoint);
    assert!(
        changes.is_empty(),
        "rolled-back rows must not reach the sink"
    );
    let rows: Vec<(i32, String)> = fixture
        .connection()
        .query("SELECT id,note FROM items ORDER BY id")
        .unwrap();
    assert_eq!(rows, [(1, "original".to_owned())]);
    fixture.sql("UPDATE items SET note='committed' WHERE id=1");
    assert_eq!(
        fixture.collect(&checkpoint).0,
        [expected(&fixture, "items", "update", key(1), &["note"])]
    );
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_restart_from_persisted_checkpoint_only_emits_new_changes() {
    let mut fixture = Fixture::new(ITEMS);
    fixture.sql("INSERT INTO items VALUES (1,'first')");
    let (first, checkpoint) = fixture.collect(&fixture.initial);
    assert_eq!(
        first,
        [expected(
            &fixture,
            "items",
            "insert",
            key(1),
            &["id", "note"]
        )]
    );
    let saved = fixture.saved();
    assert_eq!(
        (saved.file.as_str(), saved.position),
        (checkpoint.file.as_str(), checkpoint.position)
    );
    assert!(fixture.collect(&saved).0.is_empty());
    fixture.sql("UPDATE items SET note=NULL WHERE id=1");
    fixture.sql("INSERT INTO items VALUES (2,'second')");
    assert_eq!(
        fixture.collect(&saved).0,
        [
            expected(&fixture, "items", "update", key(1), &["note"]),
            expected(&fixture, "items", "insert", key(2), &["id", "note"]),
        ]
    );
    assert!(fixture.collect(&fixture.saved()).0.is_empty());
}

struct RejectChanges;
impl TransactionSink for RejectChanges {
    fn commit(&self, _source: &str, _state: &State, changes: &[OwnedChange]) -> Result<()> {
        if changes.is_empty() {
            Ok(())
        } else {
            Err(Error::Sink("integration sink rejection".into()))
        }
    }
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_sink_failure_preserves_checkpoint_and_allows_replay() {
    let mut fixture = Fixture::new(ITEMS);
    fixture.sql("INSERT INTO items VALUES (1,'retry')");
    assert!(
        matches!(fixture.consume(&fixture.initial, &RejectChanges), Err(Error::Sink(message)) if message == "integration sink rejection")
    );
    let saved = fixture.saved();
    assert_eq!(
        (saved.file.as_str(), saved.position),
        (fixture.initial.file.as_str(), fixture.initial.position)
    );
    assert_eq!(
        fixture.collect(&saved).0,
        [expected(
            &fixture,
            "items",
            "insert",
            key(1),
            &["id", "note"]
        )]
    );
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_ddl_stops_without_acknowledging_new_schema() {
    let mut fixture = Fixture::new(ITEMS);
    fixture.sql("INSERT INTO items VALUES (1,'before ddl')");
    let (_, checkpoint) = fixture.collect(&fixture.initial);
    fixture.sql("ALTER TABLE items ADD COLUMN extra INT");
    fixture.sql("UPDATE items SET extra=5 WHERE id=1");
    let output = CapturedOutput::default();
    let sink = JsonLinesSink::new(output.clone());
    let error = fixture
        .consume(&checkpoint, &sink)
        .expect_err("ALTER TABLE must stop consumption");
    assert!(
        matches!(&error, Error::Statement(sql) if sql == "ALTER TABLE items ADD COLUMN extra INT"),
        "expected the DDL statement error, got {error:?}"
    );
    assert!(output.is_empty());
    let saved = fixture.saved();
    assert_eq!(
        (saved.file.as_str(), saved.position),
        (checkpoint.file.as_str(), checkpoint.position)
    );
}
