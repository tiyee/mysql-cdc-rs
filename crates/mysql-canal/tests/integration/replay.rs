#[path = "../support/mod.rs"]
mod common;

use common::*;
use mysql_canal::{
    Error, OwnedChange, Result,
    config::RowImageMode,
    engine::{Replay, checkpoint::State},
    sink::{JsonLinesSink, TransactionSink},
};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Log {
    directory: PathBuf,
    bytes: Vec<u8>,
    snapshot: State,
    row_image: RowImageMode,
}

impl Log {
    fn new(row_image: RowImageMode) -> Self {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let directory = std::env::temp_dir().join(format!(
            "canal-replay-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let mut log = Self {
            directory,
            bytes: vec![0xfe, b'b', b'i', b'n'],
            snapshot: state(),
            row_image,
        };
        log.append(FORMAT_DESCRIPTION_EVENT, &format());
        log
    }

    fn append(&mut self, kind: u8, body: &[u8]) {
        let size = framed(kind, 0, body, true).len();
        self.bytes
            .extend(framed(kind, (self.bytes.len() + size) as u32, body, true));
    }

    fn delete(&mut self) {
        self.append(QUERY_EVENT, &query("BEGIN"));
        self.append(
            TABLE_MAP_EVENT,
            &table_map(1, &basic(), &[3, 15, 3], &[100, 0]),
        );
        let (present, data) = match self.row_image {
            RowImageMode::Minimal => (1, vec![0, 42, 0, 0, 0]),
            RowImageMode::Full => (7, full_image(42, None, 20)),
        };
        self.append(
            DELETE_ROWS_EVENT,
            &rows(1, DELETE_ROWS_EVENT, 3, &[present], &[], &data),
        );
    }

    fn replay(&self, budget: usize, sink: &dyn TransactionSink) -> Result<()> {
        let path = self.directory.join(&self.snapshot.file);
        let state_path = self.directory.join("state.json");
        self.snapshot.save(&state_path).unwrap();
        let saved = fs::read(&state_path).unwrap();
        fs::write(&path, &self.bytes).unwrap();
        let result = Replay::new(
            State::read(&state_path).unwrap(),
            budget,
            mysql_canal::protocol::mysql::version::VersionRegistry::default(),
            None,
            self.row_image,
        )
        .unwrap()
        .run(&path, sink);
        assert_eq!(fs::read(state_path).unwrap(), saved);
        result
    }
}

fn full_image(id: i32, name: Option<&str>, age: i32) -> Vec<u8> {
    let mut data = vec![if name.is_none() { 2 } else { 0 }];
    data.extend(id.to_le_bytes());
    if let Some(name) = name {
        data.push(name.len() as u8);
        data.extend(name.as_bytes());
    }
    data.extend(age.to_le_bytes());
    data
}

impl Drop for Log {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[test]
fn replay_uses_saved_schema_boundary_and_delivers_only_commits() {
    for mode in [RowImageMode::Minimal, RowImageMode::Full] {
        let mut log = Log::new(mode);
        log.append(QUERY_EVENT, &query("ALTER TABLE app.users DROP age"));
        log.append(TABLE_MAP_EVENT, &[0]);
        log.snapshot.position = log.bytes.len() as u64;
        log.delete();
        log.append(QUERY_EVENT, &query("ROLLBACK"));
        log.delete();
        log.append(XID_EVENT, &7u64.to_le_bytes());
        let mut output = Vec::new();
        log.replay(1024 * 1024, &JsonLinesSink::new(&mut output))
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output).unwrap(),
            with_fields(
                serde_json::json!({
                    "source": "test", "schema": "app", "table": "users",
                    "operation": "delete", "pk": {"type": "signed", "value": 42},
                    "changed": []
                }),
                &basic(),
                Some(&match mode {
                    RowImageMode::Minimal => vec![(0, serde_json::json!(42))],
                    RowImageMode::Full => vec![
                        (0, serde_json::json!(42)),
                        (1, serde_json::Value::Null),
                        (2, serde_json::json!(20)),
                    ],
                }),
                None
            )
        );
    }
}

#[test]
fn replay_rejects_checkpoint_inside_event_or_beyond_eof() {
    for mode in [RowImageMode::Minimal, RowImageMode::Full] {
        let mut log = Log::new(mode);
        for (position, expected) in [
            (5, "state position is not an event boundary"),
            (log.bytes.len() as u64 + 1, "state position beyond EOF"),
        ] {
            log.snapshot.position = position;
            let mut output = Vec::new();
            assert!(matches!(
                log.replay(1024, &JsonLinesSink::new(&mut output)),
                Err(Error::Invalid(message)) if message == expected
            ));
            assert!(output.is_empty());
        }
        log.snapshot.position = log.bytes.len() as u64;
        log.replay(1024, &JsonLinesSink::new(Vec::new())).unwrap();
    }
}

#[test]
fn replay_validates_commit_position_before_delivering_changes() {
    for mode in [RowImageMode::Minimal, RowImageMode::Full] {
        let mut log = Log::new(mode);
        log.delete();
        log.bytes.extend(framed(
            XID_EVENT,
            log.bytes.len() as u32,
            &7u64.to_le_bytes(),
            true,
        ));
        let mut output = Vec::new();
        assert!(matches!(
            log.replay(1024 * 1024, &JsonLinesSink::new(&mut output)),
            Err(Error::Invalid(message)) if message == "non-increasing offline event position"
        ));
        assert!(output.is_empty());
    }
}

#[test]
fn replay_rejects_incomplete_transactions_ddl_truncation_and_checksum_errors() {
    for mode in [RowImageMode::Minimal, RowImageMode::Full] {
        let mut log = Log::new(mode);
        log.delete();
        let mut output = Vec::new();
        assert!(matches!(
            log.replay(1024 * 1024, &JsonLinesSink::new(&mut output)),
            Err(Error::Invalid(message)) if message.contains("inside a transaction")
        ));
        assert!(output.is_empty());
        let pending = log.bytes.clone();
        log.append(QUERY_EVENT, &query("ALTER TABLE app.users DROP age"));
        assert!(matches!(
            log.replay(1024 * 1024, &JsonLinesSink::new(&mut output)),
            Err(Error::Statement(statement)) if statement == "ALTER TABLE app.users DROP age"
        ));
        assert!(output.is_empty());
        log.bytes = pending;
        log.append(XID_EVENT, &7u64.to_le_bytes());
        let last = log.bytes.pop().unwrap();
        assert!(matches!(
            log.replay(1024 * 1024, &JsonLinesSink::new(&mut output)),
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof
        ));
        assert!(output.is_empty());
        log.bytes.push(last ^ 1);
        assert!(matches!(
            log.replay(1024 * 1024, &JsonLinesSink::new(&mut output)),
            Err(Error::Checksum)
        ));
        assert!(output.is_empty());
    }
}

#[test]
fn replay_propagates_sink_failure_and_transaction_budget() {
    for mode in [RowImageMode::Minimal, RowImageMode::Full] {
        struct FailingSink(AtomicUsize);
        impl TransactionSink for FailingSink {
            fn commit(
                &self,
                source: &str,
                checkpoint: &State,
                changes: &[OwnedChange],
            ) -> Result<()> {
                assert_eq!(source, "test");
                assert_eq!(checkpoint.file, "binlog.000001");
                assert_eq!(changes.len(), 1);
                assert_eq!(changes[0].pk, mysql_canal::PrimaryKey::Signed(42));
                self.0.fetch_add(1, Ordering::Relaxed);
                Err(Error::Sink("test failure".into()))
            }
        }
        let mut log = Log::new(mode);
        for _ in 0..2 {
            log.delete();
            log.append(XID_EVENT, &7u64.to_le_bytes());
        }
        let sink = FailingSink(AtomicUsize::new(0));
        assert!(matches!(
            log.replay(1024 * 1024, &sink),
            Err(Error::Sink(message)) if message == "test failure"
        ));
        assert_eq!(sink.0.load(Ordering::Relaxed), 1);
        let mut output = Vec::new();
        assert!(matches!(
            log.replay(1, &JsonLinesSink::new(&mut output)),
            Err(Error::TransactionLimit(1))
        ));
        assert!(output.is_empty());
    }
}

#[test]
fn replay_requires_matching_filename() {
    for mode in [RowImageMode::Minimal, RowImageMode::Full] {
        let log = Log::new(mode);
        let error = Replay::new(
            log.snapshot.clone(),
            1024,
            mysql_canal::protocol::mysql::version::VersionRegistry::default(),
            None,
            mode,
        )
        .unwrap()
        .run(
            &log.directory.join("binlog.000002"),
            &JsonLinesSink::new(Vec::new()),
        )
        .unwrap_err();
        assert!(matches!(error, Error::Schema(message)
            if message == "replay filename must match the state's binlog file"));
    }
}

#[test]
fn replay_full_multirow_operations_keep_historical_fields_and_resume_without_duplicates() {
    use serde_json::{Value, json};

    fn transaction(log: &mut Log, kinds: [u8; 3], first_id: i32) -> Vec<Value> {
        log.append(QUERY_EVENT, &query("BEGIN"));
        let meta = log.snapshot.tables[0].clone();
        log.append(
            TABLE_MAP_EVENT,
            &table_map(1, &meta, &[3, 15, 3], &[100, 0]),
        );
        let mut expected = Vec::new();
        for (kind, operation) in kinds.into_iter().zip(["insert", "update", "delete"]) {
            let mut data = Vec::new();
            for id in [first_id, first_id + 1] {
                let new_id = if id == first_id { id + 100 } else { id };
                let before = [(0, json!(id)), (1, json!("old")), (2, json!(20))];
                let after = [(0, json!(new_id)), (1, Value::Null), (2, json!(20))];
                match operation {
                    "insert" => data.extend(full_image(id, Some("old"), 20)),
                    "update" => {
                        data.extend(full_image(id, Some("old"), 20));
                        data.extend(full_image(new_id, None, 20));
                    }
                    "delete" => data.extend(full_image(new_id, None, 20)),
                    _ => unreachable!(),
                }
                let mut change = json!({
                    "source": "test", "schema": "app", "table": "users",
                    "operation": operation,
                    "pk": {"type": "signed", "value": if operation == "insert" { id } else { new_id }},
                    "changed": if operation == "delete" { vec![] } else { vec!["id", "historical_name", "age"] }
                });
                if operation == "update" && new_id != id {
                    change["previous_pk"] = json!({"type": "signed", "value": id});
                }
                expected.push(with_fields(
                    change,
                    &meta,
                    match operation {
                        "update" => Some(&before),
                        "delete" => Some(&after),
                        _ => None,
                    },
                    match operation {
                        "insert" => Some(&before),
                        "update" => Some(&after),
                        _ => None,
                    },
                ));
            }
            log.append(kind, &rows(1, kind, 3, &[7], &[7], &data));
        }
        expected
    }

    for kinds in [
        [
            WRITE_ROWS_EVENT_V1,
            UPDATE_ROWS_EVENT_V1,
            DELETE_ROWS_EVENT_V1,
        ],
        [WRITE_ROWS_EVENT, UPDATE_ROWS_EVENT, DELETE_ROWS_EVENT],
    ] {
        let mut log = Log::new(RowImageMode::Full);
        log.snapshot.tables[0].columns[1].name = "historical_name".into();
        transaction(&mut log, kinds, 1);
        log.append(QUERY_EVENT, &query("ROLLBACK"));
        let expected = transaction(&mut log, kinds, 42);
        log.append(XID_EVENT, &7u64.to_le_bytes());
        let mut output = Vec::new();
        log.replay(1024 * 1024, &JsonLinesSink::new(&mut output))
            .unwrap();
        let changes = |output: &[u8]| {
            serde_json::Deserializer::from_slice(output)
                .into_iter::<Value>()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(changes(&output), expected);

        log.snapshot.position = log.bytes.len() as u64;
        let expected = transaction(&mut log, kinds, 50);
        log.append(QUERY_EVENT, &query("COMMIT"));
        output.clear();
        log.replay(1024 * 1024, &JsonLinesSink::new(&mut output))
            .unwrap();
        assert_eq!(changes(&output), expected);
    }
}

#[test]
fn replay_full_rejects_sparse_insert_delete_and_either_update_image_without_output() {
    for (kind, first, second) in [
        (WRITE_ROWS_EVENT_V1, 1, 0),
        (WRITE_ROWS_EVENT, 1, 0),
        (DELETE_ROWS_EVENT_V1, 1, 0),
        (DELETE_ROWS_EVENT, 1, 0),
        (UPDATE_ROWS_EVENT_V1, 1, 7),
        (UPDATE_ROWS_EVENT_V1, 7, 1),
        (UPDATE_ROWS_EVENT, 1, 7),
        (UPDATE_ROWS_EVENT, 7, 1),
    ] {
        let mut log = Log::new(RowImageMode::Full);
        log.append(QUERY_EVENT, &query("BEGIN"));
        log.append(
            TABLE_MAP_EVENT,
            &table_map(1, &basic(), &[3, 15, 3], &[100, 0]),
        );
        let image = |present| {
            if present == 7 {
                full_image(42, None, 20)
            } else {
                vec![0, 42, 0, 0, 0]
            }
        };
        let mut data = image(first);
        if matches!(kind, UPDATE_ROWS_EVENT_V1 | UPDATE_ROWS_EVENT) {
            data.extend(image(second));
        }
        log.append(kind, &rows(1, kind, 3, &[first], &[second], &data));
        log.append(XID_EVENT, &7u64.to_le_bytes());
        let mut output = Vec::new();
        assert!(matches!(
            log.replay(1024 * 1024, &JsonLinesSink::new(&mut output)),
            Err(Error::Unsupported(message))
                if message == "expected FULL row images containing every column"
        ));
        assert!(output.is_empty(), "event {kind}, bitmaps {first}/{second}");
    }
}
