#[path = "../support/mod.rs"]
mod common;

use common::*;
use mysql_canal::{
    Error, OwnedChange, Result,
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
}

impl Log {
    fn new() -> Self {
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
        self.append(
            DELETE_ROWS_EVENT,
            &rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 42, 0, 0, 0]),
        );
    }

    fn replay(&self, budget: usize, sink: &dyn TransactionSink) -> Result<()> {
        let path = self.directory.join(&self.snapshot.file);
        let state_path = self.directory.join("state.json");
        self.snapshot.save(&state_path).unwrap();
        let saved = fs::read(&state_path).unwrap();
        fs::write(&path, &self.bytes).unwrap();
        let result = Replay::new(State::read(&state_path).unwrap(), budget)
            .unwrap()
            .run(&path, sink);
        assert_eq!(fs::read(state_path).unwrap(), saved);
        result
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[test]
fn replay_uses_saved_schema_boundary_and_delivers_only_commits() {
    let mut log = Log::new();
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
        serde_json::json!({
            "source": "test", "schema": "app", "table": "users",
            "operation": "delete", "pk": {"type": "signed", "value": 42},
            "changed": []
        })
    );
}

#[test]
fn replay_rejects_checkpoint_inside_event_or_beyond_eof() {
    let mut log = Log::new();
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

#[test]
fn replay_validates_commit_position_before_delivering_changes() {
    let mut log = Log::new();
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

#[test]
fn replay_rejects_incomplete_transactions_ddl_truncation_and_checksum_errors() {
    let mut log = Log::new();
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

#[test]
fn replay_propagates_sink_failure_and_transaction_budget() {
    struct FailingSink(AtomicUsize);
    impl TransactionSink for FailingSink {
        fn commit(&self, source: &str, checkpoint: &State, changes: &[OwnedChange]) -> Result<()> {
            assert_eq!(source, "test");
            assert_eq!(checkpoint.file, "binlog.000001");
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].pk, mysql_canal::PrimaryKey::Signed(42));
            self.0.fetch_add(1, Ordering::Relaxed);
            Err(Error::Sink("test failure".into()))
        }
    }
    let mut log = Log::new();
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

#[test]
fn replay_requires_matching_filename() {
    let log = Log::new();
    let error = Replay::new(log.snapshot.clone(), 1024)
        .unwrap()
        .run(
            &log.directory.join("binlog.000002"),
            &JsonLinesSink::new(Vec::new()),
        )
        .unwrap_err();
    assert!(matches!(error, Error::Schema(message)
        if message == "replay filename must match the state's binlog file"));
}
