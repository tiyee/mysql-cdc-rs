#[path = "../support/mod.rs"]
mod common;
const UNKNOWN_TEST_EVENT: u8 = 99;
use common::*;
use pikachu_rs::{
    Error,
    engine::checkpoint::{State, StateLock},
    engine::processor::Processor,
    protocol::mysql::file::{BinlogFile, EventView},
};
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
};

fn feed(
    p: &mut Processor,
    ty: u8,
    pos: u32,
    data: &[u8],
    out: &mut impl Write,
) -> pikachu_rs::Result<()> {
    p.process(
        EventView {
            event_type: ty,
            flags: 0,
            log_pos: pos,
            data,
        },
        out,
    )
}
fn setup() -> Processor {
    Processor::new(state(), None, 1024 * 1024).unwrap()
}
fn start(p: &mut Processor, out: &mut impl Write) {
    feed(p, FORMAT_DESCRIPTION_EVENT, 100, &format(), out).unwrap();
    feed(p, QUERY_EVENT, 150, &query("BEGIN"), out).unwrap();
    feed(
        p,
        TABLE_MAP_EVENT,
        200,
        &table_map(1, &basic(), &[3, 15, 3], &[100, 0]),
        out,
    )
    .unwrap();
    feed(
        p,
        DELETE_ROWS_EVENT,
        250,
        &rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 1, 0, 0, 0]),
        out,
    )
    .unwrap();
}
fn temporary(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("pikachu-cdc-{}-{name}.json", std::process::id()))
}

#[test]
fn invisible_until_commit_and_rollback_discards() {
    let mut p = setup();
    let mut out = Vec::new();
    start(&mut p, &mut out);
    assert!(out.is_empty());
    feed(&mut p, XID_EVENT, 300, &1u64.to_le_bytes(), &mut out).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(json["schema"], "app");
    assert_eq!(json["pk"]["value"], 1);
    assert_eq!(json["changed"], serde_json::json!([]));
    assert_eq!(json["operation"], "delete");
    let mut p = setup();
    let mut out = Vec::new();
    start(&mut p, &mut out);
    feed(&mut p, QUERY_EVENT, 300, &query("ROLLBACK"), &mut out).unwrap();
    assert!(out.is_empty());
    p.finish().unwrap();
}

#[test]
fn incomplete_transaction_and_unsupported_events_stop() {
    let mut p = setup();
    let mut out = Vec::new();
    start(&mut p, &mut out);
    assert!(p.finish().is_err());
    assert!(matches!(
        feed(&mut p, TRANSACTION_PAYLOAD_EVENT, 300, &[], &mut out),
        Err(Error::Unsupported(_))
    ));
    assert!(out.is_empty());
    let mut p = setup();
    feed(&mut p, FORMAT_DESCRIPTION_EVENT, 100, &format(), &mut out).unwrap();
    assert!(matches!(
        feed(
            &mut p,
            QUERY_EVENT,
            300,
            &query("ALTER TABLE app.users RENAME COLUMN name TO n"),
            &mut out
        ),
        Err(Error::Statement(_))
    ));
    assert!(feed(&mut p, UNKNOWN_TEST_EVENT, 300, &[], &mut out).is_err());
    p.process(
        EventView {
            event_type: UNKNOWN_TEST_EVENT,
            flags: 0x80,
            log_pos: 300,
            data: &[],
        },
        &mut out,
    )
    .unwrap();
}

struct FailFlush;
impl Write for FailFlush {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("sink failed"))
    }
}
#[test]
fn checkpoint_only_after_successful_output_flush() {
    let path = temporary("flush");
    state().save(&path).unwrap();
    let mut p = Processor::new(state(), Some(path.clone()), 1024 * 1024).unwrap();
    let mut out = Vec::new();
    start(&mut p, &mut out);
    assert!(feed(&mut p, XID_EVENT, 300, &[0; 8], &mut FailFlush).is_err());
    assert_eq!(State::read(&path).unwrap().position, 4);
    let mut p = Processor::new(state(), Some(path.clone()), 1024 * 1024).unwrap();
    start(&mut p, &mut out);
    feed(&mut p, XID_EVENT, 300, &[0; 8], &mut out).unwrap();
    assert_eq!(State::read(&path).unwrap().position, 300);
    fs::remove_file(path).unwrap();
}

#[test]
fn rotation_changes_file_and_invalidates_maps() {
    let mut p = setup();
    let mut out = Vec::new();
    feed(&mut p, FORMAT_DESCRIPTION_EVENT, 100, &format(), &mut out).unwrap();
    let mut rotate = 4u64.to_le_bytes().to_vec();
    rotate.extend(b"binlog.000002");
    feed(&mut p, ROTATE_EVENT, 200, &rotate, &mut out).unwrap();
    assert_eq!(p.state().file, "binlog.000002");
    assert_eq!(p.state().position, 4);
    feed(&mut p, FORMAT_DESCRIPTION_EVENT, 100, &format(), &mut out).unwrap();
    feed(&mut p, QUERY_EVENT, 150, &query("BEGIN"), &mut out).unwrap();
    assert!(matches!(
        feed(
            &mut p,
            DELETE_ROWS_EVENT,
            200,
            &rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 1, 0, 0, 0]),
            &mut out
        ),
        Err(Error::MissingTable(1))
    ));
}

#[test]
fn transaction_budget_is_enforced_before_output() {
    let mut p = Processor::new(state(), None, 1).unwrap();
    let mut out = Vec::new();
    feed(&mut p, FORMAT_DESCRIPTION_EVENT, 100, &format(), &mut out).unwrap();
    feed(&mut p, QUERY_EVENT, 150, &query("BEGIN"), &mut out).unwrap();
    feed(
        &mut p,
        TABLE_MAP_EVENT,
        200,
        &table_map(1, &basic(), &[3, 15, 3], &[100, 0]),
        &mut out,
    )
    .unwrap();
    assert!(matches!(
        feed(
            &mut p,
            DELETE_ROWS_EVENT,
            250,
            &rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 1, 0, 0, 0]),
            &mut out
        ),
        Err(Error::TransactionLimit(1))
    ));
    assert!(out.is_empty());
}

#[test]
fn state_lock_is_exclusive_and_released_on_drop() {
    let path = temporary("lock");
    let first = StateLock::acquire(&path).unwrap();
    assert!(StateLock::acquire(&path).is_err());
    drop(first);
    drop(StateLock::acquire(&path).unwrap());
    let mut lock = path.into_os_string();
    lock.push(".lock");
    fs::remove_file(lock).unwrap();
}

#[test]
fn crc32_and_checksum_none_and_truncated_files() {
    for crc in [false, true] {
        let fde = framed(FORMAT_DESCRIPTION_EVENT, 100, &format(), crc);
        let xid = framed(XID_EVENT, 127, &[0; 8], crc);
        let mut bytes = vec![0xfe, b'b', b'i', b'n'];
        bytes.extend(&fde);
        bytes.extend(&xid);
        let mut file = BinlogFile::new(bytes.as_slice()).unwrap();
        assert_eq!(
            file.next_event().unwrap().unwrap().event_type,
            FORMAT_DESCRIPTION_EVENT
        );
        assert_eq!(file.next_event().unwrap().unwrap().data, &[0; 8]);
        assert!(file.next_event().unwrap().is_none());
        for n in 5..4 + fde.len() {
            let mut file = BinlogFile::new(&bytes[..n]).unwrap();
            assert!(file.next_event().is_err(), "prefix {n}");
        }
        if crc {
            let mut corrupted = bytes.clone();
            corrupted[25] ^= 1;
            let mut file = BinlogFile::new(corrupted.as_slice()).unwrap();
            assert!(matches!(file.next_event(), Err(Error::Checksum)));
        }
    }
}

#[test]
fn fde_header_layout_is_checked() {
    let mut p = setup();
    let mut out = Vec::new();
    let mut fde = format();
    fde[56] = 20;
    assert!(feed(&mut p, FORMAT_DESCRIPTION_EVENT, 100, &fde, &mut out).is_err());
    assert!(feed(&mut p, DELETE_ROWS_EVENT, 100, &[], &mut out).is_err());
}

#[test]
fn offline_eof_does_not_hide_partial_event_header() {
    let mut bytes = vec![0xfe, b'b', b'i', b'n'];
    bytes.extend(framed(FORMAT_DESCRIPTION_EVENT, 100, &format(), true));
    bytes.push(0);
    let mut file = BinlogFile::new(bytes.as_slice()).unwrap();
    file.next_event().unwrap();
    assert!(file.next_event().is_err());
    let mut empty = BinlogFile::new([0xfe, b'b', b'i', b'n'].as_slice()).unwrap();
    assert!(empty.next_event().is_err());
}
