#[path = "../support/mod.rs"]
mod common;
const UNKNOWN_TEST_EVENT: u8 = 99;
use common::*;
use mysql_canal::{
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
    out: &mut (impl Write + Send),
) -> mysql_canal::Result<()> {
    p.process(
        EventView {
            event_type: ty,
            flags: 0,
            log_pos: pos,
            data,
        },
        &mysql_canal::sink::JsonLinesSink::new(out),
    )
}
fn setup() -> Processor {
    Processor::new(
        state(),
        None,
        1024 * 1024,
        mysql_canal::protocol::mysql::version::VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    )
    .unwrap()
}
fn start(p: &mut Processor, out: &mut (impl Write + Send)) {
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
    assert_eq!(json["source"], "test");
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
        &mysql_canal::sink::JsonLinesSink::new(&mut out),
    )
    .unwrap();
}

#[test]
fn checkpoint_requires_complete_nonempty_source_identity() {
    let path = temporary("source-identity");
    let valid = serde_json::to_value(state()).unwrap();
    for field in ["source_uuid", "source_id", "source_plugin"] {
        for value in [None, Some(serde_json::Value::Null), Some("".into())] {
            let mut invalid = valid.clone();
            if let Some(value) = value {
                invalid[field] = value;
            } else {
                invalid.as_object_mut().unwrap().remove(field);
            }
            fs::write(&path, serde_json::to_vec(&invalid).unwrap()).unwrap();
            let error = State::read(&path).expect_err("checkpoint needs explicit source identity");
            assert!(matches!(error, Error::Json(_) | Error::Invalid(_)));
        }
    }
    state().save(&path).unwrap();
    let saved = State::read(&path).unwrap();
    assert_eq!(saved.source_id, "test");
    assert_eq!(saved.source_plugin, "mysql");
    fs::remove_file(path).unwrap();
}

#[test]
fn ddl_reports_statement_without_advancing_saved_checkpoint() {
    let path = temporary("ddl");
    state().save(&path).unwrap();
    let mut p = Processor::new(
        state(),
        Some(path.clone()),
        1024 * 1024,
        mysql_canal::protocol::mysql::version::VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    )
    .unwrap();
    let mut out = Vec::new();
    start(&mut p, &mut out);
    feed(&mut p, XID_EVENT, 300, &[0; 8], &mut out).unwrap();
    let checkpoint = State::read(&path).unwrap();
    out.clear();

    let sql = "ALTER TABLE app.users ADD COLUMN extra INT";
    let error = feed(&mut p, QUERY_EVENT, 350, &query(sql), &mut out)
        .expect_err("DDL must stop consumption");
    assert!(
        matches!(&error, Error::Statement(statement) if statement == sql),
        "expected the DDL statement error, got {error:?}"
    );
    assert!(out.is_empty());
    let saved = State::read(&path).unwrap();
    assert_eq!(
        (saved.file.as_str(), saved.position),
        (checkpoint.file.as_str(), checkpoint.position)
    );
    fs::remove_file(path).unwrap();
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
    let mut p = Processor::new(
        state(),
        Some(path.clone()),
        1024 * 1024,
        mysql_canal::protocol::mysql::version::VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    )
    .unwrap();
    let mut out = Vec::new();
    start(&mut p, &mut out);
    assert!(feed(&mut p, XID_EVENT, 300, &[0; 8], &mut FailFlush).is_err());
    assert_eq!(State::read(&path).unwrap().position, 4);
    let mut p = Processor::new(
        state(),
        Some(path.clone()),
        1024 * 1024,
        mysql_canal::protocol::mysql::version::VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    )
    .unwrap();
    start(&mut p, &mut out);
    feed(&mut p, XID_EVENT, 300, &[0; 8], &mut out).unwrap();
    assert_eq!(State::read(&path).unwrap().position, 300);
    fs::remove_file(path).unwrap();
}

#[test]
fn null_in_not_null_column_stops_without_output_or_checkpoint_advance() {
    let path = temporary("not-null");
    let mut initial = state();
    initial.tables[0].columns[2].nullable = false;
    initial.save(&path).unwrap();
    let mut p = Processor::new(
        initial.clone(),
        Some(path.clone()),
        1024 * 1024,
        mysql_canal::protocol::mysql::version::VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    )
    .unwrap();
    let mut out = Vec::new();
    feed(&mut p, FORMAT_DESCRIPTION_EVENT, 100, &format(), &mut out).unwrap();
    feed(&mut p, QUERY_EVENT, 150, &query("BEGIN"), &mut out).unwrap();
    feed(
        &mut p,
        TABLE_MAP_EVENT,
        200,
        &table_map(1, &initial.tables[0], &[3, 15, 3], &[100, 0]),
        &mut out,
    )
    .unwrap();
    // Buffer one valid row, then encounter an invalid NULL in the same event.
    let data = [0, 1, 0, 0, 0, 0, 7, 0, 0, 0, 0, 2, 0, 0, 0, 1];
    let result = feed(
        &mut p,
        UPDATE_ROWS_EVENT,
        250,
        &rows(1, UPDATE_ROWS_EVENT, 3, &[1], &[4], &data),
        &mut out,
    );
    assert!(matches!(result, Err(Error::Schema(message)) if message.contains("NOT NULL")));
    assert!(out.is_empty());
    assert!(matches!(p.finish(), Err(Error::Invalid(_))));
    let saved = State::read(&path).unwrap();
    assert_eq!(saved.file, initial.file);
    assert_eq!(saved.position, initial.position);
    assert_eq!(saved.tables, initial.tables);
    fs::remove_file(path).unwrap();
}

#[test]
fn default_adapter_rejects_unvalidated_rotate_headers_without_saving_state() {
    let path = temporary("rotate-layout");
    let initial = state();
    initial.save(&path).unwrap();
    for length in [0, 7, 9, 12, 255] {
        let mut fde = format();
        fde[57 + usize::from(ROTATE_EVENT) - 1] = length;
        let mut p = Processor::new(
            initial.clone(),
            Some(path.clone()),
            1024 * 1024,
            mysql_canal::protocol::mysql::version::VersionRegistry::default(),
            None,
            mysql_canal::config::RowImageMode::Minimal,
        )
        .unwrap();
        let mut out = Vec::new();
        let result = feed(&mut p, FORMAT_DESCRIPTION_EVENT, 100, &fde, &mut out);
        assert!(
            matches!(result, Err(Error::Unsupported(message)) if message.contains("post-header")),
            "ROTATE post-header {length} must fail"
        );
        assert!(out.is_empty());
        assert_eq!(p.state().file, initial.file);
        let saved = State::read(&path).unwrap();
        assert_eq!(saved.file, initial.file);
        assert_eq!(saved.position, initial.position);
    }
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
    let mut p = Processor::new(
        state(),
        None,
        1,
        mysql_canal::protocol::mysql::version::VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    )
    .unwrap();
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

#[test]
fn full_transactions_commit_and_resume_but_mismatches_do_not_advance_disk_state() {
    use mysql_canal::{config::RowImageMode, protocol::mysql::version::VersionRegistry};
    let path = temporary("full-mode");
    let initial = state();
    initial.save(&path).unwrap();
    let mut output = Vec::new();
    let mut processor = Processor::new(
        initial,
        Some(path.clone()),
        100_000,
        VersionRegistry::default(),
        None,
        RowImageMode::Full,
    )
    .unwrap();
    feed(
        &mut processor,
        FORMAT_DESCRIPTION_EVENT,
        0,
        &format(),
        &mut output,
    )
    .unwrap();
    feed(
        &mut processor,
        QUERY_EVENT,
        100,
        &query("BEGIN"),
        &mut output,
    )
    .unwrap();
    feed(
        &mut processor,
        TABLE_MAP_EVENT,
        150,
        &table_map(7, &basic(), &[3, 15, 3], &[100, 0]),
        &mut output,
    )
    .unwrap();
    let mut data = vec![2];
    data.extend(42i32.to_le_bytes());
    data.extend(20i32.to_le_bytes());
    feed(
        &mut processor,
        WRITE_ROWS_EVENT,
        200,
        &rows(7, WRITE_ROWS_EVENT, 3, &[7], &[], &data),
        &mut output,
    )
    .unwrap();
    assert!(output.is_empty());
    assert_eq!(State::read(&path).unwrap().position, state().position);
    feed(&mut processor, XID_EVENT, 240, &[0; 8], &mut output).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output).unwrap(),
        with_fields(
            serde_json::json!({"source":"test", "schema":"app", "table":"users", "operation":"insert", "pk":{"type":"signed", "value":42}, "changed":["id","name","age"]}),
            &basic(),
            None,
            Some(&[
                (0, serde_json::json!(42)),
                (1, serde_json::Value::Null),
                (2, serde_json::json!(20))
            ])
        )
    );
    let mut resumed = Processor::new(
        State::read(&path).unwrap(),
        Some(path.clone()),
        100_000,
        VersionRegistry::default(),
        None,
        RowImageMode::Full,
    )
    .unwrap();
    output.clear();
    feed(
        &mut resumed,
        FORMAT_DESCRIPTION_EVENT,
        0,
        &format(),
        &mut output,
    )
    .unwrap();
    feed(&mut resumed, QUERY_EVENT, 260, &query("BEGIN"), &mut output).unwrap();
    feed(
        &mut resumed,
        TABLE_MAP_EVENT,
        280,
        &table_map(7, &basic(), &[3, 15, 3], &[100, 0]),
        &mut output,
    )
    .unwrap();
    feed(
        &mut resumed,
        WRITE_ROWS_EVENT,
        300,
        &rows(7, WRITE_ROWS_EVENT, 3, &[7], &[], &data),
        &mut output,
    )
    .unwrap();
    feed(
        &mut resumed,
        QUERY_EVENT,
        320,
        &query("ROLLBACK"),
        &mut output,
    )
    .unwrap();
    assert!(output.is_empty());
    assert_eq!(State::read(&path).unwrap().position, 320);
    let saved = std::fs::read(&path).unwrap();
    feed(&mut resumed, QUERY_EVENT, 340, &query("BEGIN"), &mut output).unwrap();
    feed(
        &mut resumed,
        TABLE_MAP_EVENT,
        360,
        &table_map(7, &basic(), &[3, 15, 3], &[100, 0]),
        &mut output,
    )
    .unwrap();
    assert!(matches!(
        feed(
            &mut resumed,
            DELETE_ROWS_EVENT,
            400,
            &rows(7, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 42, 0, 0, 0]),
            &mut output
        ),
        Err(Error::Unsupported(_))
    ));
    assert!(output.is_empty());
    assert_eq!(std::fs::read(&path).unwrap(), saved);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn retained_field_values_count_toward_budget_without_output_or_checkpoint_advance() {
    let path = temporary("field-budget");
    let mut snapshot = state();
    snapshot.tables[0].columns[1].octet_length = Some(4096);
    snapshot.save(&path).unwrap();
    let saved = fs::read(&path).unwrap();
    let mut processor = Processor::new(
        snapshot.clone(),
        Some(path.clone()),
        1024,
        mysql_canal::protocol::mysql::version::VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    )
    .unwrap();
    let mut output = Vec::new();
    feed(
        &mut processor,
        FORMAT_DESCRIPTION_EVENT,
        100,
        &format(),
        &mut output,
    )
    .unwrap();
    feed(
        &mut processor,
        QUERY_EVENT,
        150,
        &query("BEGIN"),
        &mut output,
    )
    .unwrap();
    feed(
        &mut processor,
        TABLE_MAP_EVENT,
        200,
        &table_map(7, &snapshot.tables[0], &[3, 15, 3], &[0, 16]),
        &mut output,
    )
    .unwrap();
    let mut data = vec![0];
    data.extend(42i32.to_le_bytes());
    data.extend(1536u16.to_le_bytes());
    data.extend(vec![b'x'; 1536]);
    let result = feed(
        &mut processor,
        WRITE_ROWS_EVENT,
        250,
        &rows(7, WRITE_ROWS_EVENT, 3, &[3], &[], &data),
        &mut output,
    );
    assert!(matches!(result, Err(Error::TransactionLimit(1024))));
    assert!(output.is_empty());
    assert_eq!(fs::read(&path).unwrap(), saved);
    fs::remove_file(path).unwrap();
}

#[test]
fn invalid_field_json_does_not_acknowledge_the_transaction() {
    let path = temporary("field-json-error");
    let mut snapshot = state();
    snapshot.tables[0].columns[2].data_type = "double".into();
    snapshot.tables[0].columns[2].column_type = "double".into();
    snapshot.save(&path).unwrap();
    let saved = fs::read(&path).unwrap();
    let mut processor = Processor::new(
        snapshot.clone(),
        Some(path.clone()),
        100_000,
        mysql_canal::protocol::mysql::version::VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    )
    .unwrap();
    let mut output = Vec::new();
    feed(
        &mut processor,
        FORMAT_DESCRIPTION_EVENT,
        100,
        &format(),
        &mut output,
    )
    .unwrap();
    feed(
        &mut processor,
        QUERY_EVENT,
        150,
        &query("BEGIN"),
        &mut output,
    )
    .unwrap();
    feed(
        &mut processor,
        TABLE_MAP_EVENT,
        200,
        &table_map(7, &snapshot.tables[0], &[3, 15, 5], &[100, 0, 8]),
        &mut output,
    )
    .unwrap();
    let mut data = vec![0];
    data.extend(42i32.to_le_bytes());
    data.extend(f64::NAN.to_le_bytes());
    feed(
        &mut processor,
        WRITE_ROWS_EVENT,
        250,
        &rows(7, WRITE_ROWS_EVENT, 3, &[5], &[], &data),
        &mut output,
    )
    .unwrap();
    assert!(output.is_empty());
    assert!(
        matches!(feed(&mut processor,XID_EVENT,300,&[0;8],&mut output), Err(Error::Json(error)) if error.to_string().contains("non-finite field value"))
    );
    assert_eq!(fs::read(&path).unwrap(), saved);
    fs::remove_file(path).unwrap();
}

#[test]
fn conflicting_optional_metadata_never_emits_or_advances_disk_checkpoint() {
    use mysql_canal::config::RowImageMode;
    use mysql_common::binlog::consts::OptionalMetadataFieldType as Tag;
    for mode in [RowImageMode::Minimal, RowImageMode::Full] {
        for (tag, data, message) in [
            (Tag::SIGNEDNESS, vec![0x80], "id signedness differs"),
            (
                Tag::SIMPLE_PRIMARY_KEY,
                vec![2],
                "id primary-key membership differs",
            ),
        ] {
            let path = temporary("metadata-conflict");
            let snapshot = state();
            snapshot.save(&path).unwrap();
            let saved = fs::read(&path).unwrap();
            let mut processor = Processor::new(
                snapshot,
                Some(path.clone()),
                100_000,
                mysql_canal::protocol::mysql::version::VersionRegistry::default(),
                None,
                mode,
            )
            .unwrap();
            let mut output = Vec::new();
            feed(
                &mut processor,
                FORMAT_DESCRIPTION_EVENT,
                100,
                &format(),
                &mut output,
            )
            .unwrap();
            feed(
                &mut processor,
                QUERY_EVENT,
                150,
                &query("BEGIN"),
                &mut output,
            )
            .unwrap();
            let mut body = table_map(7, &basic(), &[3, 15, 3], &[100, 0]);
            body.push(tag as u8);
            lenenc(&mut body, data.len());
            body.extend(data);
            assert!(
                matches!(feed(&mut processor, TABLE_MAP_EVENT, 200, &body, &mut output), Err(Error::Schema(value)) if value == message)
            );
            assert!(output.is_empty());
            assert_eq!(fs::read(&path).unwrap(), saved);
            fs::remove_file(path).unwrap();
        }
    }
}
