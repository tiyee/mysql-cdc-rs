#[path = "../support/mod.rs"]
mod common;
use common::*;
use mysql_canal::{
    Error, Operation, PrimaryKey,
    engine::{Processor, converter::EventConverter},
    protocol::mysql::version::{EventAction, VersionRegistry},
    sink::JsonLinesSink,
};
use mysql_replication::{BinlogParser, EventData, FrameDecoder};

#[test]
fn protocol_events_convert_to_exact_cdc_changes_without_copying_utf8_keys() {
    let mut table = basic();
    table.columns[0] = column("id", "varchar", true);
    table.columns[0].charset = Some("utf8mb4".into());
    table.columns[0].octet_length = Some(100);
    let mut framing = FrameDecoder::new();
    let mut parser = BinlogParser::<mysql_replication::Minimal>::default();
    let mut converter = EventConverter::new(
        vec![table.clone()],
        VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    );
    let fde = framed(FORMAT_DESCRIPTION_EVENT, 100, &format(), false);
    let event = parser.parse(framing.decode(&fde).unwrap()).unwrap();
    assert!(matches!(
        converter
            .convert_binlog(&event, &mut |_| panic!("FDE has no change"))
            .unwrap(),
        EventAction::Ignore
    ));
    let mapping = framed(
        TABLE_MAP_EVENT,
        150,
        &table_map(1, &table, &[15, 15, 3], &[100, 0, 100, 0]),
        false,
    );
    let event = parser.parse(framing.decode(&mapping).unwrap()).unwrap();
    converter
        .convert_binlog(&event, &mut |_| panic!("TABLE_MAP has no change"))
        .unwrap();
    let mut data = vec![0, 3];
    data.extend_from_slice(b"old");
    data.extend_from_slice(&[0, 3]);
    data.extend_from_slice(b"new");
    let raw = framed(
        UPDATE_ROWS_EVENT,
        200,
        &rows(1, UPDATE_ROWS_EVENT, 3, &[1], &[1], &data),
        false,
    );
    let event = parser.parse(framing.decode(&raw).unwrap()).unwrap();
    let EventData::Rows(protocol) = &event.event else {
        panic!("ROWS expected")
    };
    assert_eq!(protocol.table_id, 1);
    let expected = protocol.images().unwrap().rows;
    let mut changes = Vec::new();
    converter
        .convert_binlog(&event, &mut |change| {
            assert_eq!(change.operation, Operation::Update);
            let PrimaryKey::Text(ref key) = change.pk else {
                panic!("text key expected")
            };
            assert_eq!(key.as_ref(), "new");
            assert_eq!(key.as_ptr(), expected[7..].as_ptr());
            assert_eq!(change.previous_pk, Some(PrimaryKey::Text("old".into())));
            assert_eq!(change.changed.as_ref(), &[0]);
            changes.push(change.into_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].changed_names().collect::<Vec<_>>(), ["id"]);
}

#[test]
fn typed_binlog_input_is_delivered_only_after_cdc_commit() {
    let mut processor = Processor::new(
        state(),
        None,
        1024 * 1024,
        mysql_canal::protocol::mysql::version::VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    )
    .unwrap();
    let mut framing = FrameDecoder::new();
    let mut parser = BinlogParser::<mysql_replication::Minimal>::default();
    let mut output = Vec::new();
    let frames = [
        framed(FORMAT_DESCRIPTION_EVENT, 100, &format(), false),
        framed(QUERY_EVENT, 150, &query("BEGIN"), false),
        framed(
            TABLE_MAP_EVENT,
            200,
            &table_map(1, &basic(), &[3, 15, 3], &[100, 0]),
            false,
        ),
        framed(
            DELETE_ROWS_EVENT,
            250,
            &rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 42, 0, 0, 0]),
            false,
        ),
    ];
    for raw in frames {
        let event = parser.parse(framing.decode(&raw).unwrap()).unwrap();
        processor
            .process_binlog(&event, &JsonLinesSink::new(&mut output))
            .unwrap();
        assert!(output.is_empty());
    }
    let commit = framed(XID_EVENT, 300, &7u64.to_le_bytes(), false);
    let event = parser.parse(framing.decode(&commit).unwrap()).unwrap();
    processor
        .process_binlog(&event, &JsonLinesSink::new(&mut output))
        .unwrap();
    processor.finish().unwrap();
    let actual: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(
        actual,
        with_fields(
            serde_json::json!({"source":"test","schema":"app","table":"users","operation":"delete","pk":{"type":"signed","value":42},"changed":[]}),
            &basic(),
            Some(&[(0, serde_json::json!(42))]),
            None
        )
    );
    assert_eq!(processor.state().position, 300);
}

#[test]
fn ddl_is_a_valid_protocol_event_and_an_explicit_cdc_error() {
    let mut framing = FrameDecoder::new();
    let mut parser = BinlogParser::<mysql_replication::Minimal>::default();
    let mut converter = EventConverter::new(
        vec![basic()],
        VersionRegistry::default(),
        None,
        mysql_canal::config::RowImageMode::Minimal,
    );
    let fde = framed(FORMAT_DESCRIPTION_EVENT, 100, &format(), false);
    let event = parser.parse(framing.decode(&fde).unwrap()).unwrap();
    converter
        .convert_binlog(&event, &mut |_| panic!("no row expected"))
        .unwrap();
    let raw = framed(
        QUERY_EVENT,
        200,
        &query("ALTER TABLE app.users ADD note TEXT"),
        false,
    );
    let event = parser.parse(framing.decode(&raw).unwrap()).unwrap();
    let EventData::Query(protocol) = &event.event else {
        panic!("QUERY expected")
    };
    assert_eq!(protocol.query, b"ALTER TABLE app.users ADD note TEXT");
    assert!(
        matches!(converter.convert_binlog(&event,&mut |_|panic!("DDL cannot emit CDC changes")),Err(Error::Statement(statement)) if statement=="ALTER TABLE app.users ADD note TEXT")
    );
}

#[test]
fn full_converter_preserves_mode_across_fde_and_borrows_utf8_keys() {
    use mysql_canal::{config::RowImageMode, protocol::mysql::file::EventView};
    let mut table = basic();
    table.columns[0] = column("id", "varchar", true);
    table.columns[0].charset = Some("utf8mb4".into());
    table.columns[0].octet_length = Some(100);
    let mut converter = EventConverter::new(
        vec![table.clone()],
        VersionRegistry::default(),
        None,
        RowImageMode::Full,
    );
    for version in ["8.4.6", "5.7.44-log"] {
        let mut fde = format();
        fde[2..52].fill(0);
        fde[2..2 + version.len()].copy_from_slice(version.as_bytes());
        let view = |event_type, data| EventView {
            event_type,
            flags: 0,
            log_pos: 0,
            data,
        };
        converter
            .convert(view(FORMAT_DESCRIPTION_EVENT, &fde), &mut |_| {
                panic!("no change")
            })
            .unwrap();
        let mapping = table_map(7, &table, &[15, 15, 3], &[100, 0, 100, 0]);
        converter
            .convert(view(TABLE_MAP_EVENT, &mapping), &mut |_| {
                panic!("no change")
            })
            .unwrap();
        let mut data = vec![0, 3];
        data.extend(b"old");
        data.extend([1, b'x']);
        data.extend(20i32.to_le_bytes());
        data.extend([2, 3]);
        data.extend("新".as_bytes());
        data.extend(20i32.to_le_bytes());
        let body = rows(7, UPDATE_ROWS_EVENT, 3, &[7], &[7], &data);
        let mut emitted = 0;
        converter
            .convert(view(UPDATE_ROWS_EVENT, &body), &mut |change| {
                let PrimaryKey::Text(ref key) = change.pk else {
                    panic!("text key expected")
                };
                assert!(matches!(key, std::borrow::Cow::Borrowed(_)));
                assert_eq!(key.as_ref(), "新");
                assert_eq!(key.as_ptr(), body[body.len() - 7..].as_ptr());
                assert_eq!(change.previous_pk, Some(PrimaryKey::Text("old".into())));
                assert_eq!(change.changed.as_ref(), &[0, 1, 2]);
                emitted += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(emitted, 1);
    }
}
