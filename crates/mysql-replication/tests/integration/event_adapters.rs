#[path = "../support/mod.rs"]
mod common;
use common::*;
use mysql_replication::{
    Decoder, Error, Operation, PrimaryKey, Result,
    engine::Processor,
    protocol::mysql::{
        events::{
            EventParser, MysqlEventParser, ParsedEvent, RowsEvent, TableMapEvent, parse_row_images,
        },
        file::EventView,
        version::{FormatDescription, ServerVersion, VersionAdapter, VersionRegistry},
    },
};
use std::{borrow::Cow, sync::Arc};

fn versioned_format() -> Vec<u8> {
    let mut bytes = format();
    bytes[2..52].fill(0);
    bytes[2..7].copy_from_slice(b"9.1.0");
    bytes
}
fn raw(kind: u8, data: &[u8]) -> EventView<'_> {
    EventView {
        event_type: kind,
        flags: 0,
        log_pos: 0,
        data,
    }
}
fn feed(p: &mut Processor, kind: u8, position: u32, data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    p.process(
        EventView {
            event_type: kind,
            flags: 0,
            log_pos: position,
            data,
        },
        out,
    )
}
// Deliberately synthetic vendor format: not a claim about real MySQL 9.1.
struct CompactHeaders;
impl VersionAdapter for CompactHeaders {
    fn name(&self) -> &str {
        "compact-headers"
    }
    fn supports(&self, v: &ServerVersion) -> bool {
        v.major == 9 && v.minor == 1
    }
    fn status_query(&self) -> &str {
        "SHOW BINARY LOG STATUS"
    }
    fn validate_format(&self, f: &FormatDescription<'_>) -> Result<()> {
        MysqlEventParser::from_format(f).map(|_| ())
    }
}
#[test]
fn fde_compiles_different_id_width_and_control_post_headers_then_rebinds_on_rotation() {
    let mut registry = VersionRegistry::default();
    registry.register(Arc::new(CompactHeaders)).unwrap();
    let mut p = Processor::with_registry(state(), None, 1_000_000, registry, None).unwrap();
    let mut out = Vec::new();
    let mut fde = versioned_format();
    for (kind, len) in [
        (TABLE_MAP_EVENT, 6),
        (QUERY_EVENT, 15),
        (ROTATE_EVENT, 10),
        (WRITE_ROWS_EVENT, 8),
        (UPDATE_ROWS_EVENT, 8),
        (DELETE_ROWS_EVENT, 8),
    ] {
        fde[57 + usize::from(kind) - 1] = len;
    }
    feed(&mut p, FORMAT_DESCRIPTION_EVENT, 100, &fde, &mut out).unwrap();
    let mut begin = query("BEGIN");
    begin.splice(13..13, [0xcc, 0xdd]);
    feed(&mut p, QUERY_EVENT, 150, &begin, &mut out).unwrap();
    let mut map = table_map(7, &basic(), &[3, 15, 3], &[100, 0]);
    map.drain(4..6);
    feed(&mut p, TABLE_MAP_EVENT, 200, &map, &mut out).unwrap();
    let mut image = rows(
        7,
        UPDATE_ROWS_EVENT,
        3,
        &[1],
        &[2],
        &[0, 11, 0, 0, 0, 0, 1, b'x'],
    );
    image.drain(4..6);
    feed(&mut p, UPDATE_ROWS_EVENT, 250, &image, &mut out).unwrap();
    feed(&mut p, XID_EVENT, 300, &[0; 8], &mut out).unwrap();
    let mut rotate = 4u64.to_le_bytes().to_vec();
    rotate.extend([0xee, 0xff]);
    rotate.extend(b"binlog.000002");
    feed(&mut p, ROTATE_EVENT, 320, &rotate, &mut out).unwrap();
    assert_eq!(p.state().file, "binlog.000002");
    feed(&mut p, FORMAT_DESCRIPTION_EVENT, 100, &format(), &mut out).unwrap();
    feed(&mut p, QUERY_EVENT, 150, &query("BEGIN"), &mut out).unwrap();
    feed(
        &mut p,
        TABLE_MAP_EVENT,
        200,
        &table_map(7, &basic(), &[3, 15, 3], &[100, 0]),
        &mut out,
    )
    .unwrap();
    feed(
        &mut p,
        UPDATE_ROWS_EVENT,
        250,
        &rows(
            7,
            UPDATE_ROWS_EVENT,
            3,
            &[1],
            &[2],
            &[0, 22, 0, 0, 0, 0, 1, b'y'],
        ),
        &mut out,
    )
    .unwrap();
    feed(&mut p, XID_EVENT, 300, &[0; 8], &mut out).unwrap();
    p.finish().unwrap();
    let changes: Vec<serde_json::Value> = std::str::from_utf8(&out)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(changes[0]["pk"]["value"], 11);
    assert_eq!(changes[1]["pk"]["value"], 22);
    for c in changes {
        assert_eq!(c["changed"], serde_json::json!(["name"]));
    }
}

const VENDOR_ROWS_EVENT: u8 = 200;
struct ReorderedFields;
impl VersionAdapter for ReorderedFields {
    fn name(&self) -> &str {
        "reordered-fields"
    }
    fn supports(&self, v: &ServerVersion) -> bool {
        v.major == 9 && v.minor == 1
    }
    fn status_query(&self) -> &str {
        "SHOW BINARY LOG STATUS"
    }
    fn create_event_parser(&self, f: &FormatDescription<'_>) -> Result<Box<dyn EventParser>> {
        if f.post_header_lengths
            .get(usize::from(VENDOR_ROWS_EVENT) - 1)
            != Some(&7)
        {
            return Err(Error::Unsupported(
                "vendor ROWS header must be 7 bytes".into(),
            ));
        }
        Ok(Box::new(ReorderedParser {
            fallback: MysqlEventParser::from_format(f)?,
        }))
    }
}
struct ReorderedParser {
    fallback: MysqlEventParser,
}
impl EventParser for ReorderedParser {
    fn parse<'a>(&self, event: EventView<'a>) -> Result<ParsedEvent<'a>> {
        let data = event.data;
        match event.event_type {
            TABLE_MAP_EVENT => {
                let header: &[u8; 8] = data
                    .get(..8)
                    .and_then(|b| b.try_into().ok())
                    .ok_or_else(|| Error::Invalid("short vendor TABLE_MAP".into()))?;
                if header[2..4] != [0xcc, 0xdd] {
                    return Err(Error::Invalid("vendor TABLE_MAP marker".into()));
                }
                let flags = u16::from_be_bytes(header[..2].try_into().unwrap());
                let id = u64::from(u32::from_be_bytes(header[4..8].try_into().unwrap()));
                // Name and column metadata encoding is unchanged, so borrow it
                // directly from the original event rather than re-encoding.
                let schema_len = usize::from(
                    *data
                        .get(8)
                        .ok_or_else(|| Error::Invalid("schema length".into()))?,
                );
                let schema_end = 9 + schema_len;
                let schema = std::str::from_utf8(
                    data.get(9..schema_end)
                        .ok_or_else(|| Error::Invalid("schema".into()))?,
                )?;
                if data.get(schema_end) != Some(&0) {
                    return Err(Error::Invalid("schema terminator".into()));
                }
                let table_len = usize::from(
                    *data
                        .get(schema_end + 1)
                        .ok_or_else(|| Error::Invalid("table length".into()))?,
                );
                let table_start = schema_end + 2;
                let table_end = table_start + table_len;
                let table = std::str::from_utf8(
                    data.get(table_start..table_end)
                        .ok_or_else(|| Error::Invalid("table".into()))?,
                )?;
                if data.get(table_end) != Some(&0) {
                    return Err(Error::Invalid("table terminator".into()));
                }
                Ok(ParsedEvent::TableMap(TableMapEvent::new(
                    id,
                    flags,
                    schema,
                    table,
                    &data[table_end + 1..],
                )))
            }
            VENDOR_ROWS_EVENT => {
                let header: &[u8; 7] = data
                    .get(..7)
                    .and_then(|b| b.try_into().ok())
                    .ok_or_else(|| Error::Invalid("short vendor ROWS".into()))?;
                let operation = match header[0] {
                    0 => Operation::Insert,
                    1 => Operation::Update,
                    2 => Operation::Delete,
                    _ => return Err(Error::Invalid("vendor operation".into())),
                };
                let flags = u16::from_be_bytes(header[1..3].try_into().unwrap());
                let id = u64::from(u32::from_be_bytes(header[3..7].try_into().unwrap()));
                Ok(ParsedEvent::Rows(RowsEvent::new(
                    id,
                    flags,
                    operation,
                    &data[7..],
                    parse_row_images,
                )))
            }
            _ => self.fallback.parse(event),
        }
    }
}
#[test]
fn custom_parser_normalizes_field_order_endianness_and_new_event_code_without_copying_key() {
    let mut key = column("id", "varchar", true);
    key.charset = Some("utf8mb4".into());
    key.octet_length = Some(100);
    let mut snapshot = state();
    snapshot.tables[0].columns = vec![key];
    let mut fde = versioned_format();
    fde.resize(57 + usize::from(VENDOR_ROWS_EVENT), 0);
    fde[57 + usize::from(VENDOR_ROWS_EVENT) - 1] = 7;
    let format = FormatDescription::parse(&fde).unwrap();
    let parser = ReorderedFields.create_event_parser(&format).unwrap();
    let canonical_map = table_map(7, &snapshot.tables[0], &[15], &[100, 0]);
    let mut map = vec![0, 0, 0xcc, 0xdd, 0, 0, 0, 7];
    map.extend(&canonical_map[8..]);
    let mut row = vec![0, 0, 0, 0, 0, 0, 7];
    row.extend([1, 1, 0, 5, b'h', b'e', b'l', b'l', b'o']);
    let mut decoder = Decoder::new(snapshot.tables.clone()).unwrap();
    let ParsedEvent::TableMap(view) = parser.parse(raw(TABLE_MAP_EVENT, &map)).unwrap() else {
        panic!("TABLE_MAP")
    };
    decoder.table_map_event(view).unwrap();
    let ParsedEvent::Rows(view) = parser.parse(raw(VENDOR_ROWS_EVENT, &row)).unwrap() else {
        panic!("ROWS")
    };
    let mut seen = false;
    decoder
        .rows_event(view, |change| {
            let PrimaryKey::Text(Cow::Borrowed(value)) = change.pk else {
                panic!("borrowed UTF-8 key required")
            };
            assert_eq!(value, "hello");
            assert_eq!(value.as_ptr(), row[row.len() - 5..].as_ptr());
            seen = true;
            Ok(())
        })
        .unwrap();
    assert!(seen);
    // The same parser works through the transaction engine, not just a direct
    // decoder unit call. No processor branch knows this vendor event code.
    let mut registry = VersionRegistry::default();
    registry.register(Arc::new(ReorderedFields)).unwrap();
    let mut p = Processor::with_registry(snapshot, None, 1_000_000, registry, None).unwrap();
    let mut out = Vec::new();
    for (kind, position, data) in [
        (FORMAT_DESCRIPTION_EVENT, 100, fde.as_slice()),
        (QUERY_EVENT, 150, query("BEGIN").as_slice()),
        (TABLE_MAP_EVENT, 200, map.as_slice()),
        (VENDOR_ROWS_EVENT, 250, row.as_slice()),
        (XID_EVENT, 300, &[0; 8]),
    ] {
        feed(&mut p, kind, position, data, &mut out).unwrap();
    }
    p.finish().unwrap();
    let change: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(change["pk"]["value"], "hello");
    assert_eq!(change["changed"], serde_json::json!(["id"]));
}
#[test]
fn fde_controls_legacy_support_and_v2_extra_data_is_lazy_but_checked_for_whitelist() {
    let fde = format();
    let f = FormatDescription::parse(&fde).unwrap();
    let parser = MysqlEventParser::from_format(&f).unwrap();
    assert!(parser.parse(raw(WRITE_ROWS_EVENT_V1, &[])).is_err());
    let mut decoder = Decoder::new(vec![basic()]).unwrap();
    let mut excluded = basic();
    excluded.table = "excluded".into();
    let excluded_map = table_map(7, &excluded, &[3, 15, 3], &[100, 0]);
    let ParsedEvent::TableMap(map) = parser.parse(raw(TABLE_MAP_EVENT, &excluded_map)).unwrap()
    else {
        panic!("map");
    };
    decoder.table_map_event(map).unwrap();
    let mut bad = 7u64.to_le_bytes()[..6].to_vec();
    bad.extend([0, 0, 1, 0]); // invalid v2 extra length=1
    let ParsedEvent::Rows(row) = parser.parse(raw(WRITE_ROWS_EVENT, &bad)).unwrap() else {
        panic!("rows")
    };
    decoder
        .rows_event(row, |_| panic!("excluded row decoded"))
        .unwrap();
    let bytes = table_map(7, &basic(), &[3, 15, 3], &[100, 0]);
    let ParsedEvent::TableMap(map) = parser.parse(raw(TABLE_MAP_EVENT, &bytes)).unwrap() else {
        panic!("map")
    };
    decoder.table_map_event(map).unwrap();
    let ParsedEvent::Rows(row) = parser.parse(raw(WRITE_ROWS_EVENT, &bad)).unwrap() else {
        panic!("rows")
    };
    assert!(decoder.rows_event(row, |_| Ok(())).is_err());
    let mut legacy = fde;
    for kind in [
        WRITE_ROWS_EVENT_V1,
        UPDATE_ROWS_EVENT_V1,
        DELETE_ROWS_EVENT_V1,
    ] {
        legacy[57 + usize::from(kind) - 1] = 8;
    }
    let f = FormatDescription::parse(&legacy).unwrap();
    let parser = MysqlEventParser::from_format(&f).unwrap();
    let body = rows(7, DELETE_ROWS_EVENT_V1, 3, &[1], &[], &[0, 11, 0, 0, 0]);
    let ParsedEvent::Rows(row) = parser.parse(raw(DELETE_ROWS_EVENT_V1, &body)).unwrap() else {
        panic!("legacy rows")
    };
    let mut seen = false;
    decoder
        .rows_event(row, |change| {
            assert_eq!(change.pk, PrimaryKey::Signed(11));
            seen = true;
            Ok(())
        })
        .unwrap();
    assert!(seen);
}
#[test]
fn structural_parser_rejects_truncation_and_incompatible_fde_lengths() {
    let fde = format();
    let format = FormatDescription::parse(&fde).unwrap();
    let parser = MysqlEventParser::from_format(&format).unwrap();
    let body = table_map(7, &basic(), &[3, 15, 3], &[100, 0]);
    // Envelope parsing deliberately leaves column metadata to the whitelist
    // compiler; verify every truncated envelope prefix, not valid envelopes.
    let envelope = 8 + 1 + 3 + 1 + 1 + 5 + 1;
    for len in 0..envelope {
        assert!(parser.parse(raw(TABLE_MAP_EVENT, &body[..len])).is_err());
    }
    for kind in [QUERY_EVENT, ROTATE_EVENT, UPDATE_ROWS_EVENT] {
        for len in 0..8 {
            assert!(parser.parse(raw(kind, &[0; 8][..len])).is_err());
        }
    }
    for (kind, len) in [
        (TABLE_MAP_EVENT, 7),
        (UPDATE_ROWS_EVENT, 9),
        (QUERY_EVENT, 12),
        (ROTATE_EVENT, 7),
    ] {
        let mut bad = fde.clone();
        bad[57 + usize::from(kind) - 1] = len;
        assert!(MysqlEventParser::from_format(&FormatDescription::parse(&bad).unwrap()).is_err());
    }
}
