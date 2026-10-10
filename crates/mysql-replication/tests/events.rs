use mysql_replication::{
    BinlogParser, Error, Event, EventData, EventHeader, FrameDecoder,
    event_type::*,
    events::{
        ColumnType, CompleteImage, Full, GtidEvent, ImageMode, Minimal, PreviousGtidsEvent,
        QueryEvent, RowValue, RowsDecoder, RowsEvent, RowsKind, SparseImage, TableMapEvent,
        TaggedGtidEvent, TypedRow, TypedRowsDecoder, TypedRowsEvent, XidEvent,
    },
    file::BinlogFile,
    parser::{ParserConfig, RowsDecode},
};
use std::io::{self, Write};

fn lenenc(value: usize, out: &mut Vec<u8>) {
    if value < 251 {
        out.push(value as u8);
    } else {
        out.push(0xfc);
        out.extend_from_slice(&(value as u16).to_le_bytes());
    }
}

fn table_body(id: u64, types: &[u8], metadata: &[u8], nullable: &[u8]) -> Vec<u8> {
    let mut body = id.to_le_bytes()[..6].to_vec();
    body.extend_from_slice(&[0, 0, 3]);
    body.extend_from_slice(b"app\0");
    body.push(1);
    body.extend_from_slice(b"t\0");
    lenenc(types.len(), &mut body);
    body.extend_from_slice(types);
    lenenc(metadata.len(), &mut body);
    body.extend_from_slice(metadata);
    body.extend_from_slice(nullable);
    body
}

fn rows_decoder(types: &[u8], metadata: &[u8], nullable: &[u8]) -> RowsDecoder {
    let body = table_body(1, types, metadata, nullable);
    let mut table = TableMapEvent::default();
    table.decode(&body).unwrap();
    RowsDecoder::new(&table).unwrap()
}

fn rows_body(count: usize, bitmaps: &[u8], values: &[u8], v2: bool) -> Vec<u8> {
    let mut body = 1u64.to_le_bytes()[..6].to_vec();
    body.extend_from_slice(&[0, 0]);
    if v2 {
        body.extend_from_slice(&[4, 0, 0xaa, 0xbb]);
    }
    lenenc(count, &mut body);
    body.extend_from_slice(bitmaps);
    body.extend_from_slice(values);
    body
}

fn rows_event(body: &[u8], kind: RowsKind, v2: bool) -> RowsEvent<'_> {
    let mut event = RowsEvent::with_layout(kind, if v2 { 10 } else { 8 }, v2);
    event.decode(body).unwrap();
    event
}

fn format(version: &str) -> Vec<u8> {
    let mut body = vec![0; 57 + 43];
    body[..2].copy_from_slice(&4u16.to_le_bytes());
    body[2..2 + version.len()].copy_from_slice(version.as_bytes());
    body[56] = 19;
    for (kind, length) in [
        (QUERY_EVENT, 13),
        (ROTATE_EVENT, 8),
        (TABLE_MAP_EVENT, 8),
        (WRITE_ROWS_EVENT, 10),
        (UPDATE_ROWS_EVENT, 10),
        (DELETE_ROWS_EVENT, 10),
        (WRITE_ROWS_EVENT_V1, 8),
        (UPDATE_ROWS_EVENT_V1, 8),
        (DELETE_ROWS_EVENT_V1, 8),
    ] {
        body[57 + usize::from(kind) - 1] = length;
    }
    body
}
fn frame(kind: u8, body: &[u8], crc: bool, flags: u16) -> Vec<u8> {
    let mut raw = vec![0; 19];
    raw[..4].copy_from_slice(&1234567890u32.to_le_bytes());
    raw[4] = kind;
    raw[5..9].copy_from_slice(&456u32.to_le_bytes());
    raw[13..17].copy_from_slice(&987u32.to_le_bytes());
    raw[17..19].copy_from_slice(&flags.to_le_bytes());
    raw.extend_from_slice(body);
    if kind == FORMAT_DESCRIPTION_EVENT {
        raw.push(u8::from(crc));
    }
    let trailer = if crc || kind == FORMAT_DESCRIPTION_EVENT {
        4
    } else {
        0
    };
    let length = (raw.len() + trailer) as u32;
    raw[9..13].copy_from_slice(&length.to_le_bytes());
    if crc {
        let mut original = raw.clone();
        if kind == FORMAT_DESCRIPTION_EVENT {
            original[17] &= !1;
        }
        raw.extend_from_slice(&crc32fast::hash(&original).to_le_bytes());
    } else if trailer != 0 {
        raw.extend_from_slice(&[9, 8, 7, 6]);
    }
    raw
}
fn file_bytes(frames: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = vec![0xfe, b'b', b'i', b'n'];
    for frame in frames {
        bytes.extend_from_slice(frame);
    }
    bytes
}
#[test]
fn complete_header_implements_the_same_object_safe_interface() {
    let raw = frame(199, &[], false, 0x8081);
    let mut header = EventHeader::default();
    let event: &mut dyn Event<'_> = &mut header;
    event.decode(&raw[..19]).unwrap();
    assert_eq!(
        header,
        EventHeader {
            timestamp: 1234567890,
            event_type: 199,
            server_id: 456,
            event_size: 19,
            log_pos: 987,
            flags: 0x8081
        }
    );
    let mut dump = Vec::new();
    header.dump(&mut dump).unwrap();
    assert!(String::from_utf8(dump).unwrap().contains("1234567890"));
    assert!(matches!(
        header.decode(&raw[..18]),
        Err(Error::Truncated { .. })
    ));
}
#[test]
fn offline_bytes_headers_and_trailers_are_exact_for_crc_and_none() {
    for crc in [false, true] {
        let raw = frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), crc, 1);
        let next = frame(XID_EVENT, &123u64.to_le_bytes(), crc, 0x8000);
        let bytes = file_bytes(&[raw.clone(), next.clone()]);
        let mut reader = BinlogFile::new(&bytes[..]).unwrap();
        let event = reader.next_event().unwrap().unwrap();
        assert_eq!(event.raw_data, raw);
        assert_eq!(event.header.flags, 1);
        let EventData::FormatDescription(description) = event.event else {
            panic!("FDE expected")
        };
        assert_eq!(description.server_version, "8.4.6");
        assert_eq!(description.post_header_lengths.len(), 43);
        let event = reader.next_event().unwrap().unwrap();
        assert_eq!(event.raw_data, next);
        let EventData::Xid(xid) = event.event else {
            panic!("XID expected")
        };
        assert_eq!(xid.xid, 123);
        assert!(reader.next_event().unwrap().is_none());
    }
}
#[test]
fn checksum_errors_do_not_install_a_format() {
    let mut raw = frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), true, 0);
    raw[30] ^= 1;
    let mut decoder = FrameDecoder::new();
    assert!(matches!(decoder.decode(&raw), Err(Error::Checksum)));
    assert!(!decoder.has_format());
    let xid = frame(XID_EVENT, &1u64.to_le_bytes(), false, 0);
    assert!(matches!(decoder.decode(&xid), Err(Error::Invalid(_))));
}
#[test]
fn query_retains_ddl_and_non_utf8_bytes_without_cdc_policy() {
    let mut body = vec![0; 13];
    body[8] = 3;
    body[11..13].copy_from_slice(&2u16.to_le_bytes());
    body.extend_from_slice(&[0xaa, 0xbb]);
    body.extend_from_slice(b"app\0ALTER TABLE app.t ADD c BLOB /*\xff*/");
    let mut event = QueryEvent::default();
    event.decode(&body).unwrap();
    assert_eq!(event.schema, b"app");
    assert_eq!(event.status_vars, &[0xaa, 0xbb]);
    assert_eq!(event.query, b"ALTER TABLE app.t ADD c BLOB /*\xff*/");
    assert_eq!(event.query.as_ptr(), body[19..].as_ptr());
}
#[test]
fn gtid_optional_metadata_distinguishes_absent_zero_and_encoded_originals() {
    let mut body = vec![1];
    body.extend_from_slice(&[0xab; 16]);
    body.extend_from_slice(&42u64.to_le_bytes());
    let mut event = GtidEvent::default();
    event.decode(&body).unwrap();
    assert_eq!(event.gno, 42);
    assert_eq!(event.last_committed, None);
    body.push(2);
    body.extend_from_slice(&0u64.to_le_bytes());
    body.extend_from_slice(&7u64.to_le_bytes());
    event.decode(&body).unwrap();
    assert_eq!(event.last_committed, Some(0));
    assert_eq!(event.immediate_commit_timestamp, None);
    body.extend_from_slice(&((1u64 << 55) | 123).to_le_bytes()[..7]);
    body.extend_from_slice(&100u64.to_le_bytes()[..7]);
    body.extend_from_slice(&[0xfc, 0x34, 0x12]);
    body.extend_from_slice(&((1u32 << 31) | 80406).to_le_bytes());
    body.extend_from_slice(&80014u32.to_le_bytes());
    event.decode(&body).unwrap();
    assert_eq!(event.immediate_commit_timestamp, Some(123));
    assert_eq!(event.original_commit_timestamp, Some(100));
    assert_eq!(event.transaction_length, Some(0x1234));
    assert_eq!(event.immediate_server_version, Some(80406));
    assert_eq!(event.original_server_version, Some(80014));
    for length in [0, 24, 26, 41, 43, 48, 55, 57, 60, 64, 68] {
        if length < body.len() {
            assert!(
                matches!(
                    event.decode(&body[..length]),
                    Err(Error::Truncated { .. }) | Err(Error::Io(_))
                ),
                "length={length}"
            );
        }
    }
}
#[test]
fn tagged_gtid_uses_a_distinct_envelope_and_borrows_its_tag() {
    // All integers fit the one-byte serialization form: value << 1.
    let mut fields = vec![0, 2, 2];
    fields.extend_from_slice(&[2; 16]);
    fields.extend_from_slice(&[4, 28, 6, 6, b't', b'a', b'g', 8, 0, 10, 32, 12, 100]);
    let mut body = vec![2, ((fields.len() + 3) * 2) as u8, 12];
    body.extend_from_slice(&fields);
    let mut event = TaggedGtidEvent::default();
    event.decode(&body).unwrap();
    assert_eq!(event.gtid.flags, 1);
    assert_eq!(event.gtid.sid, [1; 16]);
    assert_eq!(event.gtid.gno, 7);
    assert_eq!(event.tag, "tag");
    assert_eq!(event.gtid.last_committed, Some(0));
    assert_eq!(event.gtid.sequence_number, Some(8));
    assert_eq!(event.gtid.original_commit_timestamp, Some(50));
    let start = event.tag.as_ptr() as usize - body.as_ptr() as usize;
    assert_eq!(&body[start..start + 3], b"tag");
    let mut bad = body.clone();
    bad[0] = 3;
    assert!(matches!(event.decode(&bad), Err(Error::Unsupported(_))));
    let truncated = &body[..body.len() - 1];
    assert!(matches!(event.decode(truncated), Err(Error::Invalid(_))));
}
#[test]
fn previous_gtids_validates_and_borrows_intervals() {
    let mut body = 1u64.to_le_bytes().to_vec();
    body.extend_from_slice(&[1; 16]);
    body.extend_from_slice(&1u64.to_le_bytes());
    body.extend_from_slice(&4u64.to_le_bytes());
    body.extend_from_slice(&9u64.to_le_bytes());
    let mut event = PreviousGtidsEvent::default();
    event.decode(&body).unwrap();
    let sid = event.sids().next().unwrap().unwrap();
    assert_eq!(sid.sid, &[1; 16]);
    assert_eq!(sid.intervals, &body[32..]);
    let mut invalid = body.clone();
    invalid[40..48].copy_from_slice(&4u64.to_le_bytes());
    assert!(matches!(event.decode(&invalid), Err(Error::Invalid(_))));
}
#[test]
fn rows_defer_extra_data_and_sparse_maps_until_requested() {
    let mut body = 1u64.to_le_bytes()[..6].to_vec();
    body.extend_from_slice(&[0, 0, 1, 0]);
    let mut rows = RowsEvent::with_layout(RowsKind::Update, 10, true);
    rows.decode(&body).unwrap();
    assert_eq!(rows.table_id, 1);
    assert!(matches!(rows.images(), Err(Error::Invalid(_))));
    let fde = frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), false, 0);
    let raw = frame(UPDATE_ROWS_EVENT, &body, false, 0);
    let bytes = file_bytes(&[fde, raw]);
    let mut lazy = BinlogFile::with_config(&bytes[..], ParserConfig::minimal()).unwrap();
    lazy.next_event().unwrap();
    assert!(matches!(
        lazy.next_event().unwrap().unwrap().event,
        EventData::Rows(_)
    ));
    let mut eager = BinlogFile::with_config(
        &bytes[..],
        ParserConfig {
            rows: RowsDecode::ValidateImages,
            ..Default::default()
        },
    )
    .unwrap();
    eager.next_event().unwrap();
    assert!(matches!(eager.next_event(), Err(Error::Invalid(_))));
}
#[test]
fn sparse_rows_over_64_columns_and_full_images_remain_borrowed() {
    let mut body = 9u64.to_le_bytes()[..6].to_vec();
    body.extend_from_slice(&[0, 0, 2, 0, 80]);
    let mut bitmap = [0; 10];
    bitmap[8] = 1 << 6;
    body.extend_from_slice(&bitmap);
    body.extend_from_slice(&bitmap);
    body.extend_from_slice(&[0, 42, 0, 0, 0, 1]);
    let mut rows = RowsEvent::with_layout(RowsKind::Update, 10, true);
    rows.decode(&body).unwrap();
    let images = rows.images().unwrap();
    assert_eq!(images.column_count, 80);
    assert_eq!(images.after_columns, bitmap);
    assert_eq!(images.rows.as_ptr(), body[31..].as_ptr());
    let mut full = body.clone();
    full[11..31].fill(0xff);
    rows.decode(&full).unwrap();
    assert_eq!(rows.images().unwrap().after_columns, &[0xff; 10]);
}
#[test]
fn minimal_config_keeps_unused_gtid_metadata_raw_and_unknown_codes_survive() {
    let fde = frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), false, 0);
    let gtid = frame(GTID_EVENT, &[1], false, 0);
    let unknown = frame(199, &[1, 2, 3], false, 0x8080);
    let bytes = file_bytes(&[fde, gtid, unknown.clone()]);
    let mut reader = BinlogFile::with_config(&bytes[..], ParserConfig::minimal()).unwrap();
    reader.next_event().unwrap();
    assert!(matches!(
        reader.next_event().unwrap().unwrap().event,
        EventData::Raw(_)
    ));
    let event = reader.next_event().unwrap().unwrap();
    assert_eq!(event.header.event_type, 199);
    assert_eq!(event.raw_data, unknown);
    let EventData::Raw(raw) = event.event else {
        panic!("raw expected")
    };
    assert_eq!(raw.data, &[1, 2, 3]);
}
#[test]
fn owned_events_outlive_the_reader_without_self_references() {
    let owned = {
        let bytes = file_bytes(&[frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), true, 0)]);
        BinlogFile::new(&bytes[..])
            .unwrap()
            .next_event()
            .unwrap()
            .unwrap()
            .into_owned()
            .unwrap()
    };
    let event = owned.view().unwrap();
    assert_eq!(event.raw_data, owned.raw_data());
    let EventData::FormatDescription(format) = event.event else {
        panic!("FDE expected")
    };
    assert_eq!(format.server_version, "8.4.6");
}
#[test]
fn producer_version_requires_complete_numbers_and_rejects_mariadb() {
    for version in [
        "8.4",
        "8.4.",
        "8.4.invalid",
        "8x.4.6",
        "8.4x.6",
        "8.4.4294967296",
        "8.4.6.1",
        "8.4.6-",
        "8.4.6 ",
    ] {
        let raw = frame(FORMAT_DESCRIPTION_EVENT, &format(version), false, 0);
        let mut framing = FrameDecoder::new();
        assert!(
            matches!(
                BinlogParser::default().parse(framing.decode(&raw).unwrap()),
                Err(Error::Invalid(_))
            ),
            "malformed producer {version} must fail"
        );
    }
    for version in ["5.7.44-MariaDB", "8.4.6-mArIaDb", "9.7.2-MariaDB"] {
        let raw = frame(FORMAT_DESCRIPTION_EVENT, &format(version), false, 0);
        let mut framing = FrameDecoder::new();
        assert!(
            matches!(BinlogParser::default().parse(framing.decode(&raw).unwrap()), Err(Error::Unsupported(message)) if message.contains(version)),
            "unsupported producer {version} must fail"
        );
    }
    for version in [
        "5.7.44-log",
        "8.0.45-0ubuntu0.24.04.1",
        "8.4.6-commercial",
        "9.7.2+build.1",
    ] {
        let raw = frame(FORMAT_DESCRIPTION_EVENT, &format(version), false, 0);
        let mut framing = FrameDecoder::new();
        let event = BinlogParser::default()
            .parse(framing.decode(&raw).unwrap())
            .unwrap();
        let EventData::FormatDescription(description) = event.event else {
            panic!("FDE expected")
        };
        assert_eq!(description.server_version, version);
    }
}

#[test]
fn framing_and_layout_failures_are_explicit() {
    let fde = frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), false, 0);
    let mut truncated = file_bytes(std::slice::from_ref(&fde));
    truncated.extend_from_slice(&[0; 18]);
    let mut reader = BinlogFile::new(&truncated[..]).unwrap();
    reader.next_event().unwrap();
    assert!(
        matches!(reader.next_event(),Err(Error::Io(e)) if e.kind()==io::ErrorKind::UnexpectedEof)
    );
    let unknown = frame(FORMAT_DESCRIPTION_EVENT, &format("10.0.1"), false, 0);
    let mut framing = FrameDecoder::new();
    let raw = framing.decode(&unknown).unwrap();
    assert!(matches!(
        BinlogParser::default().parse(raw),
        Err(Error::Unsupported(_))
    ));
    let mut bad = fde;
    bad[9..13].copy_from_slice(&18u32.to_le_bytes());
    assert!(matches!(framing.decode(&bad), Err(Error::Invalid(_))));
}
#[test]
fn dump_propagates_writer_errors_and_xid_rejects_trailing_bytes() {
    struct Fails;
    impl Write for Fails {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("sink refused"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut xid = XidEvent::default();
    xid.decode(&42u64.to_le_bytes()).unwrap();
    assert!(matches!(xid.dump(&mut Fails),Err(Error::Io(e)) if e.to_string()=="sink refused"));
    assert!(matches!(xid.decode(&[0; 9]), Err(Error::Invalid(_))));
    assert_eq!(xid.xid, 42);
}

#[test]
fn each_fde_reselects_layout_and_unknown_changes_are_rejected() {
    let mut framing = FrameDecoder::new();
    let mut parser = BinlogParser::default();
    for version in ["5.7.44", "8.0.45", "8.4.6", "9.7.2"] {
        let raw = frame(FORMAT_DESCRIPTION_EVENT, &format(version), false, 0);
        let event = parser.parse(framing.decode(&raw).unwrap()).unwrap();
        let EventData::FormatDescription(description) = event.event else {
            panic!("FDE expected")
        };
        assert_eq!(description.server_version, version);
    }
    let mut changed = format("8.4.6");
    changed[57 + usize::from(TABLE_MAP_EVENT) - 1] = 6;
    let raw = frame(FORMAT_DESCRIPTION_EVENT, &changed, false, 0);
    assert!(
        matches!(parser.parse(framing.decode(&raw).unwrap()),Err(Error::Unsupported(message)) if message.contains("post-header"))
    );
}

#[test]
fn format_dump_reports_the_actual_algorithm_and_survives_ownership() {
    for (crc, algorithm) in [(false, "NONE"), (true, "CRC32")] {
        let owned = {
            let raw = frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), crc, 0);
            let mut framing = FrameDecoder::new();
            BinlogParser::default()
                .parse(framing.decode(&raw).unwrap())
                .unwrap()
                .into_owned()
                .unwrap()
        };
        let event = owned.view().unwrap();
        let mut output = Vec::new();
        let diagnostic: &dyn Event<'_> = &event.event;
        diagnostic.dump(&mut output).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            format!("Version: 4\nServer version: 8.4.6\nChecksum algorithm: {algorithm}\n\n")
        );
    }
}

#[test]
fn decoded_rows_have_explicit_images_for_all_operations_and_layouts() {
    let decoder = rows_decoder(&[3], &[], &[0]);
    for v2 in [false, true] {
        for kind in [RowsKind::Write, RowsKind::Update, RowsKind::Delete] {
            let bitmaps: &[u8] = if kind == RowsKind::Update {
                &[1, 1]
            } else {
                &[1]
            };
            let values = &[0xfe, 1, 0, 0, 0, 0xfe, 2, 0, 0, 0];
            let body = rows_body(1, bitmaps, values, v2);
            let event = rows_event(&body, kind, v2);
            assert_eq!(
                event.images().unwrap().extra_data,
                if v2 { &[0xaa, 0xbb][..] } else { &[] }
            );
            let rows = event
                .rows(&decoder)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), if kind == RowsKind::Update { 1 } else { 2 });
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row.before.is_some(), kind != RowsKind::Write);
                assert_eq!(row.after.is_some(), kind != RowsKind::Delete);
                for (image, expected) in [
                    (row.before.as_ref(), i as u64 + 1),
                    (
                        row.after.as_ref(),
                        if kind == RowsKind::Update {
                            2
                        } else {
                            i as u64 + 1
                        },
                    ),
                ] {
                    if let Some(image) = image {
                        assert_eq!(image.column_indices(), &[0]);
                        let fields = image.fields().collect::<Result<Vec<_>, _>>().unwrap();
                        assert_eq!(fields.len(), 1);
                        assert_eq!(fields[0].column_index, 0);
                        let RowValue::Integer(value) = fields[0].value else {
                            panic!("integer expected")
                        };
                        assert_eq!(value.unsigned(), expected);
                    }
                }
            }
        }
    }
}

#[test]
fn decoded_wide_sparse_rows_borrow_values_and_share_column_indices() {
    let mut types = vec![3; 80];
    types[79] = 15;
    let mut nullable = vec![0; 10];
    nullable[9] = 1;
    let mut bitmap = vec![0; 10];
    for index in [3, 65, 72, 79] {
        bitmap[index / 8] |= 1 << (index % 8);
    }
    let mut values = Vec::new();
    for key in [7u32, 8] {
        values.push(0xf4); // Compact NULL bit 2 is absolute column 72; padding is ignored.
        values.extend_from_slice(&key.to_le_bytes());
        values.extend_from_slice(&u32::MAX.to_le_bytes());
        values.push(3);
        values.extend_from_slice("中".as_bytes());
    }
    let body = rows_body(80, &bitmap, &values, true);
    let event = rows_event(&body, RowsKind::Write, true);
    let mut rows = {
        let decoder = rows_decoder(&types, &[30, 0], &nullable);
        event.rows(&decoder).unwrap()
    };
    let first = rows.next().unwrap().unwrap().after.unwrap();
    let second = rows.next().unwrap().unwrap().after.unwrap();
    assert!(rows.next().is_none());
    assert_eq!(first.column_indices(), &[3, 65, 72, 79]);
    assert_eq!(
        first.column_indices().as_ptr(),
        second.column_indices().as_ptr()
    );
    for (image, key) in [(&first, 7), (&second, 8)] {
        let fields = image.fields().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            fields.iter().map(|f| f.column_index).collect::<Vec<_>>(),
            [3, 65, 72, 79]
        );
        let RowValue::Integer(value) = fields[0].value else {
            panic!("integer expected")
        };
        assert_eq!(value.unsigned(), key);
        let RowValue::Integer(value) = fields[1].value else {
            panic!("integer expected")
        };
        assert_eq!(value.unsigned(), u64::from(u32::MAX));
        assert_eq!(value.signed(), -1);
        assert_eq!(fields[2].value, RowValue::Null);
        let RowValue::Bytes(bytes) = fields[3].value else {
            panic!("bytes expected")
        };
        assert_eq!(bytes, "中".as_bytes());
        assert!(
            bytes.as_ptr() >= body.as_ptr()
                && bytes.as_ptr() < body.as_ptr().wrapping_add(body.len())
        );
    }
}

#[test]
fn decoded_integer_widths_preserve_signed_and_unsigned_boundaries() {
    for (ty, width) in [(1, 1), (2, 2), (9, 3), (3, 4), (8, 8)] {
        let decoder = rows_decoder(&[ty], &[], &[0]);
        for minimum in [false, true] {
            let mut bytes = vec![if minimum { 0 } else { 0xff }; width];
            if minimum {
                bytes[width - 1] = 0x80;
            }
            let mut values = vec![0];
            values.extend_from_slice(&bytes);
            let body = rows_body(1, &[1], &values, false);
            let row = rows_event(&body, RowsKind::Write, false)
                .rows(&decoder)
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            let image = row.after.unwrap();
            let field = image.fields().next().unwrap().unwrap();
            let RowValue::Integer(value) = field.value else {
                panic!("integer expected")
            };
            assert_eq!(value.raw_bytes(), bytes);
            assert_eq!(
                value.signed(),
                if minimum {
                    i64::MIN >> (64 - width * 8)
                } else {
                    -1
                }
            );
            assert_eq!(
                value.unsigned(),
                if minimum {
                    1u64 << (width * 8 - 1)
                } else {
                    u64::MAX >> (64 - width * 8)
                }
            );
        }
    }
}

#[test]
fn decoded_decimals_are_exact_and_preserve_scale() {
    let mut large = Vec::new();
    for _ in 0..7 {
        large.extend_from_slice(&999_999_999u32.to_be_bytes());
    }
    // DECIMAL(65,30): 35 integral digits (8+9+9+9), then 30 fractional (9+9+9+3).
    large[..4].copy_from_slice(&99_999_999u32.to_be_bytes());
    large.extend_from_slice(&999u16.to_be_bytes());
    large[0] ^= 0x80;
    let cases = [
        (5, 2, vec![0x80, 0x0c, 0x1e], "12.30".to_owned()),
        (5, 2, vec![0x7f, 0xf3, 0xe1], "-12.30".to_owned()),
        (4, 4, vec![0x80, 0x2a], "0.0042".to_owned()),
        (3, 0, vec![0x80, 0x00], "0".to_owned()),
        (3, 0, vec![0x7f, 0xff], "0".to_owned()),
        (
            65,
            30,
            large,
            format!("{}.{}", "9".repeat(35), "9".repeat(30)),
        ),
    ];
    for (precision, scale, bytes, expected) in cases {
        let decoder = rows_decoder(&[246], &[precision, scale], &[0]);
        let mut values = vec![0];
        values.extend_from_slice(&bytes);
        let body = rows_body(1, &[1], &values, true);
        let image = rows_event(&body, RowsKind::Write, true)
            .rows(&decoder)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .after
            .unwrap();
        let RowValue::Decimal(value) = image.fields().next().unwrap().unwrap().value else {
            panic!("decimal expected")
        };
        assert_eq!(value.to_string(), expected);
        assert_eq!(value.precision(), precision);
        assert_eq!(value.scale(), scale);
        assert_eq!(value.raw_bytes(), bytes);
    }
    let decoder = rows_decoder(&[246], &[2, 0], &[0]);
    let body = rows_body(1, &[1], &[0, 0xff], false);
    let mut rows = rows_event(&body, RowsKind::Write, false)
        .rows(&decoder)
        .unwrap();
    assert!(
        matches!(rows.next(), Some(Err(Error::Invalid(message))) if message == "invalid packed DECIMAL digit group")
    );
    assert!(rows.next().is_none());
}

#[test]
fn decoded_values_cover_variable_lengths_and_opaque_protocol_types() {
    // FLOAT, DOUBLE, VARCHAR(300), CHAR(500), ENUM, SET, BLOB, BIT(10), YEAR,
    // TIMESTAMP2(6), JSON, GEOMETRY and VECTOR.
    let types = [4, 5, 15, 254, 254, 254, 252, 16, 13, 17, 245, 255, 242];
    let metadata = [4, 8, 44, 1, 0xee, 244, 247, 2, 248, 8, 2, 2, 1, 6, 4, 1, 4];
    let decoder = rows_decoder(&types, &metadata, &[0, 0]);
    assert_eq!(
        decoder.columns()[4].wire_type(),
        ColumnType::MYSQL_TYPE_STRING
    );
    assert_eq!(
        decoder.columns()[4].column_type(),
        ColumnType::MYSQL_TYPE_ENUM
    );
    assert_eq!(decoder.columns()[9].metadata(), &[6]);
    assert!(!decoder.columns()[0].nullable());
    let mut values = vec![0, 0];
    values.extend_from_slice(&1.5f32.to_le_bytes());
    values.extend_from_slice(&(-2.25f64).to_le_bytes());
    values.extend_from_slice(&[3, 0, b'a', b'b', b'c']);
    values.extend_from_slice(&[2, 0, 0xff, 0]);
    values.extend_from_slice(&513u16.to_le_bytes());
    values.extend_from_slice(&u64::MAX.to_le_bytes());
    values.extend_from_slice(&[2, 0, 0, 0xff]);
    values.extend_from_slice(&[3, 0xff, 125]);
    values.extend_from_slice(&[0; 7]);
    values.extend_from_slice(&[1, 0, 0, 0, 4]);
    values.extend_from_slice(&[2, 0xaa, 0xbb]);
    values.extend_from_slice(&[4, 0, 0, 0, 0, 0, 0x80, 0x3f]);
    let body = rows_body(13, &[0xff, 0x1f], &values, true);
    let image = rows_event(&body, RowsKind::Write, true)
        .rows(&decoder)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .after
        .unwrap();
    let fields = image.fields().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        fields.iter().map(|f| f.column_index).collect::<Vec<_>>(),
        (0..13).collect::<Vec<_>>()
    );
    let expected = [
        RowValue::Float(1.5),
        RowValue::Double(-2.25),
        RowValue::Bytes(b"abc"),
        RowValue::Bytes(&[0xff, 0]),
        RowValue::Enum(513),
        RowValue::Set(u64::MAX),
        RowValue::Bytes(&[0, 0xff]),
        RowValue::Bit(1023),
        RowValue::Year(2025),
        RowValue::Encoded(&[0; 7]),
        RowValue::Encoded(&[4]),
        RowValue::Encoded(&[0xaa, 0xbb]),
        RowValue::Encoded(&[0, 0, 0x80, 0x3f]),
    ];
    assert_eq!(fields.iter().map(|f| f.value).collect::<Vec<_>>(), expected);
}

#[test]
fn decoded_update_does_not_emit_a_partial_pair_and_iterators_stop_on_error() {
    let decoder = rows_decoder(&[3, 15], &[10, 0], &[2]);
    let values = [0, 1, 0, 0, 0, 0, 3, b'a', b'b', b'c'];
    for length in 0..values.len() {
        let body = rows_body(2, &[1, 2], &values[..length], true);
        let event = rows_event(&body, RowsKind::Update, true);
        if length == 0 {
            assert!(
                matches!(event.rows(&decoder), Err(Error::Invalid(message)) if message == "ROWS event contains no rows")
            );
        } else {
            let mut rows = event.rows(&decoder).unwrap();
            assert!(
                matches!(rows.next(), Some(Err(Error::Truncated { .. }))),
                "length={length}"
            );
            assert!(rows.next().is_none());
            assert!(rows.next().is_none());
        }
    }
    let body = rows_body(2, &[1, 2], &values, true);
    let row = rows_event(&body, RowsKind::Update, true)
        .rows(&decoder)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(row.before.unwrap().column_indices(), &[0]);
    let image = row.after.unwrap();
    assert_eq!(image.column_indices(), &[1]);
    assert_eq!(
        image.fields().next().unwrap().unwrap().value,
        RowValue::Bytes(b"abc")
    );
}

#[test]
fn decoded_rows_reject_mismatches_empty_images_invalid_lengths_and_nulls() {
    let decoder = rows_decoder(&[3, 15], &[3, 0], &[2]);
    let body = rows_body(2, &[1], &[0, 1, 0, 0, 0], false);
    let mut event = rows_event(&body, RowsKind::Write, false);
    event.table_id = 2;
    assert!(
        matches!(event.rows(&decoder), Err(Error::Invalid(message)) if message == "ROWS table ID differs from TABLE_MAP")
    );
    let body = rows_body(1, &[1], &[0], false);
    assert!(
        matches!(rows_event(&body, RowsKind::Write, false).rows(&decoder), Err(Error::Invalid(message)) if message == "ROWS column count differs from TABLE_MAP")
    );
    for (kind, maps) in [
        (RowsKind::Write, vec![0]),
        (RowsKind::Delete, vec![0]),
        (RowsKind::Update, vec![1, 0]),
    ] {
        let body = rows_body(2, &maps, &[0], false);
        assert!(
            matches!(rows_event(&body, kind, false).rows(&decoder), Err(Error::Invalid(message)) if message == "empty row image")
        );
    }
    for (maps, values, expected) in [
        (vec![1], vec![1], "NULL in a NOT NULL column"),
        (
            vec![2],
            vec![0, 4, b'a', b'b', b'c', b'd'],
            "column length exceeds TABLE_MAP maximum",
        ),
    ] {
        let body = rows_body(2, &maps, &values, false);
        let mut rows = rows_event(&body, RowsKind::Write, false)
            .rows(&decoder)
            .unwrap();
        assert!(matches!(rows.next(), Some(Err(Error::Invalid(message))) if message == expected));
        assert!(rows.next().is_none());
    }
    // A sparse INSERT is legal and does not need a primary key in the raw library.
    let body = rows_body(2, &[2], &[0xff], false);
    let image = rows_event(&body, RowsKind::Write, false)
        .rows(&decoder)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .after
        .unwrap();
    assert_eq!(image.column_indices(), &[1]);
    assert_eq!(
        image.fields().next().unwrap().unwrap().value,
        RowValue::Null
    );
}

#[test]
fn rows_decoder_rejects_invalid_or_unsupported_column_metadata() {
    for (types, metadata) in [
        (vec![4], vec![8]),
        (vec![5], vec![4]),
        (vec![246], vec![66, 2]),
        (vec![246], vec![2, 3]),
        (vec![246], vec![65, 31]),
        (vec![16], vec![8, 0]),
        (vec![16], vec![1, 8]),
        (vec![17], vec![7]),
        (vec![254], vec![247, 3]),
        (vec![254], vec![248, 9]),
        (vec![254], vec![1, 1]),
        (vec![252], vec![0]),
        (vec![245], vec![3]),
        (vec![3], vec![1]),
    ] {
        let body = table_body(1, &types, &metadata, &[0]);
        let mut table = TableMapEvent::default();
        table.decode(&body).unwrap();
        assert!(
            matches!(RowsDecoder::new(&table), Err(Error::Invalid(_))),
            "types={types:?}, metadata={metadata:?}"
        );
    }
    for ty in [0, 20, 243, 244] {
        let body = table_body(1, &[ty], &[], &[0]);
        let mut table = TableMapEvent::default();
        table.decode(&body).unwrap();
        assert!(
            matches!(RowsDecoder::new(&table), Err(Error::Unsupported(message)) if message == format!("wire column type {ty}"))
        );
    }
    let body = table_body(1, &[15], &[10], &[0]);
    let mut table = TableMapEvent::default();
    table.decode(&body).unwrap();
    assert!(matches!(
        RowsDecoder::new(&table),
        Err(Error::Truncated { .. })
    ));
    let mut body = table_body(1, &[3], &[], &[0]);
    body.extend_from_slice(&[99, 2, 1]);
    table.decode(&body).unwrap();
    assert!(matches!(
        RowsDecoder::new(&table),
        Err(Error::Truncated { .. })
    ));
}

#[test]
fn row_decoder_remaps_are_explicit_and_owned_views_decode_without_copying_values() {
    let map = table_body(1, &[15], &[10, 0], &[0]);
    let rows = rows_body(1, &[1], &[0, 3, b'a', b'b', b'c'], true);
    let bytes = file_bytes(&[
        frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), true, 0),
        frame(TABLE_MAP_EVENT, &map, true, 0),
        frame(WRITE_ROWS_EVENT, &rows, true, 0),
    ]);
    let (owned, decoder) = {
        let mut reader = BinlogFile::new(&bytes[..]).unwrap();
        reader.next_event().unwrap();
        let event = reader.next_event().unwrap().unwrap();
        let EventData::TableMap(table) = event.event else {
            panic!("TABLE_MAP expected")
        };
        let decoder = RowsDecoder::new(&table).unwrap();
        (
            reader.next_event().unwrap().unwrap().into_owned().unwrap(),
            decoder,
        )
    };
    let view = owned.view().unwrap();
    let EventData::Rows(rows) = view.event else {
        panic!("ROWS expected")
    };
    let image = rows
        .rows(&decoder)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .after
        .unwrap();
    let RowValue::Bytes(data) = image.fields().next().unwrap().unwrap().value else {
        panic!("bytes expected")
    };
    assert_eq!(data, b"abc");
    let offset = owned
        .raw_data()
        .windows(3)
        .position(|w| w == b"abc")
        .unwrap();
    assert_eq!(data.as_ptr(), owned.raw_data()[offset..].as_ptr());
    for image in [
        rows.with_mode::<Minimal>(&decoder)
            .unwrap()
            .rows()
            .next()
            .unwrap()
            .unwrap()
            .after
            .unwrap()
            .as_ref()
            .clone(),
        rows.with_mode::<Full>(&decoder)
            .unwrap()
            .rows()
            .next()
            .unwrap()
            .unwrap()
            .after
            .unwrap()
            .as_ref()
            .clone(),
    ] {
        assert_eq!(image.column_count(), 1);
        let RowValue::Bytes(text) = image.fields().next().unwrap().unwrap().value else {
            panic!("bytes expected")
        };
        assert_eq!(text, b"abc");
        assert_eq!(text.as_ptr(), data.as_ptr());
    }
    let remapped = rows_decoder(&[3], &[], &[0]);
    let body = rows_body(1, &[1], &[0, 42, 0, 0, 0], false);
    let image = rows_event(&body, RowsKind::Write, false)
        .rows(&remapped)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .after
        .unwrap();
    let RowValue::Integer(value) = image.fields().next().unwrap().unwrap().value else {
        panic!("integer expected")
    };
    assert_eq!(value.signed(), 42);
    assert_eq!(
        decoder.columns()[0].column_type(),
        ColumnType::MYSQL_TYPE_VARCHAR
    );
    assert_eq!(
        remapped.columns()[0].column_type(),
        ColumnType::MYSQL_TYPE_LONG
    );
}

#[test]
fn decoded_updates_pair_multiple_rows_and_keep_absent_columns_distinct_from_null() {
    let decoder = rows_decoder(&[3, 15, 3], &[10, 0], &[2]);
    // Before contains key + counter, after contains only nullable text.
    let values = [
        0xfc, 1, 0, 0, 0, 10, 0, 0, 0, 0xfe, 1, b'a', 0xfc, 2, 0, 0, 0, 20, 0, 0, 0, 0xff,
    ];
    let body = rows_body(3, &[5, 2], &values, true);
    let rows = rows_event(&body, RowsKind::Update, true)
        .rows(&decoder)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 2);
    for (i, row) in rows.iter().enumerate() {
        let before = row.before.as_ref().unwrap();
        assert_eq!(before.column_indices(), &[0, 2]);
        let fields = before.fields().collect::<Result<Vec<_>, _>>().unwrap();
        let RowValue::Integer(key) = fields[0].value else {
            panic!("integer expected")
        };
        let RowValue::Integer(counter) = fields[1].value else {
            panic!("integer expected")
        };
        assert_eq!(key.unsigned(), i as u64 + 1);
        assert_eq!(counter.unsigned(), (i as u64 + 1) * 10);
        let after = row.after.as_ref().unwrap();
        assert_eq!(after.column_indices(), &[1]);
        let fields = after.fields().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields[0].value,
            if i == 0 {
                RowValue::Bytes(b"a")
            } else {
                RowValue::Null
            }
        );
    }
}

#[test]
fn decoded_null_bitmap_crosses_bytes_using_present_column_ordinals() {
    let mut nullable = vec![0; 10];
    nullable[9] = 8; // Absolute column 75 is compact present-column 9.
    let decoder = rows_decoder(&[1; 80], &[], &nullable);
    let mut bitmap = vec![0; 10];
    for index in [0, 2, 8, 16, 24, 32, 40, 48, 64, 75] {
        bitmap[index / 8] |= 1 << (index % 8);
    }
    let values = [0, 0xfe, 10, 11, 12, 13, 14, 15, 16, 17];
    // The first NULL byte covers eight values, the second marks column 75 NULL.
    let mut complete = values.to_vec();
    complete.push(18);
    let body = rows_body(80, &bitmap, &complete, false);
    let image = rows_event(&body, RowsKind::Write, false)
        .rows(&decoder)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .after
        .unwrap();
    let fields = image.fields().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(fields.len(), 10);
    for (i, field) in fields[..9].iter().enumerate() {
        let RowValue::Integer(value) = field.value else {
            panic!("integer expected")
        };
        assert_eq!(value.unsigned(), i as u64 + 10);
    }
    assert_eq!(fields[9].column_index, 75);
    assert_eq!(fields[9].value, RowValue::Null);
}

#[test]
fn full_rows_decode_v1_v2_insert_update_delete_and_borrow_text() {
    let decoder = rows_decoder(&[3, 15], &[10, 0], &[2]);
    for v2 in [false, true] {
        for kind in [RowsKind::Write, RowsKind::Update, RowsKind::Delete] {
            let mut values = vec![0];
            values.extend(42i32.to_le_bytes());
            values.extend([3, b'o', b'l', b'd']);
            if kind == RowsKind::Update {
                values.push(2);
                values.extend(43i32.to_le_bytes());
            }
            let bitmaps: &[u8] = if kind == RowsKind::Update {
                &[3, 3]
            } else {
                &[3]
            };
            let body = rows_body(2, bitmaps, &values, v2);
            let event = rows_event(&body, kind, v2);
            let rows = event
                .rows(&decoder)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(row.before.is_some(), kind != RowsKind::Write);
            assert_eq!(row.after.is_some(), kind != RowsKind::Delete);
            for (after, image) in [(false, &row.before), (true, &row.after)] {
                let Some(image) = image else { continue };
                assert_eq!(image.column_indices(), &[0, 1]);
                let fields = image.fields().collect::<Result<Vec<_>, _>>().unwrap();
                let RowValue::Integer(key) = fields[0].value else {
                    panic!("integer expected")
                };
                assert_eq!(
                    key.signed(),
                    if after && kind == RowsKind::Update {
                        43
                    } else {
                        42
                    }
                );
                if after && kind == RowsKind::Update {
                    assert_eq!(fields[1].value, RowValue::Null);
                } else {
                    let RowValue::Bytes(text) = fields[1].value else {
                        panic!("text expected")
                    };
                    assert_eq!(text, b"old");
                    assert_eq!(
                        text.as_ptr(),
                        body[body.len() - values.len() + 6..].as_ptr()
                    );
                }
            }
        }
    }
}

#[test]
fn typed_full_events_preserve_v1_v2_operations_and_borrowed_values() {
    let decoder = TypedRowsDecoder::<Full>::from(rows_decoder(&[3, 15], &[10, 0], &[2]));
    assert_eq!(decoder.table_id(), 1);
    assert_eq!(decoder.columns().len(), 2);
    for v2 in [false, true] {
        for kind in [RowsKind::Write, RowsKind::Update, RowsKind::Delete] {
            let mut values = vec![0];
            values.extend(42i32.to_le_bytes());
            values.extend([3, b'o', b'l', b'd']);
            if kind == RowsKind::Update {
                values.push(2);
                values.extend(43i32.to_le_bytes());
            }
            let bitmaps: &[u8] = if kind == RowsKind::Update {
                &[3, 3]
            } else {
                &[3]
            };
            let body = rows_body(2, bitmaps, &values, v2);
            let event = rows_event(&body, kind, v2);
            let typed: TypedRowsEvent<'_, Full> = decoder.event(&event).unwrap();
            assert_eq!(typed.table_id(), 1);
            assert_eq!(typed.column_count(), 2);
            assert_eq!(typed.kind(), kind);
            assert_eq!(typed.flags(), event.flags);
            let mut rows = typed.rows();
            let row: TypedRow<'_, Full> = rows.next().unwrap().unwrap();
            assert!(rows.next().is_none());
            assert!(rows.next().is_none());
            assert_eq!(row.before.is_some(), kind != RowsKind::Write);
            assert_eq!(row.after.is_some(), kind != RowsKind::Delete);
            for (after, image) in [(false, row.before), (true, row.after)] {
                let Some(image): Option<CompleteImage<'_>> = image else {
                    continue;
                };
                assert_eq!(image.column_count(), 2);
                assert_eq!(image.fields().count(), 2);
                let RowValue::Integer(value) = image.field(0).unwrap() else {
                    panic!("integer expected")
                };
                assert_eq!(
                    value.signed(),
                    if after && kind == RowsKind::Update {
                        43
                    } else {
                        42
                    }
                );
                if after && kind == RowsKind::Update {
                    assert_eq!(image.field(1).unwrap(), RowValue::Null);
                } else {
                    let RowValue::Bytes(text) = image.field(1).unwrap() else {
                        panic!("bytes expected")
                    };
                    assert_eq!(text, b"old");
                    assert_eq!(
                        text.as_ptr(),
                        body[body.len() - values.len() + 6..].as_ptr()
                    );
                }
                assert!(matches!(image.field(2), Err(Error::Invalid(message))
                    if message == "column index outside complete row image"));
            }
        }
    }
}

#[test]
fn typed_minimal_images_distinguish_absent_null_and_present_columns() {
    let decoder = rows_decoder(&[3, 15, 3], &[10, 0], &[2]);
    let body = rows_body(3, &[5, 2], &[0, 1, 0, 0, 0, 10, 0, 0, 0, 1], true);
    let event = rows_event(&body, RowsKind::Update, true);
    let typed = event.with_mode::<Minimal>(&decoder).unwrap();
    let row: TypedRow<'_, Minimal> = typed.rows().next().unwrap().unwrap();
    let before: SparseImage<'_> = row.before.unwrap();
    let after: SparseImage<'_> = row.after.unwrap();
    assert_eq!(before.column_count(), 3);
    assert_eq!(before.column_indices(), &[0, 2]);
    assert_eq!(before.field(1).unwrap(), None);
    let Some(RowValue::Integer(value)) = before.field(2).unwrap() else {
        panic!("integer expected")
    };
    assert_eq!(value.signed(), 10);
    assert_eq!(after.column_count(), 3);
    assert_eq!(after.column_indices(), &[1]);
    assert_eq!(after.field(0).unwrap(), None);
    assert_eq!(after.field(1).unwrap(), Some(RowValue::Null));
    assert_eq!(after.field(2).unwrap(), None);
    assert_eq!(after.field(3).unwrap(), None);
    assert!(matches!(CompleteImage::try_from(before.as_ref().clone()),
        Err(Error::Unsupported(message)) if message == "FULL row images must contain every table column"));

    // A complete bitmap is valid in MINIMAL too; bytes cannot prove the producer setting.
    let body = rows_body(3, &[7], &[2, 1, 0, 0, 0, 10, 0, 0, 0], false);
    let event = rows_event(&body, RowsKind::Write, false);
    let row = event
        .with_mode::<Minimal>(&decoder)
        .unwrap()
        .rows()
        .next()
        .unwrap()
        .unwrap();
    let image = CompleteImage::try_from(row.after.unwrap().as_ref().clone()).unwrap();
    assert_eq!(image.column_count(), 3);
    assert_eq!(image.field(1).unwrap(), RowValue::Null);
}

#[test]
fn typed_full_rejects_sparse_shapes_before_iteration_including_sparse_prefixes() {
    let decoder = rows_decoder(&[1, 1, 1], &[], &[0]);
    for v2 in [false, true] {
        for (kind, bitmaps) in [
            (RowsKind::Write, &[3][..]),
            (RowsKind::Delete, &[3][..]),
            (RowsKind::Update, &[3, 7][..]),
            (RowsKind::Update, &[7, 3][..]),
        ] {
            // Shape validation precedes value validation, even for truncated values.
            let body = rows_body(3, bitmaps, &[0], v2);
            let event = rows_event(&body, kind, v2);
            assert!(matches!(event.with_mode::<Full>(&decoder),
                Err(Error::Unsupported(message)) if message == "FULL row images must contain every table column"));
        }
    }
    let body = rows_body(3, &[3], &[0, 5, 6], true);
    let event = rows_event(&body, RowsKind::Write, true);
    let row = event.rows(&decoder).unwrap().next().unwrap().unwrap();
    let image = row.after.unwrap();
    assert_eq!(image.column_indices(), &[0, 1]);
    assert_eq!(image.column_count(), 3);
    assert!(matches!(
        CompleteImage::try_from(image),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn typed_full_wide_images_keep_shared_plans_after_decoder_is_dropped() {
    let mut nullable = [0; 10];
    nullable[9] = 8;
    let mut values = Vec::new();
    for offset in [0, 80] {
        let mut nulls = [0; 10];
        nulls[9] = 8;
        values.extend(nulls);
        for column in 0..80u8 {
            if column != 75 {
                values.push(column + offset);
            }
        }
    }
    let body = rows_body(80, &[255; 10], &values, true);
    let event = rows_event(&body, RowsKind::Write, true);
    let typed = {
        let map_body = table_body(1, &[1; 80], &[], &nullable);
        let mut map = TableMapEvent::default();
        map.decode(&map_body).unwrap();
        let decoder = TypedRowsDecoder::<Full>::new(&map).unwrap();
        decoder.event(&event).unwrap()
    };
    let rows = typed.rows().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(rows.len(), 2);
    let first = rows[0].after.as_ref().unwrap();
    let second = rows[1].after.as_ref().unwrap();
    assert_eq!(
        first.as_ref().column_indices().as_ptr(),
        second.as_ref().column_indices().as_ptr()
    );
    for (offset, image) in [(0, first), (80, second)] {
        assert_eq!(image.column_count(), 80);
        assert_eq!(image.field(75).unwrap(), RowValue::Null);
        let RowValue::Integer(value) = image.field(79).unwrap() else {
            panic!("integer expected")
        };
        assert_eq!(value.unsigned(), 79 + offset);
        assert_eq!(image.fields().count(), 80);
    }
}

#[test]
fn typed_decoding_preserves_errors_and_never_yields_partial_updates() {
    let decoder = TypedRowsDecoder::<Full>::from(rows_decoder(&[3], &[], &[0]));
    let body = rows_body(1, &[1, 1], &[0, 1, 0, 0, 0, 0, 2], true);
    let event = rows_event(&body, RowsKind::Update, true);
    let mut rows = decoder.rows(&event).unwrap();
    assert!(matches!(
        rows.next(),
        Some(Err(Error::Truncated { need: 4, have: 1 }))
    ));
    assert!(rows.next().is_none());
    assert!(rows.next().is_none());

    let body = rows_body(1, &[1], &[1], false);
    let event = rows_event(&body, RowsKind::Write, false);
    let mut rows = decoder.rows(&event).unwrap();
    assert!(matches!(rows.next(), Some(Err(Error::Invalid(message)))
        if message == "NULL in a NOT NULL column"));
    assert!(rows.next().is_none());

    let body = rows_body(2, &[3], &[0, 1, 2], true);
    let event = rows_event(&body, RowsKind::Write, true);
    assert!(matches!(decoder.event(&event), Err(Error::Invalid(message))
        if message == "ROWS column count differs from TABLE_MAP"));
    let body = rows_body(1, &[1], &[0, 1, 0, 0, 0], true);
    let mut event = rows_event(&body, RowsKind::Write, true);
    event.table_id = 2;
    assert!(matches!(decoder.rows(&event), Err(Error::Invalid(message))
        if message == "ROWS table ID differs from TABLE_MAP"));
}

#[test]
fn typed_minimal_v1_v2_wide_multirow_operations_share_plans_and_borrow_text() {
    let mut types = [1; 80];
    types[75] = 15;
    let mut nullable = [0; 10];
    nullable[9] = 8;
    let decoder = TypedRowsDecoder::<Minimal>::from(rows_decoder(&types, &[20, 0], &nullable));
    let mut before = [0; 10];
    before[8] = 64;
    let mut after = [0; 10];
    after[9] = 8;
    for v2 in [false, true] {
        for kind in [RowsKind::Write, RowsKind::Update, RowsKind::Delete] {
            let (bitmaps, values) = match kind {
                RowsKind::Write => {
                    let mut bitmap = after;
                    bitmap[0] = 1;
                    (
                        bitmap.to_vec(),
                        [vec![0, 10, 3], "中".as_bytes().to_vec(), vec![2, 11]].concat(),
                    )
                }
                RowsKind::Update => (
                    [before.as_slice(), after.as_slice()].concat(),
                    [vec![0, 20, 0, 3], "中".as_bytes().to_vec(), vec![0, 21, 1]].concat(),
                ),
                RowsKind::Delete => (before.to_vec(), vec![0, 20, 0, 21]),
            };
            let body = rows_body(80, &bitmaps, &values, v2);
            let event = rows_event(&body, kind, v2);
            let rows = decoder
                .rows(&event)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(rows.len(), 2);
            for (index, row) in rows.iter().enumerate() {
                assert_eq!(row.before.is_some(), kind != RowsKind::Write);
                assert_eq!(row.after.is_some(), kind != RowsKind::Delete);
                if let Some(image) = &row.before {
                    assert_eq!(image.column_count(), 80);
                    assert_eq!(image.column_indices(), &[70]);
                    assert_eq!(image.field(0).unwrap(), None);
                    let Some(RowValue::Integer(value)) = image.field(70).unwrap() else {
                        panic!("integer expected")
                    };
                    assert_eq!(value.unsigned(), 20 + index as u64);
                }
                if let Some(image) = &row.after {
                    assert_eq!(image.column_count(), 80);
                    assert_eq!(
                        image.column_indices(),
                        if kind == RowsKind::Write {
                            &[0, 75][..]
                        } else {
                            &[75][..]
                        }
                    );
                    assert_eq!(image.field(70).unwrap(), None);
                    if kind == RowsKind::Write {
                        let Some(RowValue::Integer(value)) = image.field(0).unwrap() else {
                            panic!("integer expected")
                        };
                        assert_eq!(value.unsigned(), 10 + index as u64);
                    }
                    if index == 0 {
                        let Some(RowValue::Bytes(text)) = image.field(75).unwrap() else {
                            panic!("bytes expected")
                        };
                        assert_eq!(text, "中".as_bytes());
                        let offset = body
                            .windows(text.len())
                            .position(|value| value == text)
                            .unwrap();
                        assert_eq!(text.as_ptr(), body[offset..].as_ptr());
                    } else {
                        assert_eq!(image.field(75).unwrap(), Some(RowValue::Null));
                    }
                }
            }
            for (first, second) in [
                (&rows[0].before, &rows[1].before),
                (&rows[0].after, &rows[1].after),
            ] {
                if let (Some(first), Some(second)) = (first, second) {
                    assert_eq!(
                        first.column_indices().as_ptr(),
                        second.column_indices().as_ptr()
                    );
                }
            }
        }
    }
}

fn typed_late_error<M: ImageMode>() {
    let decoder = TypedRowsDecoder::<M>::from(rows_decoder(&[3], &[], &[0]));
    for v2 in [false, true] {
        let values = [0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 0, 3, 0, 0, 0, 0, 4];
        let body = rows_body(1, &[1, 1], &values, v2);
        let event = rows_event(&body, RowsKind::Update, v2);
        let mut rows = decoder.rows(&event).unwrap();
        let row = rows.next().unwrap().unwrap();
        for (image, expected) in [(row.before, 1), (row.after, 2)] {
            let image = image.unwrap();
            let field = image.as_ref().fields().next().unwrap().unwrap();
            let RowValue::Integer(value) = field.value else {
                panic!("integer expected")
            };
            assert_eq!(value.unsigned(), expected);
        }
        assert!(matches!(
            rows.next(),
            Some(Err(Error::Truncated { need: 4, have: 1 }))
        ));
        assert!(rows.next().is_none());
        assert!(rows.next().is_none());
    }
}

#[test]
fn typed_modes_return_valid_rows_before_one_late_error_and_then_stop() {
    typed_late_error::<Minimal>();
    typed_late_error::<Full>();
}
