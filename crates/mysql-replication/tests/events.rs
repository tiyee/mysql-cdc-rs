use mysql_replication::{
    BinlogParser, Error, Event, EventData, EventHeader, FrameDecoder,
    event_type::*,
    events::{
        GtidEvent, PreviousGtidsEvent, QueryEvent, RowsEvent, RowsKind, TaggedGtidEvent, XidEvent,
    },
    file::BinlogFile,
    parser::{ParserConfig, RowsDecode},
};
use std::io::{self, Write};

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
