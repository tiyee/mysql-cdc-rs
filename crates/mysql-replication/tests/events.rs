use mysql_replication::{
    BinlogParser, Error, Event, EventData, EventHeader, FrameDecoder,
    event_type::*,
    events::{
        BinlogMode, ColumnType, CompleteImage, Full, GtidEvent, Minimal, PreviousGtidsEvent,
        QueryEvent, Row, RowImage, RowValue, RowsDecoder, RowsEvent, RowsKind, SparseImage,
        TableMapEvent, TaggedGtidEvent, XidEvent,
    },
    file::BinlogFile,
    parser::ParserConfig,
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

fn rows_decoder(types: &[u8], metadata: &[u8], nullable: &[u8]) -> RowsDecoder<Minimal> {
    let body = table_body(1, types, metadata, nullable);
    let mut table = TableMapEvent::default();
    table.decode(&body).unwrap();
    RowsDecoder::<Minimal>::new(&table).unwrap()
}

fn rows_decoder_for<M: BinlogMode>(
    types: &[u8],
    metadata: &[u8],
    nullable: &[u8],
) -> RowsDecoder<M> {
    let body = table_body(1, types, metadata, nullable);
    let mut table = TableMapEvent::default();
    table.decode(&body).unwrap();
    RowsDecoder::<M>::new(&table).unwrap()
}
fn rows_event_for<'a, M: BinlogMode>(body: &'a [u8], kind: RowsKind, v2: bool) -> RowsEvent<'a, M> {
    let mut event = RowsEvent::<M>::with_layout(kind, if v2 { 10 } else { 8 }, v2);
    event.decode(body).unwrap();
    event
}
struct ImagePair<'a> {
    before: Option<RowImage<'a>>,
    after: Option<RowImage<'a>>,
}
trait WireRowsFixture<'a> {
    fn raw_rows(
        &self,
        decoder: &RowsDecoder<Minimal>,
    ) -> mysql_replication::Result<
        Box<dyn Iterator<Item = mysql_replication::Result<ImagePair<'a>>> + 'a>,
    >;
}
impl<'a> WireRowsFixture<'a> for RowsEvent<'a, Minimal> {
    fn raw_rows(
        &self,
        decoder: &RowsDecoder<Minimal>,
    ) -> mysql_replication::Result<
        Box<dyn Iterator<Item = mysql_replication::Result<ImagePair<'a>>> + 'a>,
    > {
        Ok(Box::new(decoder.rows(self)?.map(|row| {
            let (before, after) = row?.operation.into_images();
            Ok(ImagePair {
                before: before.map(|image| image.as_ref().clone()),
                after: after.map(|image| image.as_ref().clone()),
            })
        })))
    }
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

fn rows_event(body: &[u8], kind: RowsKind, v2: bool) -> RowsEvent<'_, Minimal> {
    let mut event = RowsEvent::<Minimal>::with_layout(kind, if v2 { 10 } else { 8 }, v2);
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
        let mut reader =
            BinlogFile::<_, Minimal>::new(&bytes[..], ParserConfig::default()).unwrap();
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
    let mut rows = RowsEvent::<Minimal>::with_layout(RowsKind::Update, 10, true);
    rows.decode(&body).unwrap();
    assert_eq!(rows.table_id, 1);
    assert!(matches!(rows.images(), Err(Error::Invalid(_))));
    let fde = frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), false, 0);
    let raw = frame(UPDATE_ROWS_EVENT, &body, false, 0);
    let bytes = file_bytes(&[fde, raw]);
    let mut frames = mysql_replication::file::FrameReader::new(&bytes[..]).unwrap();
    frames.next_frame().unwrap().unwrap();
    assert_eq!(
        frames.next_frame().unwrap().unwrap().header.event_type,
        UPDATE_ROWS_EVENT
    );
    let mut typed = BinlogFile::<_, Minimal>::new(&bytes[..], ParserConfig::default()).unwrap();
    typed.next_event().unwrap().unwrap();
    assert!(
        matches!(typed.next_event(), Err(Error::Invalid(message)) if message == "missing TABLE_MAP for 1")
    );
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
    let mut rows = RowsEvent::<Minimal>::with_layout(RowsKind::Update, 10, true);
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
    let mut reader = BinlogFile::<_, Minimal>::new(
        &bytes[..],
        ParserConfig {
            decode_gtids: false,
            decode_previous_gtids: false,
        },
    )
    .unwrap();
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
        BinlogFile::<_, Minimal>::new(&bytes[..], ParserConfig::default())
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
                BinlogParser::<Minimal>::default().parse(framing.decode(&raw).unwrap()),
                Err(Error::Invalid(_))
            ),
            "malformed producer {version} must fail"
        );
    }
    for version in ["5.7.44-MariaDB", "8.4.6-mArIaDb", "9.7.2-MariaDB"] {
        let raw = frame(FORMAT_DESCRIPTION_EVENT, &format(version), false, 0);
        let mut framing = FrameDecoder::new();
        assert!(
            matches!(BinlogParser::<Minimal>::default().parse(framing.decode(&raw).unwrap()), Err(Error::Unsupported(message)) if message.contains(version)),
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
        let event = BinlogParser::<Minimal>::default()
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
    let mut reader =
        BinlogFile::<_, Minimal>::new(&truncated[..], ParserConfig::default()).unwrap();
    reader.next_event().unwrap();
    assert!(
        matches!(reader.next_event(),Err(Error::Io(e)) if e.kind()==io::ErrorKind::UnexpectedEof)
    );
    let unknown = frame(FORMAT_DESCRIPTION_EVENT, &format("10.0.1"), false, 0);
    let mut framing = FrameDecoder::new();
    let raw = framing.decode(&unknown).unwrap();
    assert!(matches!(
        BinlogParser::<Minimal>::default().parse(raw),
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
    let mut parser = BinlogParser::<Minimal>::default();
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
            BinlogParser::<Minimal>::default()
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
                .raw_rows(&decoder)
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
                        assert_eq!(fields[0].column.index, 0);
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
        event.raw_rows(&decoder).unwrap()
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
            fields.iter().map(|f| f.column.index).collect::<Vec<_>>(),
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
                .raw_rows(&decoder)
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
            .raw_rows(&decoder)
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
        .raw_rows(&decoder)
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
        decoder.columns()[4].definition.wire_type(),
        ColumnType::MYSQL_TYPE_STRING
    );
    assert_eq!(
        decoder.columns()[4].definition.column_type(),
        ColumnType::MYSQL_TYPE_ENUM
    );
    assert_eq!(decoder.columns()[9].definition.metadata(), &[6]);
    assert!(!decoder.columns()[0].definition.nullable());
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
        .raw_rows(&decoder)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .after
        .unwrap();
    let fields = image.fields().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        fields.iter().map(|f| f.column.index).collect::<Vec<_>>(),
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
                matches!(event.raw_rows(&decoder), Err(Error::Invalid(message)) if message == "ROWS event contains no rows")
            );
        } else {
            let mut rows = event.raw_rows(&decoder).unwrap();
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
        .raw_rows(&decoder)
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
        matches!(event.raw_rows(&decoder), Err(Error::Invalid(message)) if message == "ROWS table ID differs from TABLE_MAP")
    );
    let body = rows_body(1, &[1], &[0], false);
    assert!(
        matches!(rows_event(&body, RowsKind::Write, false).raw_rows(&decoder), Err(Error::Invalid(message)) if message == "ROWS column count differs from TABLE_MAP")
    );
    for (kind, maps) in [
        (RowsKind::Write, vec![0]),
        (RowsKind::Delete, vec![0]),
        (RowsKind::Update, vec![1, 0]),
    ] {
        let body = rows_body(2, &maps, &[0], false);
        assert!(
            matches!(rows_event(&body, kind, false).raw_rows(&decoder), Err(Error::Invalid(message)) if message == "empty row image")
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
            .raw_rows(&decoder)
            .unwrap();
        assert!(matches!(rows.next(), Some(Err(Error::Invalid(message))) if message == expected));
        assert!(rows.next().is_none());
    }
    // A sparse INSERT is legal and does not need a primary key in the raw library.
    let body = rows_body(2, &[2], &[0xff], false);
    let image = rows_event(&body, RowsKind::Write, false)
        .raw_rows(&decoder)
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
            matches!(RowsDecoder::<Minimal>::new(&table), Err(Error::Invalid(_))),
            "types={types:?}, metadata={metadata:?}"
        );
    }
    for ty in [0, 20, 243, 244] {
        let body = table_body(1, &[ty], &[], &[0]);
        let mut table = TableMapEvent::default();
        table.decode(&body).unwrap();
        assert!(
            matches!(RowsDecoder::<Minimal>::new(&table), Err(Error::Unsupported(message)) if message == format!("wire column type {ty}"))
        );
    }
    let body = table_body(1, &[15], &[10], &[0]);
    let mut table = TableMapEvent::default();
    table.decode(&body).unwrap();
    assert!(matches!(
        RowsDecoder::<Minimal>::new(&table),
        Err(Error::Truncated { .. })
    ));
    let mut body = table_body(1, &[3], &[], &[0]);
    body.extend_from_slice(&[99, 2, 1]);
    table.decode(&body).unwrap();
    assert!(matches!(
        RowsDecoder::<Minimal>::new(&table),
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
        let mut reader =
            BinlogFile::<_, Minimal>::new(&bytes[..], ParserConfig::default()).unwrap();
        reader.next_event().unwrap();
        let event = reader.next_event().unwrap().unwrap();
        let EventData::TableMap(table) = event.event else {
            panic!("TABLE_MAP expected")
        };
        let decoder = RowsDecoder::<Minimal>::new(&table).unwrap();
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
        .raw_rows(&decoder)
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
    let full_body = &owned.raw_data()[19..owned.raw_data().len() - 4];
    let full_event = rows_event_for::<Full>(full_body, RowsKind::Write, true);
    let mut table = TableMapEvent::default();
    table.decode(&map).unwrap();
    let full_decoder = RowsDecoder::<Full>::new(&table).unwrap();
    for image in [
        decoder
            .rows(&rows)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .operation
            .into_images()
            .1
            .unwrap()
            .as_ref()
            .clone(),
        full_decoder
            .rows(&full_event)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .operation
            .into_images()
            .1
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
        .raw_rows(&remapped)
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
        decoder.columns()[0].definition.column_type(),
        ColumnType::MYSQL_TYPE_VARCHAR
    );
    assert_eq!(
        remapped.columns()[0].definition.column_type(),
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
        .raw_rows(&decoder)
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
        .raw_rows(&decoder)
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
    assert_eq!(fields[9].column.index, 75);
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
                .raw_rows(&decoder)
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
    let decoder = rows_decoder_for::<Full>(&[3, 15], &[10, 0], &[2]);
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
            let event = rows_event_for::<Full>(&body, kind, v2);
            assert_eq!(event.table_id, 1);
            assert_eq!(decoder.columns().len(), 2);
            assert_eq!(event.kind, kind);
            assert_eq!(event.flags, 0);
            let mut rows = decoder.rows(&event).unwrap();
            let row: Row<'_, Full> = rows.next().unwrap().unwrap();
            let (before, after) = row.operation.into_images();
            assert!(rows.next().is_none());
            assert!(rows.next().is_none());
            assert_eq!(before.is_some(), kind != RowsKind::Write);
            assert_eq!(after.is_some(), kind != RowsKind::Delete);
            for (after, image) in [(false, before), (true, after)] {
                let Some(image): Option<CompleteImage<'_>> = image else {
                    continue;
                };
                assert_eq!(image.column_count(), 2);
                assert_eq!(image.fields().count(), 2);
                let RowValue::Integer(value) = image.field(0).unwrap().value else {
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
                    assert_eq!(image.field(1).unwrap().value, RowValue::Null);
                } else {
                    let RowValue::Bytes(text) = image.field(1).unwrap().value else {
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
    let row: Row<'_, Minimal> = decoder.rows(&event).unwrap().next().unwrap().unwrap();
    let (before, after) = row.operation.into_images();
    let before: SparseImage<'_> = before.unwrap();
    let after: SparseImage<'_> = after.unwrap();
    assert_eq!(before.column_count(), 3);
    assert_eq!(before.column_indices(), &[0, 2]);
    assert_eq!(before.field(1).unwrap().map(|field| field.value), None);
    let Some(RowValue::Integer(value)) = before.field(2).unwrap().map(|field| field.value) else {
        panic!("integer expected")
    };
    assert_eq!(value.signed(), 10);
    assert_eq!(after.column_count(), 3);
    assert_eq!(after.column_indices(), &[1]);
    assert_eq!(after.field(0).unwrap().map(|field| field.value), None);
    assert_eq!(
        after.field(1).unwrap().map(|field| field.value),
        Some(RowValue::Null)
    );
    assert_eq!(after.field(2).unwrap().map(|field| field.value), None);
    assert!(
        matches!(after.field(3), Err(Error::Invalid(message)) if message == "column index outside row image")
    );
    assert!(matches!(CompleteImage::try_from(before.as_ref().clone()),
        Err(Error::Unsupported(message)) if message == "FULL row images must contain every table column"));

    // A complete bitmap is valid in MINIMAL too; bytes cannot prove the producer setting.
    let body = rows_body(3, &[7], &[2, 1, 0, 0, 0, 10, 0, 0, 0], false);
    let event = rows_event(&body, RowsKind::Write, false);
    let row = decoder.rows(&event).unwrap().next().unwrap().unwrap();
    let (_, after) = row.operation.into_images();
    let image = CompleteImage::try_from(after.unwrap().as_ref().clone()).unwrap();
    assert_eq!(image.column_count(), 3);
    assert_eq!(image.field(1).unwrap().value, RowValue::Null);
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
            let mut event = RowsEvent::<Full>::with_layout(kind, if v2 { 10 } else { 8 }, v2);
            assert!(matches!(event.decode(&body),
                Err(Error::Unsupported(message)) if message == "FULL row images must contain every table column"));
        }
    }
    let body = rows_body(3, &[3], &[0, 5, 6], true);
    let event = rows_event(&body, RowsKind::Write, true);
    let row = event.raw_rows(&decoder).unwrap().next().unwrap().unwrap();
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
    let event = rows_event_for::<Full>(&body, RowsKind::Write, true);
    let typed = {
        let map_body = table_body(1, &[1; 80], &[], &nullable);
        let mut map = TableMapEvent::default();
        map.decode(&map_body).unwrap();
        let decoder = RowsDecoder::<Full>::new(&map).unwrap();
        decoder.rows(&event).unwrap()
    };
    let rows = typed.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(rows.len(), 2);
    let first = rows[0].operation.after().unwrap();
    let second = rows[1].operation.after().unwrap();
    assert_eq!(
        first.as_ref().column_indices().as_ptr(),
        second.as_ref().column_indices().as_ptr()
    );
    for (offset, image) in [(0, first), (80, second)] {
        assert_eq!(image.column_count(), 80);
        assert_eq!(image.field(75).unwrap().value, RowValue::Null);
        let RowValue::Integer(value) = image.field(79).unwrap().value else {
            panic!("integer expected")
        };
        assert_eq!(value.unsigned(), 79 + offset);
        assert_eq!(image.fields().count(), 80);
    }
}

#[test]
fn typed_decoding_preserves_errors_and_never_yields_partial_updates() {
    let decoder = rows_decoder_for::<Full>(&[3], &[], &[0]);
    let body = rows_body(1, &[1, 1], &[0, 1, 0, 0, 0, 0, 2], true);
    let event = rows_event_for::<Full>(&body, RowsKind::Update, true);
    let mut rows = decoder.rows(&event).unwrap();
    assert!(matches!(
        rows.next(),
        Some(Err(Error::Truncated { need: 4, have: 1 }))
    ));
    assert!(rows.next().is_none());
    assert!(rows.next().is_none());

    let body = rows_body(1, &[1], &[1], false);
    let event = rows_event_for::<Full>(&body, RowsKind::Write, false);
    let mut rows = decoder.rows(&event).unwrap();
    assert!(matches!(rows.next(), Some(Err(Error::Invalid(message)))
        if message == "NULL in a NOT NULL column"));
    assert!(rows.next().is_none());

    let body = rows_body(2, &[3], &[0, 1, 2], true);
    let event = rows_event_for::<Full>(&body, RowsKind::Write, true);
    assert!(matches!(decoder.rows(&event), Err(Error::Invalid(message))
        if message == "ROWS column count differs from TABLE_MAP"));
    let body = rows_body(1, &[1], &[0, 1, 0, 0, 0], true);
    let mut event = rows_event_for::<Full>(&body, RowsKind::Write, true);
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
    let decoder = rows_decoder_for::<Minimal>(&types, &[20, 0], &nullable);
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
                let (before, after) = (row.operation.before(), row.operation.after());
                assert_eq!(before.is_some(), kind != RowsKind::Write);
                assert_eq!(after.is_some(), kind != RowsKind::Delete);
                if let Some(image) = &before {
                    assert_eq!(image.column_count(), 80);
                    assert_eq!(image.column_indices(), &[70]);
                    assert_eq!(image.field(0).unwrap().map(|field| field.value), None);
                    let Some(RowValue::Integer(value)) =
                        image.field(70).unwrap().map(|field| field.value)
                    else {
                        panic!("integer expected")
                    };
                    assert_eq!(value.unsigned(), 20 + index as u64);
                }
                if let Some(image) = &after {
                    assert_eq!(image.column_count(), 80);
                    assert_eq!(
                        image.column_indices(),
                        if kind == RowsKind::Write {
                            &[0, 75][..]
                        } else {
                            &[75][..]
                        }
                    );
                    assert_eq!(image.field(70).unwrap().map(|field| field.value), None);
                    if kind == RowsKind::Write {
                        let Some(RowValue::Integer(value)) =
                            image.field(0).unwrap().map(|field| field.value)
                        else {
                            panic!("integer expected")
                        };
                        assert_eq!(value.unsigned(), 10 + index as u64);
                    }
                    if index == 0 {
                        let Some(RowValue::Bytes(text)) =
                            image.field(75).unwrap().map(|field| field.value)
                        else {
                            panic!("bytes expected")
                        };
                        assert_eq!(text, "中".as_bytes());
                        let offset = body
                            .windows(text.len())
                            .position(|value| value == text)
                            .unwrap();
                        assert_eq!(text.as_ptr(), body[offset..].as_ptr());
                    } else {
                        assert_eq!(
                            image.field(75).unwrap().map(|field| field.value),
                            Some(RowValue::Null)
                        );
                    }
                }
            }
            for (first, second) in [
                (rows[0].operation.before(), rows[1].operation.before()),
                (rows[0].operation.after(), rows[1].operation.after()),
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

fn typed_late_error<M: BinlogMode>() {
    let decoder = rows_decoder_for::<M>(&[3], &[], &[0]);
    for v2 in [false, true] {
        let values = [0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 0, 3, 0, 0, 0, 0, 4];
        let body = rows_body(1, &[1, 1], &values, v2);
        let event = rows_event_for::<M>(&body, RowsKind::Update, v2);
        let mut rows = decoder.rows(&event).unwrap();
        let row = rows.next().unwrap().unwrap();
        let (before, after) = row.operation.into_images();
        for (image, expected) in [(before, 1), (after, 2)] {
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

#[test]
fn generic_event_envelopes_preserve_modes_owned_views_and_borrowed_values() {
    fn check<M: BinlogMode>(crc: bool, v2: bool) {
        let body = rows_body(2, &[3], &[0, 7, 3, b'a', b'b', b'c'], v2);
        let row_frame = frame(
            if v2 {
                WRITE_ROWS_EVENT
            } else {
                WRITE_ROWS_EVENT_V1
            },
            &body,
            crc,
            0,
        );
        let bytes = file_bytes(&[
            frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), crc, 0),
            frame(
                TABLE_MAP_EVENT,
                &table_body(1, &[1, 15], &[10, 0], &[0]),
                crc,
                0,
            ),
            row_frame.clone(),
        ]);
        let mut reader = BinlogFile::<_, M>::new(&bytes[..], ParserConfig::default()).unwrap();
        assert!(matches!(
            reader.next_event().unwrap().unwrap().event,
            EventData::FormatDescription(_)
        ));
        let mapping = reader.next_event().unwrap().unwrap();
        let EventData::TableMap(table) = mapping.event else {
            panic!("TABLE_MAP expected")
        };
        assert_eq!(table.table_id, 1);
        let owned: mysql_replication::OwnedBinlogEvent<M> =
            reader.next_event().unwrap().unwrap().into_owned().unwrap();
        drop(reader);
        assert_eq!(owned.raw_data(), row_frame);
        let view: mysql_replication::BinlogEvent<'_, M> = owned.view().unwrap();
        assert_eq!(view.header.server_id, 456);
        let EventData::Rows(rows) = view.event else {
            panic!("ROWS expected")
        };
        let mut rows = rows.rows().unwrap();
        let row: Row<'_, M> = rows.next().unwrap().unwrap();
        let (before, after) = row.operation.into_images();
        assert!(before.is_none());
        let image = after.unwrap();
        let fields = image
            .as_ref()
            .fields()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(fields.len(), 2);
        let RowValue::Integer(value) = fields[0].value else {
            panic!("integer expected")
        };
        assert_eq!(value.signed(), 7);
        let RowValue::Bytes(value) = fields[1].value else {
            panic!("bytes expected")
        };
        assert_eq!(value, b"abc");
        let start = owned.raw_data().as_ptr() as usize;
        assert!((start..start + owned.raw_data().len()).contains(&(value.as_ptr() as usize)));
        assert!(rows.next().is_none());
    }
    for crc in [false, true] {
        for v2 in [false, true] {
            check::<Minimal>(crc, v2);
            check::<Full>(crc, v2);
        }
    }
}

#[test]
fn full_parser_rejects_sparse_envelopes_before_returning_them() {
    for (kind, v2, bitmaps) in [
        (WRITE_ROWS_EVENT_V1, false, &[3][..]),
        (DELETE_ROWS_EVENT_V1, false, &[3][..]),
        (UPDATE_ROWS_EVENT_V1, false, &[3, 7][..]),
        (WRITE_ROWS_EVENT, true, &[3][..]),
        (DELETE_ROWS_EVENT, true, &[3][..]),
        (UPDATE_ROWS_EVENT, true, &[7, 3][..]),
    ] {
        let bytes = file_bytes(&[
            frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), true, 0),
            frame(
                TABLE_MAP_EVENT,
                &table_body(1, &[1, 1, 1], &[], &[0]),
                true,
                0,
            ),
            frame(kind, &rows_body(3, bitmaps, &[0], v2), true, 0),
        ]);
        let mut full = BinlogFile::<_, Full>::new(&bytes[..], ParserConfig::default()).unwrap();
        full.next_event().unwrap().unwrap();
        full.next_event().unwrap().unwrap();
        assert!(matches!(full.next_event(), Err(Error::Unsupported(message))
            if message == "FULL row images must contain every table column"));
        let mut minimal =
            BinlogFile::<_, Minimal>::new(&bytes[..], ParserConfig::default()).unwrap();
        minimal.next_event().unwrap().unwrap();
        minimal.next_event().unwrap().unwrap();
        assert!(matches!(
            minimal.next_event().unwrap().unwrap().event,
            EventData::Rows(_)
        ));
    }
}

#[test]
fn full_envelope_keeps_statement_end_markers_without_row_images() {
    let mut marker = 0x00ff_ffffu64.to_le_bytes()[..6].to_vec();
    marker.extend(1u16.to_le_bytes());
    let bytes = file_bytes(&[
        frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), false, 0),
        frame(WRITE_ROWS_EVENT, &marker, false, 0),
    ]);
    let mut reader = BinlogFile::<_, Full>::new(&bytes[..], ParserConfig::default()).unwrap();
    reader.next_event().unwrap().unwrap();
    let EventData::RowsStatementEnd(marker) = reader.next_event().unwrap().unwrap().event else {
        panic!("marker expected")
    };
    assert_eq!(marker.table_id(), 0x00ff_ffff);
    assert_eq!(marker.flags(), 1);
}

#[test]
fn typed_stream_checks_mode_conflicts_before_connecting() {
    use mysql_replication::stream::{
        ConnectionConfig, ReplicationConfig, ReplicationStream, RowImage,
    };
    let connection = ConnectionConfig::new("UNSET_GENERIC_IMAGE_MODE_TEST_URL");
    let mut config = ReplicationConfig::new(123, "binlog.000001", 4);
    config.required_row_image = Some(RowImage::Minimal);
    assert!(
        matches!(ReplicationStream::<Full>::connect(&connection, config),
        Err(Error::Invalid(message)) if message == "replication configuration conflicts with the generic image mode")
    );
}

fn table_with_optional(optional: &[u8]) -> Vec<u8> {
    let mut table = table_body(1, &[1, 15, 1], &[20, 0], &[2]);
    table.extend(optional);
    table
}

fn column_metadata(name: &str) -> Vec<u8> {
    // Independent wire TLVs: signedness, names, character collations and primary key.
    let mut names = vec![2, b'i', b'd'];
    lenenc(name.len(), &mut names);
    names.extend(name.as_bytes());
    names.extend([3, b'a', b'g', b'e']);
    let mut optional = vec![1, 1, 0x80, 4];
    lenenc(names.len(), &mut optional);
    optional.extend(names);
    optional.extend([3, 1, 45, 8, 1, 0, 200, 2, 0xaa, 0xbb]);
    optional
}

#[test]
fn generic_events_bind_historical_columns_and_keep_them_in_owned_views() {
    fn check<M: BinlogMode>(
        bitmaps: &[u8],
        values: &[u8],
        expected_before: &[u16],
        expected_after: &[u16],
    ) {
        for crc in [false, true] {
            for v2 in [false, true] {
                let optional = column_metadata("昵称");
                let mut row = rows_body(3, bitmaps, values, v2);
                row[6..8].copy_from_slice(&1u16.to_le_bytes());
                let raw_row = frame(
                    if v2 {
                        UPDATE_ROWS_EVENT
                    } else {
                        UPDATE_ROWS_EVENT_V1
                    },
                    &row,
                    crc,
                    0,
                );
                let bytes = file_bytes(&[
                    frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), crc, 0),
                    frame(TABLE_MAP_EVENT, &table_with_optional(&optional), crc, 0),
                    raw_row.clone(),
                ]);
                let owned = {
                    let mut reader =
                        BinlogFile::<_, M>::new(&bytes[..], ParserConfig::default()).unwrap();
                    reader.next_event().unwrap().unwrap();
                    reader.next_event().unwrap().unwrap();
                    reader.next_event().unwrap().unwrap().into_owned().unwrap()
                };
                assert_eq!(owned.raw_data(), raw_row);
                let view = owned.view().unwrap();
                let EventData::Rows(rows) = view.event else {
                    panic!("ROWS expected")
                };
                let table = rows.table().unwrap();
                assert_eq!(&*table.schema, b"app");
                assert_eq!(&*table.table, b"t");
                assert_eq!(&*table.optional_metadata, optional);
                let columns = rows.columns().unwrap();
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].name.as_deref(), Some("id"));
                assert_eq!(columns[1].name.as_deref(), Some("昵称"));
                assert_eq!(columns[2].name.as_deref(), Some("age"));
                assert_eq!(
                    columns.iter().map(|c| c.index).collect::<Vec<_>>(),
                    [0, 1, 2]
                );
                assert_eq!(
                    columns.iter().map(|c| c.unsigned).collect::<Vec<_>>(),
                    [Some(true), None, Some(false)]
                );
                assert_eq!(
                    columns.iter().map(|c| c.primary_key).collect::<Vec<_>>(),
                    [Some(true), Some(false), Some(false)]
                );
                assert_eq!(columns[0].primary_key_prefix, Some(0));
                assert_eq!(columns[1].collation_id, Some(45));
                assert_eq!(
                    columns[1].definition.column_type(),
                    ColumnType::MYSQL_TYPE_VARCHAR
                );
                assert!(columns[1].definition.nullable());
                let mut iter = rows.rows().unwrap();
                let operation = iter.next().unwrap().unwrap().operation;
                let mysql_replication::events::RowOperation::Update { before, after } = operation
                else {
                    panic!("UPDATE expected")
                };
                let before = before.as_ref();
                let after = after.as_ref();
                assert_eq!(before.column_indices(), expected_before);
                assert_eq!(after.column_indices(), expected_after);
                assert!(std::ptr::eq(columns.as_ptr(), before.columns().as_ptr()));
                assert!(std::ptr::eq(columns.as_ptr(), after.columns().as_ptr()));
                let RowValue::Integer(key) = before.fields().next().unwrap().unwrap().value else {
                    panic!("integer expected")
                };
                assert_eq!(key.unsigned(), 7);
                let null = after
                    .fields()
                    .find(|field| field.as_ref().unwrap().column.index == 1)
                    .unwrap()
                    .unwrap();
                assert_eq!(null.value, RowValue::Null);
                assert!(iter.next().is_none());
                // Rebuilding an owned view keeps the same historical metadata allocation.
                let again = owned.view().unwrap();
                let EventData::Rows(again) = again.event else {
                    panic!("ROWS expected")
                };
                assert!(std::ptr::eq(
                    again.columns().unwrap().as_ptr(),
                    columns.as_ptr()
                ));
            }
        }
    }
    check::<Full>(
        &[7, 7],
        &[0, 7, 3, b'o', b'l', b'd', 20, 2, 8, 21],
        &[0, 1, 2],
        &[0, 1, 2],
    );
    check::<Minimal>(&[1, 3], &[0, 7, 2, 8], &[0], &[0, 1]);
}

#[test]
fn typed_fields_expose_metadata_and_preserve_absent_null_and_borrowed_values() {
    let body = table_with_optional(&column_metadata("name"));
    let mut table = TableMapEvent::default();
    table.decode(&body).unwrap();
    let decoder = RowsDecoder::<Minimal>::new(&table).unwrap();
    let body = rows_body(3, &[3], &[2, 7], true);
    let event = rows_event(&body, RowsKind::Write, true);
    let row = decoder.rows(&event).unwrap().next().unwrap().unwrap();
    let after = row.operation.after().unwrap();
    let key = after.field(0).unwrap().unwrap();
    assert_eq!(key.column.name.as_deref(), Some("id"));
    assert_eq!(key.column.unsigned, Some(true));
    let null = after.field(1).unwrap().unwrap();
    assert_eq!(null.column.name.as_deref(), Some("name"));
    assert_eq!(null.value, RowValue::Null);
    assert_eq!(after.field(2).unwrap(), None);
    assert!(
        matches!(after.field(3), Err(Error::Invalid(message)) if message == "column index outside row image")
    );
    let fields = after.fields().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(fields, [key, null]);
    assert!(std::ptr::eq(key.column, &after.columns()[0]));

    let body = rows_body(3, &[7], &[0, 7, 3, b'n', b'e', b'w', 20], false);
    let event = rows_event_for::<Full>(&body, RowsKind::Write, false);
    let decoder = RowsDecoder::<Full>::new(&table).unwrap();
    let row = decoder.rows(&event).unwrap().next().unwrap().unwrap();
    let after = row.operation.after().unwrap();
    let field: mysql_replication::events::Field<'_, '_> = after.field(1).unwrap();
    assert_eq!(field.column.name.as_deref(), Some("name"));
    let RowValue::Bytes(value) = field.value else {
        panic!("borrowed text expected")
    };
    assert_eq!(value, b"new");
    assert_eq!(value.as_ptr(), body[body.len() - 4..].as_ptr());
    assert_eq!(after.fields().count(), 3);
}

#[test]
fn table_metadata_validates_known_attributes_and_preserves_missing_information() {
    for (optional, message) in [
        (vec![1, 2, 0, 0], "TABLE_MAP signedness bitmap length"),
        (vec![4, 1, 0], "empty TABLE_MAP column name"),
        (
            vec![3, 2, 45, 45],
            "TABLE_MAP charset count differs from columns",
        ),
        (
            vec![3, 1, 45, 2, 1, 45],
            "conflicting TABLE_MAP charset metadata",
        ),
        (
            vec![2, 3, 45, 1, 45],
            "TABLE_MAP charset index outside character columns",
        ),
        (vec![8, 1, 3], "TABLE_MAP primary-key index outside columns"),
        (vec![8, 2, 0, 0], "duplicate TABLE_MAP primary-key index"),
        (
            vec![8, 1, 0, 9, 2, 0, 0],
            "conflicting TABLE_MAP primary-key metadata",
        ),
        (
            vec![200, 0, 200, 0],
            "duplicate TABLE_MAP optional metadata",
        ),
    ] {
        let body = table_with_optional(&optional);
        let mut table = TableMapEvent::default();
        table.decode(&body).unwrap();
        assert!(
            matches!(RowsDecoder::<Minimal>::new(&table), Err(Error::Invalid(actual)) if actual == message)
        );
    }
    for optional in [vec![4, 2, 1], vec![4, 2, 2, b'x'], vec![9, 1, 0]] {
        let body = table_with_optional(&optional);
        let mut table = TableMapEvent::default();
        table.decode(&body).unwrap();
        assert!(matches!(
            RowsDecoder::<Minimal>::new(&table),
            Err(Error::Truncated { .. })
        ));
    }
    let decoder = rows_decoder(&[3, 15], &[20, 0], &[2]);
    for column in decoder.columns() {
        assert_eq!(column.name, None);
        assert_eq!(column.unsigned, None);
        assert_eq!(column.collation_id, None);
        assert_eq!(column.primary_key, None);
    }
    // Charset overrides use the compact character-column index, not the table ordinal.
    let mut body = table_body(1, &[1, 15, 1, 15], &[20, 0, 20, 0], &[0]);
    body.extend([2, 3, 45, 1, 63, 9, 2, 3, 4]);
    let mut table = TableMapEvent::default();
    table.decode(&body).unwrap();
    let decoder = RowsDecoder::<Minimal>::new(&table).unwrap();
    assert_eq!(decoder.columns()[1].collation_id, Some(45));
    assert_eq!(decoder.columns()[3].collation_id, Some(63));
    assert_eq!(decoder.columns()[3].primary_key, Some(true));
    assert_eq!(decoder.columns()[3].primary_key_prefix, Some(4));
}

#[test]
fn typed_parser_remaps_and_sources_keep_independent_metadata_snapshots() {
    fn read<M: BinlogMode>(name: &str) -> mysql_replication::OwnedBinlogEvent<M> {
        let bytes = file_bytes(&[
            frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), true, 0),
            frame(
                TABLE_MAP_EVENT,
                &table_with_optional(&column_metadata(name)),
                true,
                0,
            ),
            frame(
                WRITE_ROWS_EVENT,
                &rows_body(3, &[7], &[2, 7, 20], true),
                true,
                0,
            ),
        ]);
        let mut reader = BinlogFile::<_, M>::new(&bytes[..], ParserConfig::default()).unwrap();
        reader.next_event().unwrap().unwrap();
        reader.next_event().unwrap().unwrap();
        reader.next_event().unwrap().unwrap().into_owned().unwrap()
    }
    fn check<M: BinlogMode>() {
        let first = read::<M>("first");
        let second = read::<M>("second");
        for (event, expected) in [(&first, "first"), (&second, "second")] {
            let view = event.view().unwrap();
            let EventData::Rows(rows) = view.event else {
                panic!("ROWS expected")
            };
            assert_eq!(rows.columns().unwrap()[1].name.as_deref(), Some(expected));
        }
        let bytes = file_bytes(&[
            frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), false, 0),
            frame(
                TABLE_MAP_EVENT,
                &table_with_optional(&column_metadata("old")),
                false,
                0,
            ),
            frame(
                WRITE_ROWS_EVENT,
                &rows_body(3, &[7], &[2, 7, 20], true),
                false,
                0,
            ),
            frame(
                TABLE_MAP_EVENT,
                &table_with_optional(&column_metadata("new")),
                false,
                0,
            ),
            frame(
                WRITE_ROWS_EVENT,
                &rows_body(3, &[7], &[2, 7, 20], true),
                false,
                0,
            ),
        ]);
        let mut reader = BinlogFile::<_, M>::new(&bytes[..], ParserConfig::default()).unwrap();
        reader.next_event().unwrap().unwrap();
        reader.next_event().unwrap().unwrap();
        let old = reader.next_event().unwrap().unwrap().into_owned().unwrap();
        reader.next_event().unwrap().unwrap();
        let new = reader.next_event().unwrap().unwrap().into_owned().unwrap();
        for (event, expected) in [(&old, "old"), (&new, "new")] {
            let view = event.view().unwrap();
            let EventData::Rows(rows) = view.event else {
                panic!("ROWS expected")
            };
            assert_eq!(rows.columns().unwrap()[1].name.as_deref(), Some(expected));
        }
    }
    check::<Full>();
    check::<Minimal>();
}

#[test]
fn typed_parsers_reject_missing_maps_and_partial_json_while_raw_events_remain_available() {
    fn check<M: BinlogMode>() {
        for (kind, body, expected) in [
            (
                WRITE_ROWS_EVENT,
                rows_body(1, &[1], &[0, 7], true),
                "missing TABLE_MAP for 1",
            ),
            (PARTIAL_UPDATE_ROWS_EVENT, vec![], "partial JSON row images"),
        ] {
            let bytes = file_bytes(&[
                frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), false, 0),
                frame(kind, &body, false, 0),
            ]);
            let mut reader = BinlogFile::<_, M>::new(&bytes[..], ParserConfig::default()).unwrap();
            reader.next_event().unwrap().unwrap();
            match reader.next_event() {
                Err(Error::Invalid(message)) if kind == WRITE_ROWS_EVENT => {
                    assert_eq!(message, expected)
                }
                Err(Error::Unsupported(message)) if kind == PARTIAL_UPDATE_ROWS_EVENT => {
                    assert_eq!(message, expected)
                }
                _ => panic!("expected explicit typed parser error"),
            }
            let mut raw = mysql_replication::file::FrameReader::new(&bytes[..]).unwrap();
            raw.next_frame().unwrap().unwrap();
            assert!(raw.next_frame().unwrap().is_some());
        }
    }
    check::<Full>();
    check::<Minimal>();
}

#[test]
fn typed_parser_boundaries_clear_maps_without_invalidating_owned_events() {
    fn check<M: BinlogMode>() {
        let mut final_row = rows_body(1, &[1], &[0, 7], true);
        final_row[6..8].copy_from_slice(&1u16.to_le_bytes());
        let mut sentinel = 0x00ff_ffffu64.to_le_bytes()[..6].to_vec();
        sentinel.extend(1u16.to_le_bytes());
        let mut rotate = 4u64.to_le_bytes().to_vec();
        rotate.extend(b"binlog.000002");
        let mut query = vec![0; 13];
        query.extend(b"\0COMMIT");
        for boundary in [
            frame(WRITE_ROWS_EVENT, &final_row, true, 0),
            frame(WRITE_ROWS_EVENT, &sentinel, true, 0),
            frame(ROTATE_EVENT, &rotate, true, 0),
            frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), true, 0),
            frame(XID_EVENT, &7u64.to_le_bytes(), true, 0),
            frame(QUERY_EVENT, &query, true, 0),
        ] {
            let bytes = file_bytes(&[
                frame(FORMAT_DESCRIPTION_EVENT, &format("8.4.6"), true, 0),
                frame(TABLE_MAP_EVENT, &table_body(1, &[1], &[], &[0]), true, 0),
                frame(
                    WRITE_ROWS_EVENT,
                    &rows_body(1, &[1], &[0, 7], true),
                    true,
                    0,
                ),
                boundary,
                frame(
                    WRITE_ROWS_EVENT,
                    &rows_body(1, &[1], &[0, 8], true),
                    true,
                    0,
                ),
            ]);
            let mut reader = BinlogFile::<_, M>::new(&bytes[..], ParserConfig::default()).unwrap();
            reader.next_event().unwrap().unwrap();
            reader.next_event().unwrap().unwrap();
            let old = reader.next_event().unwrap().unwrap().into_owned().unwrap();
            let boundary = reader.next_event().unwrap().unwrap().into_owned().unwrap();
            if let EventData::RowsStatementEnd(marker) = boundary.view().unwrap().event {
                assert_eq!(marker.table_id(), 0x00ff_ffff);
                assert_eq!(marker.flags(), 1);
                assert_eq!(marker.kind(), RowsKind::Write);
            }
            assert!(
                matches!(reader.next_event(), Err(Error::Invalid(message)) if message == "missing TABLE_MAP for 1")
            );
            let view = old.view().unwrap();
            let EventData::Rows(rows) = view.event else {
                panic!("ROWS expected")
            };
            let row = rows.rows().unwrap().next().unwrap().unwrap();
            let after = row.operation.after().unwrap();
            let RowValue::Integer(key) = after.as_ref().fields().next().unwrap().unwrap().value
            else {
                panic!("integer expected")
            };
            assert_eq!(key.unsigned(), 7);
        }
    }
    check::<Full>();
    check::<Minimal>();
}

#[test]
fn optional_metadata_uses_compact_numeric_and_enum_set_column_ordinals() {
    let mut types = vec![1; 12];
    types[1] = 15;
    types[5] = 15;
    let mut body = table_body(1, &types, &[20, 0, 20, 0], &[0, 0]);
    body.extend([1, 2, 0x81, 0x80]);
    let mut table = TableMapEvent::default();
    table.decode(&body).unwrap();
    let decoder = RowsDecoder::<Minimal>::new(&table).unwrap();
    let numeric: Vec<_> = decoder
        .columns()
        .iter()
        .filter_map(|column| column.unsigned)
        .collect();
    assert_eq!(
        numeric,
        [
            true, false, false, false, false, false, false, true, true, false
        ]
    );
    assert_eq!(decoder.columns()[1].unsigned, None);
    assert_eq!(decoder.columns()[5].unsigned, None);

    let mut body = table_body(1, &[1, 254, 15, 254], &[247, 1, 20, 0, 248, 1], &[0]);
    body.extend([3, 1, 45, 11, 4, 63, 0xfc, 255, 0]);
    let mut table = TableMapEvent::default();
    table.decode(&body).unwrap();
    let decoder = RowsDecoder::<Minimal>::new(&table).unwrap();
    assert_eq!(
        decoder.columns()[1].definition.column_type(),
        ColumnType::MYSQL_TYPE_ENUM
    );
    assert_eq!(
        decoder.columns()[3].definition.column_type(),
        ColumnType::MYSQL_TYPE_SET
    );
    assert_eq!(
        decoder
            .columns()
            .iter()
            .map(|column| column.collation_id)
            .collect::<Vec<_>>(),
        [None, Some(63), Some(45), Some(255)]
    );
}
