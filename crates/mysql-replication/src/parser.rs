use crate::{BinlogFrame, Error, Result, error::invalid, event_type as code, events::*};

/// ROWS values are never materialized. Bitmaps can also be deferred until a table matches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RowsDecode {
    #[default]
    Deferred,
    ValidateImages,
}
#[derive(Debug, Clone, Copy)]
pub struct ParserConfig {
    pub rows: RowsDecode,
    pub decode_gtids: bool,
    pub decode_previous_gtids: bool,
}
impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            rows: RowsDecode::Deferred,
            decode_gtids: true,
            decode_previous_gtids: true,
        }
    }
}
impl ParserConfig {
    /// CDC needs group boundaries, not GTID metadata or previous GTID interval lists.
    pub fn minimal() -> Self {
        Self {
            decode_gtids: false,
            decode_previous_gtids: false,
            ..Self::default()
        }
    }
}

/// FDE selects a file's layouts once; row loops never inspect server versions.
pub struct BinlogParser<M: BinlogMode = Unspecified> {
    mode: std::marker::PhantomData<M>,
    config: ParserConfig,
    headers: Option<[u8; 256]>,
}
impl Default for BinlogParser {
    fn default() -> Self {
        Self::new(ParserConfig::default())
    }
}
impl BinlogParser {
    pub fn new(config: ParserConfig) -> Self {
        Self::for_mode(config)
    }
}
impl<M: BinlogMode> BinlogParser<M> {
    pub fn for_mode(config: ParserConfig) -> Self {
        Self {
            config,
            headers: None,
            mode: std::marker::PhantomData,
        }
    }
    pub fn with_mode<N: BinlogMode>(self) -> BinlogParser<N> {
        BinlogParser {
            config: self.config,
            headers: self.headers,
            mode: std::marker::PhantomData,
        }
    }
    pub fn parse<'a>(&mut self, frame: BinlogFrame<'a>) -> Result<BinlogEvent<'a, M>> {
        let kind = frame.header.event_type;
        let mut event = if kind == code::FORMAT_DESCRIPTION_EVENT {
            let mut format = FormatDescriptionEvent::default();
            format.decode(frame.body)?;
            format.checksum_algorithm =
                match frame.raw_data.get(EventHeader::LEN + frame.body.len()) {
                    Some(0) => ChecksumAlgorithm::None,
                    Some(1) => ChecksumAlgorithm::Crc32,
                    _ => return Err(invalid("missing FDE checksum algorithm")),
                };
            let (major, minor, _) = crate::version::parse_server_version(format.server_version)?;
            if format
                .server_version
                .to_ascii_lowercase()
                .contains("mariadb")
                || !matches!((major, minor), (5, 7) | (8, 0..=4) | (9, 0 | 7))
            {
                return Err(Error::Unsupported(format!(
                    "unregistered producer {}",
                    format.server_version
                )));
            }
            for (kind, length) in [
                (code::QUERY_EVENT, 13),
                (code::ROTATE_EVENT, 8),
                (code::TABLE_MAP_EVENT, 8),
                (code::WRITE_ROWS_EVENT, 10),
                (code::UPDATE_ROWS_EVENT, 10),
                (code::DELETE_ROWS_EVENT, 10),
            ] {
                if format.post_header_lengths.get(usize::from(kind) - 1) != Some(&length) {
                    return Err(Error::Unsupported(format!(
                        "unvalidated post-header length for event {kind}"
                    )));
                }
            }
            for kind in [
                code::WRITE_ROWS_EVENT_V1,
                code::UPDATE_ROWS_EVENT_V1,
                code::DELETE_ROWS_EVENT_V1,
            ] {
                if !matches!(
                    format.post_header_lengths.get(usize::from(kind) - 1),
                    Some(0 | 8)
                ) {
                    return Err(Error::Unsupported(format!(
                        "unvalidated legacy ROWS post-header {kind}"
                    )));
                }
            }
            let mut headers = [0; 256];
            if format.post_header_lengths.len() > 255 {
                return Err(invalid("too many FDE event types"));
            }
            headers[1..1 + format.post_header_lengths.len()]
                .copy_from_slice(format.post_header_lengths);
            self.headers = Some(headers);
            EventData::FormatDescription(format)
        } else {
            if self.headers.is_none() && kind != code::ROTATE_EVENT {
                return Err(invalid("event before FORMAT_DESCRIPTION"));
            }
            let post_header = self
                .headers
                .as_ref()
                .map_or(8, |h| usize::from(h[usize::from(kind)]));
            match kind {
                code::QUERY_EVENT => EventData::Query(QueryEvent::with_post_header(post_header)),
                code::ROTATE_EVENT => EventData::Rotate(RotateEvent::with_post_header(post_header)),
                code::TABLE_MAP_EVENT => {
                    EventData::TableMap(TableMapEvent::with_post_header(post_header))
                }
                code::WRITE_ROWS_EVENT_V1
                | code::UPDATE_ROWS_EVENT_V1
                | code::DELETE_ROWS_EVENT_V1
                | code::WRITE_ROWS_EVENT
                | code::UPDATE_ROWS_EVENT
                | code::DELETE_ROWS_EVENT => {
                    if post_header == 0 {
                        return Err(Error::Unsupported(format!(
                            "ROWS event {kind} absent from FDE"
                        )));
                    }
                    let operation = match kind {
                        code::WRITE_ROWS_EVENT_V1 | code::WRITE_ROWS_EVENT => RowsKind::Write,
                        code::UPDATE_ROWS_EVENT_V1 | code::UPDATE_ROWS_EVENT => RowsKind::Update,
                        _ => RowsKind::Delete,
                    };
                    let v2 = matches!(
                        kind,
                        code::WRITE_ROWS_EVENT | code::UPDATE_ROWS_EVENT | code::DELETE_ROWS_EVENT
                    );
                    EventData::Rows(RowsEvent::<M>::for_mode(operation, post_header, v2))
                }
                code::XID_EVENT => EventData::Xid(Default::default()),
                code::GTID_EVENT if self.config.decode_gtids => EventData::Gtid(Default::default()),
                code::ANONYMOUS_GTID_EVENT if self.config.decode_gtids => {
                    EventData::AnonymousGtid(Default::default())
                }
                code::GTID_TAGGED_LOG_EVENT if self.config.decode_gtids => {
                    EventData::TaggedGtid(Default::default())
                }
                code::PREVIOUS_GTIDS_EVENT if self.config.decode_previous_gtids => {
                    EventData::PreviousGtids(Default::default())
                }
                code::HEARTBEAT_EVENT => EventData::Heartbeat(Default::default()),
                _ => EventData::Raw(Default::default()),
            }
        };
        if kind != code::FORMAT_DESCRIPTION_EVENT {
            event.decode(frame.body)?;
        }
        if let EventData::Rows(rows) = &event
            && self.config.rows == RowsDecode::ValidateImages
        {
            rows.images()?;
        }
        if let EventData::Gtid(gtid) = &event
            && gtid.gno == 0
        {
            return Err(invalid("GTID GNO must be positive"));
        }
        Ok(BinlogEvent {
            raw_data: frame.raw_data,
            header: frame.header,
            event,
            body: frame.body,
        })
    }
}
