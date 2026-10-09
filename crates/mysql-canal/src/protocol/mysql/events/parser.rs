use super::{EventParser, ParsedEvent, RowImages, RowImagesParser, RowsEvent, TableMapEvent};
use crate::{
    error::invalid,
    protocol::mysql::{
        event_type as event,
        file::EventView,
        version::{EventAction, FormatDescription},
        wire::Cursor,
    },
    Error, Operation, Result,
};

#[derive(Clone, Copy)]
struct RowsLayout {
    operation: Operation,
    id_width: usize,
    parse_images: RowImagesParser,
    standard: bool,
}

/// Compiled once from each FDE. CommittedTransaction codes directly index a fixed dispatch table;
/// the per-row loop never branches on producer version or ROWS v1/v2.
pub struct MysqlEventParser {
    table_id_width: usize,
    query_post_header: usize,
    rotate_post_header: usize,
    rows: [Option<RowsLayout>; 256],
}
impl MysqlEventParser {
    pub fn from_format(format: &FormatDescription<'_>) -> Result<Self> {
        if format.binlog_version != 4 || format.header_length != 19 {
            return Err(Error::Unsupported("MySQL v4 framing requires a 19-byte common header; use a SourcePlugin for other framing".into()));
        }
        let length = |kind: u8| {
            usize::from(
                format
                    .post_header_lengths
                    .get(usize::from(kind) - 1)
                    .copied()
                    .unwrap_or(0),
            )
        };
        let table_id_width = id_width(length(event::TABLE_MAP_EVENT), 2)?;
        let query_post_header = length(event::QUERY_EVENT);
        let rotate_post_header = length(event::ROTATE_EVENT);
        if query_post_header < 13 || rotate_post_header < 8 {
            return Err(Error::Unsupported(
                "QUERY/ROTATE post-header too short".into(),
            ));
        }
        let mut rows = [None; 256];
        for (kind, operation, v2) in row_kinds() {
            let len = length(kind);
            if len == 0 {
                continue;
            } // Removed/absent wire event, never guess.
            let width = id_width(len, if v2 { 4 } else { 2 })?;
            rows[usize::from(kind)] = Some(RowsLayout {
                operation,
                id_width: width,
                parse_images: if v2 {
                    parse_v2_row_images
                } else {
                    parse_row_images
                },
                standard: width == 6,
            });
        }
        Ok(Self {
            table_id_width,
            query_post_header,
            rotate_post_header,
            rows,
        })
    }
    /// Raw Decoder compatibility API: modern TABLE_MAP plus ROWS v1 and v2,
    /// without a file-level FDE. Streaming callers should use from_format.
    pub(crate) fn standard() -> &'static Self {
        static STANDARD: std::sync::OnceLock<MysqlEventParser> = std::sync::OnceLock::new();
        STANDARD.get_or_init(|| {
            let mut rows = [None; 256];
            for (kind, operation, v2) in row_kinds() {
                rows[usize::from(kind)] = Some(RowsLayout {
                    operation,
                    id_width: 6,
                    parse_images: if v2 {
                        parse_v2_row_images
                    } else {
                        parse_row_images
                    },
                    standard: true,
                });
            }
            Self {
                table_id_width: 6,
                query_post_header: 13,
                rotate_post_header: 8,
                rows,
            }
        })
    }
    pub(crate) fn table_map<'a>(&self, body: &'a [u8]) -> Result<TableMapEvent<'a>> {
        let mut input = Cursor::new(body);
        let id = input.le(self.table_id_width)?;
        let flags = input.le(2)? as u16;
        let schema = input.name()?;
        let table = input.name()?;
        let mut parsed = TableMapEvent::new(id, flags, schema, table, input.remaining());
        if self.table_id_width == 6 {
            parsed.standard_body = Some(body);
        }
        Ok(parsed)
    }
    pub(crate) fn rows<'a>(&self, kind: u8, body: &'a [u8]) -> Result<RowsEvent<'a>> {
        let layout = self.rows[usize::from(kind)].ok_or_else(|| {
            Error::Unsupported(format!("ROWS event {kind} not advertised by this FDE"))
        })?;
        let mut input = Cursor::new(body);
        let id = input.le(layout.id_width)?;
        let flags = input.le(2)? as u16;
        let mut parsed = RowsEvent::new(
            id,
            flags,
            layout.operation,
            input.remaining(),
            layout.parse_images,
        );
        if layout.standard {
            parsed.standard_body = Some((kind, body));
        }
        Ok(parsed)
    }
}
impl EventParser for MysqlEventParser {
    fn parse<'a>(&self, raw: EventView<'a>) -> Result<ParsedEvent<'a>> {
        Ok(match raw.event_type {
            event::TABLE_MAP_EVENT => ParsedEvent::TableMap(self.table_map(raw.data)?),
            event::WRITE_ROWS_EVENT_V1
            | event::UPDATE_ROWS_EVENT_V1
            | event::DELETE_ROWS_EVENT_V1
            | event::WRITE_ROWS_EVENT
            | event::UPDATE_ROWS_EVENT
            | event::DELETE_ROWS_EVENT => ParsedEvent::Rows(self.rows(raw.event_type, raw.data)?),
            event::QUERY_EVENT => {
                ParsedEvent::Control(parse_query(raw.data, self.query_post_header)?)
            }
            event::ROTATE_EVENT => {
                ParsedEvent::Control(parse_rotate(raw.data, self.rotate_post_header)?)
            }
            _ => ParsedEvent::Unclassified(raw),
        })
    }
}
fn id_width(header: usize, other: usize) -> Result<usize> {
    match header.checked_sub(other) {
        Some(width @ (4 | 6)) => Ok(width),
        _ => Err(Error::Unsupported(format!(
            "TABLE_MAP/ROWS post-header length {header}"
        ))),
    }
}
fn row_kinds() -> [(u8, Operation, bool); 6] {
    [
        (event::WRITE_ROWS_EVENT_V1, Operation::Insert, false),
        (event::UPDATE_ROWS_EVENT_V1, Operation::Update, false),
        (event::DELETE_ROWS_EVENT_V1, Operation::Delete, false),
        (event::WRITE_ROWS_EVENT, Operation::Insert, true),
        (event::UPDATE_ROWS_EVENT, Operation::Update, true),
        (event::DELETE_ROWS_EVENT, Operation::Delete, true),
    ]
}
pub fn parse_row_images(operation: Operation, body: &[u8]) -> Result<RowImages<'_>> {
    let mut input = Cursor::new(body);
    let column_count = input.lenenc()?;
    if column_count == 0 || column_count > 4096 {
        return Err(invalid("ROWS column count outside 1..4096"));
    }
    let before_columns = input.bitmap(column_count)?;
    let after_columns = if operation == Operation::Update {
        input.bitmap(column_count)?
    } else {
        before_columns
    };
    Ok(RowImages {
        column_count,
        before_columns,
        after_columns,
        rows: input.remaining(),
    })
}
pub fn parse_v2_row_images(operation: Operation, body: &[u8]) -> Result<RowImages<'_>> {
    let mut input = Cursor::new(body);
    let len = input.le(2)? as usize;
    input.take(
        len.checked_sub(2)
            .ok_or_else(|| invalid("extra-data length < 2"))?,
    )?;
    parse_row_images(operation, input.remaining())
}
pub(crate) fn parse_query(body: &[u8], post_header: usize) -> Result<EventAction<'_>> {
    use mysql_replication::events::{Event, QueryEvent};
    let mut event = QueryEvent::with_post_header(post_header);
    event.decode(body)?;
    if event.error_code != 0 {
        return Err(invalid("QUERY event contains server error"));
    }
    let query = std::str::from_utf8(event.query)?
        .trim()
        .trim_end_matches(';')
        .trim();
    if query.eq_ignore_ascii_case("BEGIN") {
        Ok(EventAction::Begin)
    } else if query.eq_ignore_ascii_case("COMMIT") {
        Ok(EventAction::Commit)
    } else if query.eq_ignore_ascii_case("ROLLBACK") {
        Ok(EventAction::Rollback)
    } else {
        Err(Error::Statement(query.chars().take(200).collect()))
    }
}
pub(crate) fn parse_rotate(body: &[u8], post_header: usize) -> Result<EventAction<'_>> {
    let mut input = Cursor::new(body);
    let position = Cursor::new(input.take(post_header)?).le(8)?;
    let file = std::str::from_utf8(input.remaining())?;
    if file.is_empty() || position < 4 || position > u64::from(u32::MAX) {
        return Err(invalid("invalid ROTATE file/position"));
    }
    Ok(EventAction::Rotate { file, position })
}
