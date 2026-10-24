//! Version adapters are statically linked plugins, selected once per connection/FDE.
//! They are object safe; row values still borrow the input event across this boundary.
use super::events::{EventParser, MysqlEventParser, RowsEvent, TableMapEvent};
use super::{decoder::Decoder, event_type as event, file::EventView, wire::Cursor};
use crate::{Change, Error, Result, TableMeta, config::RowImageMode, error::invalid};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerVersion {
    pub raw: String,
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}
impl ServerVersion {
    pub fn parse(raw: &str) -> Result<Self> {
        let (major, minor, patch) = mysql_replication::version::parse_server_version(raw)?;
        Ok(Self {
            raw: raw.into(),
            major,
            minor,
            patch,
        })
    }
}

pub struct FormatDescription<'a> {
    pub server: ServerVersion,
    pub binlog_version: u16,
    pub header_length: u8,
    pub post_header_lengths: &'a [u8],
}
impl<'a> FormatDescription<'a> {
    pub fn parse(body: &'a [u8]) -> Result<Self> {
        let mut cursor = Cursor::new(body);
        let binlog_version = cursor.le(2)? as u16;
        let version = cursor.take(50)?;
        let end = version
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(version.len());
        let server = ServerVersion::parse(std::str::from_utf8(&version[..end])?)?;
        cursor.take(4)?;
        let header_length = cursor.byte()?;
        Ok(Self {
            server,
            binlog_version,
            header_length,
            post_header_lengths: cursor.remaining(),
        })
    }
}

pub trait RowDecoder: Send {
    fn clear_table_maps(&mut self);
    fn table_map_event(&mut self, event: TableMapEvent<'_>) -> Result<()> {
        self.table_map(event.standard_body.ok_or_else(|| {
            Error::Unsupported(
                "custom TABLE_MAP layout requires RowDecoder::table_map_event".into(),
            )
        })?)
    }
    fn rows_event(
        &mut self,
        event: RowsEvent<'_>,
        emit: &mut dyn FnMut(Change<'_>) -> Result<()>,
    ) -> Result<()> {
        let (kind, body) = event.standard_body.ok_or_else(|| {
            Error::Unsupported("custom ROWS layout requires RowDecoder::rows_event".into())
        })?;
        self.rows(kind, body, emit)
    }
    fn table_map(&mut self, body: &[u8]) -> Result<()>;
    fn rows(
        &mut self,
        event_type: u8,
        body: &[u8],
        emit: &mut dyn FnMut(Change<'_>) -> Result<()>,
    ) -> Result<()>;
}
impl<M: crate::CdcMode> RowDecoder for Decoder<M> {
    fn table_map_event(&mut self, event: TableMapEvent<'_>) -> Result<()> {
        Decoder::table_map_event(self, event)
    }
    fn rows_event(
        &mut self,
        event: RowsEvent<'_>,
        emit: &mut dyn FnMut(Change<'_>) -> Result<()>,
    ) -> Result<()> {
        Decoder::rows_event(self, event, emit)
    }
    fn clear_table_maps(&mut self) {
        self.clear_table_maps();
    }
    fn table_map(&mut self, body: &[u8]) -> Result<()> {
        self.table_map(body)
    }
    fn rows(
        &mut self,
        event_type: u8,
        body: &[u8],
        emit: &mut dyn FnMut(Change<'_>) -> Result<()>,
    ) -> Result<()> {
        self.rows(event_type, body, emit)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum EventAction<'a> {
    TableMap,
    Rows,
    Begin,
    Commit,
    Rollback,
    GroupStart,
    Ignore,
    Rotate { file: &'a str, position: u64 },
}

pub trait VersionAdapter: Send + Sync {
    fn name(&self) -> &str;
    fn supports(&self, version: &ServerVersion) -> bool;
    fn status_query(&self) -> &str;
    fn validate_server(
        &self,
        variables: &BTreeMap<String, String>,
        consumer_id: u32,
    ) -> Result<()> {
        validate_server_variables(variables, consumer_id)
    }
    /// Existing adapters retain their MINIMAL validation; FULL is opt-in.
    fn validate_server_with_row_image(
        &self,
        variables: &BTreeMap<String, String>,
        consumer_id: u32,
        row_image: RowImageMode,
    ) -> Result<()> {
        if row_image != RowImageMode::Minimal {
            return Err(Error::Unsupported(format!(
                "adapter {} does not support FULL row images",
                self.name()
            )));
        }
        self.validate_server(variables, consumer_id)
    }
    fn validate_format(&self, format: &FormatDescription<'_>) -> Result<()> {
        validate_mysql_v4(format)
    }
    /// Bind a structural parser once per file, using the file's FDE layout.
    fn create_event_parser(&self, format: &FormatDescription<'_>) -> Result<Box<dyn EventParser>> {
        Ok(Box::new(MysqlEventParser::from_format(format)?))
    }
    fn create_decoder(
        &self,
        tables: Vec<TableMeta>,
        _format: &FormatDescription<'_>,
    ) -> Result<Box<dyn RowDecoder>> {
        Ok(Box::new(Decoder::new(tables)?))
    }
    /// Existing custom decoders must explicitly opt into FULL images.
    fn create_decoder_with_row_image(
        &self,
        tables: Vec<TableMeta>,
        format: &FormatDescription<'_>,
        row_image: RowImageMode,
    ) -> Result<Box<dyn RowDecoder>> {
        if row_image != RowImageMode::Minimal {
            return Err(Error::Unsupported(format!(
                "adapter {} does not support FULL row images",
                self.name()
            )));
        }
        self.create_decoder(tables, format)
    }
    fn classify<'a>(&self, event: EventView<'a>) -> Result<EventAction<'a>> {
        classify_mysql_event(event)
    }
}

#[derive(Clone)]
pub struct VersionRegistry {
    adapters: Vec<Arc<dyn VersionAdapter>>,
}
impl VersionRegistry {
    pub fn empty() -> Self {
        Self {
            adapters: Vec::new(),
        }
    }
    pub fn register(&mut self, adapter: Arc<dyn VersionAdapter>) -> Result<()> {
        if adapter.name().is_empty() || self.adapters.iter().any(|a| a.name() == adapter.name()) {
            return Err(Error::Schema("empty/duplicate version adapter name".into()));
        }
        self.adapters.push(adapter);
        Ok(())
    }
    pub fn resolve(
        &self,
        version: &ServerVersion,
        selected: Option<&str>,
    ) -> Result<Arc<dyn VersionAdapter>> {
        let matches: Vec<_> = self
            .adapters
            .iter()
            .filter(|a| selected.is_none_or(|name| name == a.name()) && a.supports(version))
            .collect();
        match matches.as_slice() {
            [adapter] => Ok(Arc::clone(adapter)),
            [] => Err(Error::Unsupported(format!(
                "server {} / adapter {}; register a matching VersionAdapter{}",
                version.raw,
                selected.unwrap_or("auto"),
                if (version.major, version.minor) == (5, 8) {
                    "; Oracle MySQL has no 5.8 release series"
                } else {
                    ""
                }
            ))),
            _ => Err(Error::Schema(format!(
                "ambiguous adapters for {}; set source.adapter explicitly",
                version.raw
            ))),
        }
    }
}
impl Default for VersionRegistry {
    fn default() -> Self {
        let adapters: Vec<Arc<dyn VersionAdapter>> = [
            (5, 7, "mysql57"),
            (8, 0, "mysql80"),
            (8, 1, "mysql81"),
            (8, 2, "mysql82"),
            (8, 3, "mysql83"),
            (8, 4, "mysql84"),
            (9, 0, "mysql90"),
            (9, 7, "mysql97"),
        ]
        .into_iter()
        .map(|(major, minor, name)| {
            Arc::new(MysqlVersion { major, minor, name }) as Arc<dyn VersionAdapter>
        })
        .collect();
        Self { adapters }
    }
}
struct MysqlVersion {
    major: u32,
    minor: u32,
    name: &'static str,
}
impl VersionAdapter for MysqlVersion {
    fn validate_server_with_row_image(
        &self,
        variables: &BTreeMap<String, String>,
        consumer_id: u32,
        row_image: RowImageMode,
    ) -> Result<()> {
        validate_server_variables_with_row_image(variables, consumer_id, row_image)
    }
    fn create_decoder_with_row_image(
        &self,
        tables: Vec<TableMeta>,
        _format: &FormatDescription<'_>,
        row_image: RowImageMode,
    ) -> Result<Box<dyn RowDecoder>> {
        match row_image {
            RowImageMode::Minimal => Ok(Box::new(Decoder::<crate::Minimal>::for_mode(tables)?)),
            RowImageMode::Full => Ok(Box::new(Decoder::<crate::Full>::for_mode(tables)?)),
        }
    }
    fn name(&self) -> &str {
        self.name
    }
    fn supports(&self, version: &ServerVersion) -> bool {
        version.major == self.major
            && version.minor == self.minor
            && !version.raw.to_ascii_lowercase().contains("mariadb")
    }
    fn status_query(&self) -> &str {
        if self.major > 8 || self.major == 8 && self.minor >= 4 {
            "SHOW BINARY LOG STATUS"
        } else {
            "SHOW MASTER STATUS"
        }
    }
}

pub fn validate_server_variables(
    variables: &BTreeMap<String, String>,
    consumer_id: u32,
) -> Result<()> {
    validate_server_variables_with_row_image(variables, consumer_id, RowImageMode::Minimal)
}

pub fn validate_server_variables_with_row_image(
    variables: &BTreeMap<String, String>,
    consumer_id: u32,
    row_image: RowImageMode,
) -> Result<()> {
    for (name, expected) in [
        ("log_bin", "ON"),
        ("binlog_format", "ROW"),
        ("binlog_row_image", row_image.server_value()),
    ] {
        if !variables
            .get(name)
            .is_some_and(|v| v.eq_ignore_ascii_case(expected))
        {
            return Err(Error::Unsupported(format!("requires {name}={expected}")));
        }
    }
    if variables
        .get("binlog_transaction_compression")
        .is_some_and(|v| !v.eq_ignore_ascii_case("OFF"))
        || variables
            .get("binlog_row_value_options")
            .is_some_and(|v| !v.is_empty())
    {
        return Err(Error::Unsupported(
            "disable binlog_transaction_compression and binlog_row_value_options".into(),
        ));
    }
    let upstream_id: u32 = variables
        .get("server_id")
        .and_then(|v| v.parse().ok())
        .filter(|id| *id > 0)
        .ok_or_else(|| invalid("missing/invalid upstream server_id"))?;
    if consumer_id == upstream_id {
        return Err(Error::Schema(
            "CDC server_id must differ from source server_id".into(),
        ));
    }
    Ok(())
}

pub fn validate_mysql_v4(format: &FormatDescription<'_>) -> Result<()> {
    if format.binlog_version != 4 || format.header_length != 19 {
        return Err(Error::Unsupported(
            "requires binlog v4 with 19-byte common header".into(),
        ));
    }
    for (kind, length) in [
        (event::QUERY_EVENT, 13),
        (event::ROTATE_EVENT, 8),
        (event::TABLE_MAP_EVENT, 8),
        (event::WRITE_ROWS_EVENT, 10),
        (event::UPDATE_ROWS_EVENT, 10),
        (event::DELETE_ROWS_EVENT, 10),
    ] {
        if format.post_header_lengths.get(usize::from(kind) - 1) != Some(&length) {
            return Err(Error::Unsupported(format!(
                "post-header length for event {kind}"
            )));
        }
    }
    for kind in [
        event::WRITE_ROWS_EVENT_V1,
        event::UPDATE_ROWS_EVENT_V1,
        event::DELETE_ROWS_EVENT_V1,
    ] {
        if !matches!(
            format.post_header_lengths.get(usize::from(kind) - 1),
            Some(0 | 8)
        ) {
            return Err(Error::Unsupported(format!(
                "post-header length for legacy event {kind}"
            )));
        }
    }
    Ok(())
}

pub fn classify_mysql_event(event: EventView<'_>) -> Result<EventAction<'_>> {
    Ok(match event.event_type {
        event::ROTATE_EVENT => super::events::parse_rotate(event.data, 8)?,
        event::QUERY_EVENT => super::events::parse_query(event.data, 13)?,
        event::XID_EVENT => {
            if event.data.len() != 8 {
                return Err(invalid("XID length != 8"));
            }
            EventAction::Commit
        }
        event::TABLE_MAP_EVENT => EventAction::TableMap,
        event::WRITE_ROWS_EVENT_V1
        | event::UPDATE_ROWS_EVENT_V1
        | event::DELETE_ROWS_EVENT_V1
        | event::WRITE_ROWS_EVENT
        | event::UPDATE_ROWS_EVENT
        | event::DELETE_ROWS_EVENT => EventAction::Rows,
        event::GTID_EVENT | event::ANONYMOUS_GTID_EVENT | event::GTID_TAGGED_LOG_EVENT => {
            EventAction::GroupStart
        }
        event::STOP_EVENT
        | event::HEARTBEAT_EVENT
        | event::IGNORABLE_EVENT
        | event::ROWS_QUERY_EVENT
        | event::PREVIOUS_GTIDS_EVENT
        | event::HEARTBEAT_LOG_EVENT_V2 => EventAction::Ignore,
        event::XA_PREPARE_LOG_EVENT => return Err(Error::Unsupported("XA transactions".into())),
        event::PARTIAL_UPDATE_ROWS_EVENT => {
            return Err(Error::Unsupported(
                "partial JSON row images; disable binlog_row_value_options".into(),
            ));
        }
        event::TRANSACTION_PAYLOAD_EVENT => {
            return Err(Error::Unsupported(
                "compressed transactions; disable binlog_transaction_compression".into(),
            ));
        }
        _ if event.flags & 0x80 != 0 => EventAction::Ignore,
        other => {
            return Err(Error::Unsupported(format!(
                "non-ignorable binlog event {other}"
            )));
        }
    })
}
