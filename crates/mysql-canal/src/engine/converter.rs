//! Explicit conversion from borrowed binlog views into lightweight CDC changes.
use crate::{
    Change, Result, TableMeta,
    config::RowImageMode,
    error::invalid,
    protocol::mysql::{
        event_type as event,
        events::{EventParser, ParsedEvent},
        file::EventView,
        version::{EventAction, FormatDescription, RowDecoder, VersionRegistry},
    },
};

/// Per-source conversion state. Transaction visibility and persistence belong to Processor.
pub struct EventConverter {
    row_image: RowImageMode,
    tables: Vec<TableMeta>,
    format_body: Vec<u8>,
    decoder: Option<Box<dyn RowDecoder>>,
    parser: Option<Box<dyn EventParser>>,
    registry: VersionRegistry,
    selected_adapter: Option<String>,
}
impl EventConverter {
    pub fn new(
        tables: Vec<TableMeta>,
        registry: VersionRegistry,
        selected_adapter: Option<String>,
        row_image: RowImageMode,
    ) -> Self {
        Self {
            row_image,
            tables,
            format_body: Vec::new(),
            decoder: None,
            parser: None,
            registry,
            selected_adapter,
        }
    }
    pub fn is_initialized(&self) -> bool {
        self.decoder.is_some()
    }
    pub fn clear_table_maps(&mut self) {
        if let Some(decoder) = &mut self.decoder {
            decoder.clear_table_maps();
        }
    }
    pub(super) fn evolved_tables(&self, event: EventView<'_>) -> Result<Option<Vec<TableMeta>>> {
        let Some(parser) = &self.parser else {
            return Ok(None);
        };
        let Some(query) = parser.ddl_query(event)? else {
            return Ok(None);
        };
        let format = FormatDescription::parse(&self.format_body)?;
        super::ddl::apply(&self.tables, query, &format.server)
    }

    pub(super) fn with_tables(&self, tables: Vec<TableMeta>) -> Result<Self> {
        let mut next = Self::new(
            tables,
            self.registry.clone(),
            self.selected_adapter.clone(),
            self.row_image,
        );
        next.set_format(&self.format_body)?;
        Ok(next)
    }

    fn set_format(&mut self, body: &[u8]) -> Result<()> {
        self.parser = None;
        self.decoder = None;
        let format = FormatDescription::parse(body)?;
        let adapter = self
            .registry
            .resolve(&format.server, self.selected_adapter.as_deref())?;
        adapter.validate_format(&format)?;
        let parser = adapter.create_event_parser(&format)?;
        let decoder = adapter.create_decoder(self.tables.clone(), &format, self.row_image)?;
        self.format_body.clear();
        self.format_body.extend_from_slice(body);
        self.parser = Some(parser);
        self.decoder = Some(decoder);
        Ok(())
    }

    /// Converts typed binlog input while preserving custom CDC layout adapters.
    pub fn convert_binlog<'a, M: mysql_replication::events::BinlogMode>(
        &mut self,
        event: &'a mysql_replication::BinlogEvent<'_, M>,
        emit: &mut dyn FnMut(Change<'_>) -> Result<()>,
    ) -> Result<EventAction<'a>> {
        self.convert(event.frame().into(), emit)
    }
    /// Event code dispatch happens once per event. Only selected row fields are decoded.
    pub fn convert<'a>(
        &mut self,
        event: EventView<'a>,
        emit: &mut dyn FnMut(Change<'_>) -> Result<()>,
    ) -> Result<EventAction<'a>> {
        if event.event_type == event::FORMAT_DESCRIPTION_EVENT {
            self.set_format(event.data)?;
            return Ok(EventAction::Ignore);
        }
        let parsed = if let Some(parser) = &self.parser {
            parser.parse(event)?
        } else if event.event_type == event::ROTATE_EVENT {
            ParsedEvent::Control(crate::protocol::mysql::version::classify_mysql_event(
                event,
            )?)
        } else {
            return Err(invalid("event before FORMAT_DESCRIPTION"));
        };
        match parsed {
            ParsedEvent::TableMap(table) => {
                self.decoder
                    .as_mut()
                    .ok_or_else(|| invalid("TABLE_MAP without decoder"))?
                    .table_map_event(table)?;
                Ok(EventAction::Ignore)
            }
            ParsedEvent::Rows(rows) => {
                self.decoder
                    .as_mut()
                    .ok_or_else(|| invalid("ROWS without decoder"))?
                    .rows_event(rows, emit)?;
                Ok(EventAction::Ignore)
            }
            ParsedEvent::Control(action) => Ok(action),
        }
    }
}
