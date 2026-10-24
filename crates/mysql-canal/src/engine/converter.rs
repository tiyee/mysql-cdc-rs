//! Explicit conversion from borrowed binlog views into lightweight CDC changes.
use crate::{
    Change, Result, TableMeta,
    config::RowImageMode,
    error::invalid,
    protocol::mysql::{
        event_type as event,
        events::{EventParser, ParsedEvent},
        file::EventView,
        version::{EventAction, FormatDescription, RowDecoder, VersionAdapter, VersionRegistry},
    },
};
use std::sync::Arc;

/// Per-source conversion state. Transaction visibility and persistence belong to Processor.
pub struct EventConverter {
    row_image: RowImageMode,
    tables: Vec<TableMeta>,
    decoder: Option<Box<dyn RowDecoder>>,
    parser: Option<Box<dyn EventParser>>,
    adapter: Option<Arc<dyn VersionAdapter>>,
    registry: VersionRegistry,
    selected_adapter: Option<String>,
}
impl EventConverter {
    pub fn new(
        tables: Vec<TableMeta>,
        registry: VersionRegistry,
        selected_adapter: Option<String>,
    ) -> Self {
        Self::with_row_image(tables, registry, selected_adapter, RowImageMode::Minimal)
    }

    pub fn with_row_image(
        tables: Vec<TableMeta>,
        registry: VersionRegistry,
        selected_adapter: Option<String>,
        row_image: RowImageMode,
    ) -> Self {
        Self {
            row_image,
            tables,
            decoder: None,
            parser: None,
            adapter: None,
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
            let format = FormatDescription::parse(event.data)?;
            let adapter = self
                .registry
                .resolve(&format.server, self.selected_adapter.as_deref())?;
            adapter.validate_format(&format)?;
            let parser = adapter.create_event_parser(&format)?;
            let decoder = adapter.create_decoder_with_row_image(
                self.tables.clone(),
                &format,
                self.row_image,
            )?;
            self.parser = Some(parser);
            self.decoder = Some(decoder);
            self.adapter = Some(adapter);
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
            ParsedEvent::Unclassified(raw) => self
                .adapter
                .as_ref()
                .ok_or_else(|| invalid("event without adapter"))?
                .classify(raw),
        }
    }
}
