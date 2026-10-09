use crate::{
    Result,
    engine::checkpoint::State,
    engine::transaction::TransactionBuffer,
    error::invalid,
    protocol::mysql::event_type as event,
    protocol::mysql::events::{EventParser, ParsedEvent},
    protocol::mysql::file::EventView,
    protocol::mysql::version::{
        EventAction, FormatDescription, RowDecoder, VersionAdapter, VersionRegistry,
    },
};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Single-source transaction engine; adapters own version-specific decoding.
pub struct Processor {
    decoder: Option<Box<dyn RowDecoder>>,
    event_parser: Option<Box<dyn EventParser>>,
    adapter: Option<Arc<dyn VersionAdapter>>,
    registry: VersionRegistry,
    selected_adapter: Option<String>,
    transaction: TransactionBuffer,
    state: State,
    state_path: Option<PathBuf>,
}
impl Processor {
    pub fn new(
        state: State,
        state_path: Option<PathBuf>,
        transaction_budget: usize,
    ) -> Result<Self> {
        Self::with_registry(
            state,
            state_path,
            transaction_budget,
            VersionRegistry::default(),
            None,
        )
    }
    pub fn with_registry(
        state: State,
        state_path: Option<PathBuf>,
        transaction_budget: usize,
        registry: VersionRegistry,
        selected_adapter: Option<String>,
    ) -> Result<Self> {
        state.validate()?;
        Ok(Self {
            decoder: None,
            event_parser: None,
            adapter: None,
            registry,
            selected_adapter,
            transaction: TransactionBuffer::new(transaction_budget),
            state,
            state_path,
        })
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    pub fn finish(&self) -> Result<()> {
        if self.decoder.is_none() {
            return Err(invalid("stream ended without FORMAT_DESCRIPTION"));
        }
        if self.transaction.is_active() {
            return Err(invalid(
                "stream ended inside a transaction; checkpoint was not advanced",
            ));
        }
        Ok(())
    }
    pub fn process(&mut self, event: EventView<'_>, output: &mut impl Write) -> Result<()> {
        self.process_inner(event, |tx, state, path| tx.commit(output, state, path))
    }
    pub fn process_to(
        &mut self,
        event: EventView<'_>,
        source_id: &str,
        sink: &dyn crate::sink::TransactionSink,
    ) -> Result<()> {
        self.process_inner(event, |tx, state, path| {
            tx.commit_with(state, path, |changes| {
                sink.commit(source_id, state, changes)
            })
        })
    }
    fn process_inner(
        &mut self,
        event: EventView<'_>,
        commit: impl FnOnce(&mut TransactionBuffer, &State, Option<&Path>) -> Result<()>,
    ) -> Result<()> {
        if event.event_type != event::FORMAT_DESCRIPTION_EVENT
            && event.event_type != event::ROTATE_EVENT
            && self.decoder.is_none()
        {
            return Err(invalid("event before FORMAT_DESCRIPTION"));
        }
        if event.log_pos != 0
            && !matches!(
                event.event_type,
                event::HEARTBEAT_EVENT | event::HEARTBEAT_LOG_EVENT_V2
            )
        {
            self.state.position = u64::from(event.log_pos);
        }
        if event.event_type == event::FORMAT_DESCRIPTION_EVENT {
            if self.transaction.is_active() {
                return Err(invalid("FORMAT_DESCRIPTION inside transaction"));
            }
            let format = FormatDescription::parse(event.data)?;
            // Select using the file's producer version, including historical files
            // being replayed after an upstream server upgrade.
            let adapter = self
                .registry
                .resolve(&format.server, self.selected_adapter.as_deref())?;
            adapter.validate_format(&format)?;
            let parser = adapter.create_event_parser(&format)?;
            let decoder = adapter.create_decoder(self.state.tables.clone(), &format)?;
            self.event_parser = Some(parser);
            self.decoder = Some(decoder);
            self.adapter = Some(adapter);
            return Ok(());
        }
        let parsed = if let Some(parser) = &self.event_parser {
            parser.parse(event)?
        } else {
            // A fake ROTATE can precede the first FDE in a v4 dump stream.
            ParsedEvent::Control(crate::protocol::mysql::version::classify_mysql_event(
                event,
            )?)
        };
        let action = match parsed {
            ParsedEvent::TableMap(table_map) => {
                return self
                    .decoder
                    .as_mut()
                    .ok_or_else(|| invalid("TABLE_MAP without decoder"))?
                    .table_map_event(table_map);
            }
            ParsedEvent::Rows(rows) => {
                let tx = &mut self.transaction;
                return self
                    .decoder
                    .as_mut()
                    .ok_or_else(|| invalid("ROWS without decoder"))?
                    .rows_event(rows, &mut |change| tx.push(change));
            }
            ParsedEvent::Control(action) => action,
            ParsedEvent::Unclassified(raw) => self
                .adapter
                .as_ref()
                .ok_or_else(|| invalid("event without adapter"))?
                .classify(raw)?,
        };
        match action {
            EventAction::Rotate { file, position } => {
                if self.transaction.is_active() {
                    return Err(invalid("ROTATE inside transaction"));
                }
                self.state.file = file.into();
                self.state.position = position;
                if let Some(decoder) = &mut self.decoder {
                    decoder.clear_table_maps();
                }
            }
            EventAction::Begin => self.transaction.begin()?,
            EventAction::Commit => commit(
                &mut self.transaction,
                &self.state,
                self.state_path.as_deref(),
            )?,
            EventAction::Rollback => {
                self.transaction.rollback();
                if let Some(path) = &self.state_path {
                    self.state.save(path)?;
                }
            }
            EventAction::TableMap | EventAction::Rows => {
                return Err(invalid(
                    "EventParser must normalize TABLE_MAP/ROWS into typed views",
                ));
            }
            EventAction::GroupStart if self.transaction.is_active() => {
                return Err(invalid("new GTID inside unfinished transaction"));
            }
            EventAction::GroupStart | EventAction::Ignore => {}
        }
        Ok(())
    }
}
