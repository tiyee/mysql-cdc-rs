use crate::{
    engine::checkpoint::State,
    engine::transaction::TransactionBuffer,
    error::invalid,
    protocol::mysql::event_type as event,
    protocol::mysql::file::EventView,
    protocol::mysql::version::{EventAction, VersionRegistry},
    Result,
};
use std::{
    io::Write,
    path::{Path, PathBuf},
};

/// Per-source transaction engine; adapters own version-specific decoding.
pub struct Processor {
    converter: super::converter::EventConverter,
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
            converter: super::converter::EventConverter::new(
                state.tables.clone(),
                registry,
                selected_adapter,
            ),
            transaction: TransactionBuffer::new(transaction_budget),
            state,
            state_path,
        })
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    pub fn finish(&self) -> Result<()> {
        if !self.converter.is_initialized() {
            return Err(invalid("stream ended without FORMAT_DESCRIPTION"));
        }
        if self.transaction.is_active() {
            return Err(invalid(
                "stream ended inside a transaction; checkpoint was not advanced",
            ));
        }
        Ok(())
    }
    pub fn process(
        &mut self,
        event: EventView<'_>,
        output: &mut (impl Write + Send),
    ) -> Result<()> {
        self.process_to(event, &crate::sink::JsonLinesSink::new(output))
    }
    /// Converts a protocol event before transaction delivery; this does not expose
    /// uncommitted rows to the transaction sink.
    pub fn process_binlog(
        &mut self,
        event: &mysql_replication::BinlogEvent<'_>,
        sink: &dyn crate::sink::TransactionSink,
    ) -> Result<()> {
        self.process_to(event.frame().into(), sink)
    }
    pub fn process_to(
        &mut self,
        event: EventView<'_>,
        sink: &dyn crate::sink::TransactionSink,
    ) -> Result<()> {
        self.process_inner(event, |tx, state, path| {
            tx.commit_with(state, path, |changes| {
                sink.commit(&state.source_id, state, changes)
            })
        })
    }
    fn process_inner(
        &mut self,
        event: EventView<'_>,
        commit: impl FnOnce(&mut TransactionBuffer, &State, Option<&Path>) -> Result<()>,
    ) -> Result<()> {
        if event.log_pos != 0
            && !matches!(
                event.event_type,
                event::HEARTBEAT_EVENT | event::HEARTBEAT_LOG_EVENT_V2
            )
        {
            self.state.position = u64::from(event.log_pos);
        }
        if self.transaction.is_active() && event.event_type == event::FORMAT_DESCRIPTION_EVENT {
            return Err(invalid("FORMAT_DESCRIPTION inside transaction"));
        }
        let tx = &mut self.transaction;
        let action = self
            .converter
            .convert(event, &mut |change| tx.push(change))?;
        match action {
            EventAction::Rotate { file, position } => {
                if self.transaction.is_active() {
                    return Err(invalid("ROTATE inside transaction"));
                }
                self.state.file = file.into();
                self.state.position = position;
                self.converter.clear_table_maps();
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
