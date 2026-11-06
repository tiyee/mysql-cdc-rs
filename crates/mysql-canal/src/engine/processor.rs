use crate::{
    Result,
    config::RowImageMode,
    engine::checkpoint::State,
    engine::transaction::TransactionBuffer,
    error::invalid,
    protocol::mysql::event_type as event,
    protocol::mysql::file::EventView,
    protocol::mysql::version::{EventAction, VersionRegistry},
};
use std::path::{Path, PathBuf};

/// Per-source transaction engine; adapters own version-specific decoding.
pub struct Processor {
    converter: super::converter::EventConverter,
    transaction: TransactionBuffer,
    state: State,
    state_path: Option<PathBuf>,
    group_open: bool,
    pending_rotation: bool,
}
impl Processor {
    pub fn new(
        state: State,
        state_path: Option<PathBuf>,
        transaction_budget: usize,
        registry: VersionRegistry,
        selected_adapter: Option<String>,
        row_image: RowImageMode,
    ) -> Result<Self> {
        state.validate()?;
        Ok(Self {
            converter: super::converter::EventConverter::new(
                state.tables.clone(),
                registry,
                selected_adapter,
                row_image,
            ),
            transaction: TransactionBuffer::new(transaction_budget),
            state,
            state_path,
            group_open: false,
            pending_rotation: false,
        })
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    pub fn finish(&self) -> Result<()> {
        if !self.converter.is_initialized() {
            return Err(invalid("stream ended without FORMAT_DESCRIPTION"));
        }
        if self.group_open || self.transaction.is_active() {
            return Err(invalid(
                "stream ended inside a transaction; checkpoint was not advanced",
            ));
        }
        Ok(())
    }
    /// Converts a protocol event before transaction delivery; this does not expose
    /// uncommitted rows to the transaction sink.
    pub fn process_binlog<M: mysql_replication::events::BinlogMode>(
        &mut self,
        event: &mysql_replication::BinlogEvent<'_, M>,
        sink: &dyn crate::sink::TransactionSink,
    ) -> Result<()> {
        self.process(event.frame().into(), sink)
    }
    pub fn process(
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
        if (self.group_open || self.transaction.is_active())
            && event.event_type == event::FORMAT_DESCRIPTION_EVENT
        {
            return Err(invalid("FORMAT_DESCRIPTION inside transaction"));
        }
        if self.pending_rotation
            && !matches!(
                event.event_type,
                event::FORMAT_DESCRIPTION_EVENT
                    | event::ROTATE_EVENT
                    | event::HEARTBEAT_EVENT
                    | event::HEARTBEAT_LOG_EVENT_V2
            )
        {
            return Err(invalid("event before rotated file FORMAT_DESCRIPTION"));
        }
        let tx = &mut self.transaction;
        let action = self
            .converter
            .convert(event, &mut |change| tx.push(change))?;
        if event.event_type == event::FORMAT_DESCRIPTION_EVENT && self.pending_rotation {
            if let Some(path) = &self.state_path {
                self.state.save(path)?;
            }
            self.pending_rotation = false;
        }
        match action {
            EventAction::Rotate { file, position } => {
                if self.group_open || self.transaction.is_active() {
                    return Err(invalid("ROTATE inside transaction"));
                }
                if self.pending_rotation && self.state.file != file {
                    return Err(invalid("ROTATE before rotated file FORMAT_DESCRIPTION"));
                }
                self.pending_rotation |= self.state.file != file;
                self.state.file = file.into();
                self.state.position = position;
                self.converter.clear_table_maps();
            }
            EventAction::Begin => self.transaction.begin()?,
            EventAction::Commit => {
                commit(
                    &mut self.transaction,
                    &self.state,
                    self.state_path.as_deref(),
                )?;
                self.group_open = false;
            }
            EventAction::Rollback => {
                self.transaction.rollback();
                self.group_open = false;
                if let Some(path) = &self.state_path {
                    self.state.save(path)?;
                }
            }
            EventAction::GroupStart if self.group_open || self.transaction.is_active() => {
                return Err(invalid("new GTID inside unfinished transaction"));
            }
            EventAction::GroupStart => self.group_open = true,
            EventAction::Ignore => {}
        }
        Ok(())
    }
}
