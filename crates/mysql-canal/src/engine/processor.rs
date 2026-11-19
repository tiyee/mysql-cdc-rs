use crate::{
    Result,
    config::RowImageMode,
    engine::checkpoint::{State, read_checkpoint, save_checkpoint},
    engine::transaction::TransactionBuffer,
    error::invalid,
    protocol::mysql::event_type as event,
    protocol::mysql::file::EventView,
    protocol::mysql::version::{EventAction, VersionRegistry},
};
use std::path::PathBuf;

/// Per-source transaction engine; adapters own version-specific decoding.
pub struct Processor {
    converter: super::converter::EventConverter,
    transaction: TransactionBuffer,
    state: State,
    state_path: Option<PathBuf>,
    checkpoint: (String, u64),
    track_ddl: bool,
    schema_history: Vec<State>,
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
        let mut schema_history = Vec::new();
        if let Some(path) = &state_path
            && path.try_exists()?
        {
            let (saved, history) = read_checkpoint(path)?;
            if !history.is_empty() && saved != state {
                return Err(invalid("processor state differs from versioned checkpoint"));
            }
            schema_history = history;
        }
        Ok(Self {
            converter: super::converter::EventConverter::new(
                state.tables.clone(),
                registry,
                selected_adapter,
                row_image,
            ),
            transaction: TransactionBuffer::new(transaction_budget),
            checkpoint: (state.file.clone(), state.position),
            state,
            state_path,
            group_open: false,
            track_ddl: false,
            schema_history,
            pending_rotation: false,
        })
    }
    /// Enables supported standalone MySQL ALTER statements from this checkpoint.
    pub fn with_ddl_tracking(mut self) -> Self {
        if self.schema_history.is_empty() {
            self.schema_history.push(self.state.clone());
        }
        self.track_ddl = true;
        self
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
        let mut position = self.state.position;
        if event.log_pos != 0
            && !matches!(
                event.event_type,
                event::HEARTBEAT_EVENT | event::HEARTBEAT_LOG_EVENT_V2
            )
        {
            let next = u64::from(event.log_pos);
            if !self.schema_history.is_empty()
                && !matches!(
                    event.event_type,
                    event::FORMAT_DESCRIPTION_EVENT | event::ROTATE_EVENT
                )
                && (next <= position
                    || (self.state.file == self.checkpoint.0 && next <= self.checkpoint.1))
            {
                return Err(invalid("non-increasing event position in versioned stream"));
            }
            position = next;
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
        let result = self
            .converter
            .convert(event, &mut |change| self.transaction.push(change));
        if matches!(result, Err(crate::Error::Statement(_)))
            && self.track_ddl
            && event.event_type == event::QUERY_EVENT
            && let Some(tables) = self.converter.evolved_tables(event)?
        {
            return self.commit_ddl(event.log_pos, tables, sink);
        }
        let action = result?;
        if !self.schema_history.is_empty()
            && event.log_pos == 0
            && matches!(action, EventAction::Commit | EventAction::Rollback)
        {
            return Err(invalid("tracked commit boundary has no end position"));
        }
        let previous_position = self.state.position;
        self.state.position = position;
        if event.event_type == event::FORMAT_DESCRIPTION_EVENT && self.pending_rotation {
            if !self.schema_history.is_empty() {
                if self
                    .schema_history
                    .iter()
                    .any(|s| s.file == self.state.file)
                {
                    return Err(invalid("cannot reuse a binlog filename in schema history"));
                }
                if position < previous_position {
                    return Err(invalid(
                        "rotated file format ends before its starting position",
                    ));
                }
                let mut baseline = self.state.clone();
                baseline.position = previous_position;
                self.schema_history.push(baseline);
            }
            if let Some(path) = &self.state_path {
                save_checkpoint(path, &self.state, &self.schema_history)?;
                self.record_checkpoint();
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
                let state = &self.state;
                self.transaction.commit_with(state, None, |changes| {
                    sink.commit(&state.source_id, state, changes)?;
                    if let Some(path) = &self.state_path {
                        save_checkpoint(path, state, &self.schema_history)?;
                    }
                    Ok(())
                })?;
                if self.state_path.is_some() {
                    self.record_checkpoint();
                }
                self.group_open = false;
            }
            EventAction::Rollback => {
                self.transaction.rollback();
                self.group_open = false;
                if let Some(path) = &self.state_path {
                    save_checkpoint(path, &self.state, &self.schema_history)?;
                    self.record_checkpoint();
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
    fn commit_ddl(
        &mut self,
        end: u32,
        tables: Vec<crate::TableMeta>,
        sink: &dyn crate::sink::TransactionSink,
    ) -> Result<()> {
        if self.transaction.is_active() || end == 0 {
            return Err(invalid(
                "tracked DDL requires a standalone commit with an end position",
            ));
        }
        let mut next = self.state.clone();
        next.position = end.into();
        next.tables = tables;
        let converter = self.converter.with_tables(next.tables.clone())?;
        sink.commit(&next.source_id, &next, &[])?;
        self.schema_history.push(next.clone());
        if let Some(path) = &self.state_path
            && let Err(error) = save_checkpoint(path, &next, &self.schema_history)
        {
            self.schema_history.pop();
            return Err(error);
        }
        self.converter = converter;
        self.state = next;
        self.group_open = false;
        self.record_checkpoint();
        Ok(())
    }
    fn record_checkpoint(&mut self) {
        if self.checkpoint.0 != self.state.file {
            self.checkpoint.0.clone_from(&self.state.file);
        }
        self.checkpoint.1 = self.state.position;
    }
}
