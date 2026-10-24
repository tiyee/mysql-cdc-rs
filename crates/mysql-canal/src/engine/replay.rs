//! Offline CDC replay using a checkpoint's historical schema and commit boundary.

use crate::{
    Error, Result,
    config::RowImageMode,
    engine::{Processor, checkpoint::State},
    error::invalid,
    protocol::mysql::{event_type as event, file::BinlogFile, version::VersionRegistry},
    sink::TransactionSink,
};
use std::{fs::File, io::BufReader, path::Path};

/// Replays one binlog file into an explicit sink without persisting checkpoints.
///
/// Earlier events are skipped except for the FDE needed to select the file's
/// adapter. The saved schema is never replaced by current server metadata.
pub struct Replay {
    processor: Processor,
}

impl Replay {
    pub fn new(
        state: State,
        transaction_budget: usize,
        registry: VersionRegistry,
        selected_adapter: Option<String>,
        row_image: RowImageMode,
    ) -> Result<Self> {
        Ok(Self {
            processor: Processor::new(
                state,
                None,
                transaction_budget,
                registry,
                selected_adapter,
                row_image,
            )?,
        })
    }

    /// Delivers committed transactions and rejects invalid or incomplete input.
    /// Earlier commits may have reached the sink when a later event fails.
    pub fn run(mut self, path: &Path, sink: &dyn TransactionSink) -> Result<()> {
        if path.file_name().and_then(|name| name.to_str())
            != Some(self.processor.state().file.as_str())
        {
            return Err(Error::Schema(
                "replay filename must match the state's binlog file".into(),
            ));
        }
        let start = self.processor.state().position;
        let mut file = BinlogFile::new(BufReader::new(File::open(path)?))?;
        let mut offset = 4u64;
        while let Some(event) = file.next_event()? {
            let next = u64::from(event.log_pos);
            if next <= offset {
                return Err(invalid("non-increasing offline event position"));
            }
            if offset < start && next > start {
                return Err(invalid("state position is not an event boundary"));
            }
            // Only the FDE is applied before the checkpoint's schema boundary.
            if event.event_type == event::FORMAT_DESCRIPTION_EVENT || offset >= start {
                self.processor.process(event, sink)?;
            }
            offset = next;
        }
        if offset < start {
            return Err(invalid("state position beyond EOF"));
        }
        self.processor.finish()
    }
}
