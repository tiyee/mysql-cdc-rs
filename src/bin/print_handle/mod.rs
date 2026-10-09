use mysql_replication::{
    Event, EventHandler, OwnedChange, Result,
    engine::checkpoint::State,
    sink::{JsonLinesSink, TransactionSink},
};
use std::io::{BufWriter, Stdout};

/// The CLI's default listener, printing one JSON object per changed row.
pub struct PrintHandle(JsonLinesSink<BufWriter<Stdout>>);

impl PrintHandle {
    pub fn new() -> Self {
        Self(JsonLinesSink::new(BufWriter::with_capacity(
            256 * 1024,
            std::io::stdout(),
        )))
    }
}

impl EventHandler for PrintHandle {
    fn handle(&self, event: Event<'_>) -> Result<()> {
        self.0
            .commit(event.source_id, event.checkpoint, event.changes)
    }
}

impl TransactionSink for PrintHandle {
    fn commit(&self, source_id: &str, checkpoint: &State, changes: &[OwnedChange]) -> Result<()> {
        self.handle(Event {
            source_id,
            checkpoint,
            changes,
        })
    }
}
