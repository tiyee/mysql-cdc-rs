//! Transaction sink contract and output implementations.
mod handler;
mod json_lines;
use crate::{OwnedChange, Result, engine::checkpoint::State};
pub use handler::TransactionHandler;
pub use json_lines::JsonLinesSink;

/// A successful commit acknowledges the complete transaction, before checkpoint
/// persistence. Implementations must be safe for concurrent source workers.
pub trait TransactionSink: Send + Sync {
    fn commit(&self, source_id: &str, state: &State, changes: &[OwnedChange]) -> Result<()>;
}
