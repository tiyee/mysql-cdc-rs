//! Transaction sink contract and output implementations.
mod json_lines;
use crate::{engine::checkpoint::State, OwnedChange, Result};
pub use json_lines::JsonLinesSink;

/// A successful commit acknowledges the complete transaction, before checkpoint
/// persistence. Implementations must be safe for concurrent source workers.
pub trait TransactionSink: Send + Sync {
    fn commit(&self, source_id: &str, state: &State, changes: &[OwnedChange]) -> Result<()>;
}
