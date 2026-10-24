//! Synchronous handlers for committed transactions.

use crate::{CommittedTransaction, Result};

/// Handles converted events synchronously, applying backpressure to consumption.
///
/// Handlers run in registration order, with no interleaving across sources.
/// An error stops consumption without acknowledging this transaction. Earlier
/// handlers may already have produced side effects, so retries must tolerate
/// replay. Panics also stop consumption; they do not acknowledge the event.
pub trait TransactionHandler: Send + Sync {
    fn handle(&self, event: CommittedTransaction<'_>) -> Result<()>;
}

impl<F> TransactionHandler for F
where
    F: Fn(CommittedTransaction<'_>) -> Result<()> + Send + Sync,
{
    fn handle(&self, event: CommittedTransaction<'_>) -> Result<()> {
        self(event)
    }
}
