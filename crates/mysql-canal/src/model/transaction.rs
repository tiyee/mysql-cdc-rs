//! Committed transaction views borrowed during handler delivery.

use crate::{OwnedChange, engine::checkpoint::State};

/// A normalized, committed transaction. Row values are not materialized.
///
/// The data is borrowed from the transaction buffer and is valid only during
/// the callback. Clone changes only when retaining them beyond the callback.
#[derive(Clone, Copy, Debug)]
pub struct CommittedTransaction<'a> {
    pub source_id: &'a str,
    /// The commit boundary, persisted only after every handler succeeds.
    pub checkpoint: &'a State,
    /// Changes in binlog order; empty for a fully filtered transaction.
    pub changes: &'a [OwnedChange],
}
