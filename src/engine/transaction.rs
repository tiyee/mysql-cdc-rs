use crate::{Change, Error, OwnedChange, Result, engine::checkpoint::State};
use std::path::Path;

/// Failures must abort consumption; retry from the last persisted commit boundary.
/// Stdout cannot provide exactly-once delivery. A durable custom sink should ack a
/// transaction before persisting its position.
pub struct TransactionBuffer {
    changes: Vec<OwnedChange>,
    bytes: usize,
    limit: usize,
    active: bool,
}
impl TransactionBuffer {
    pub fn new(limit: usize) -> Self {
        Self {
            changes: Vec::new(),
            bytes: 0,
            limit,
            active: false,
        }
    }
    pub fn begin(&mut self) -> Result<()> {
        if self.active {
            return Err(crate::error::invalid(
                "nested transaction / incomplete start position",
            ));
        }
        self.active = true;
        Ok(())
    }
    pub fn is_active(&self) -> bool {
        self.active
    }
    pub fn push(&mut self, change: Change<'_>) -> Result<()> {
        if !self.active {
            return Err(crate::error::invalid(
                "row outside BEGIN; start at a transaction boundary",
            ));
        }
        let owned = change.into_owned();
        let bytes = owned.memory_bytes();
        let total = self
            .bytes
            .checked_add(bytes)
            .ok_or(Error::TransactionLimit(self.limit))?;
        let size = std::mem::size_of::<OwnedChange>();
        let mut capacity = self.changes.capacity();
        if self.changes.len() == capacity {
            let maximum =
                (self.limit.saturating_sub(total) / size).saturating_add(self.changes.len() + 1);
            capacity = capacity.saturating_mul(2).max(8).min(maximum);
        }
        let unused_capacity = capacity
            .saturating_sub(self.changes.len() + 1)
            .saturating_mul(size);
        if total.saturating_add(unused_capacity) > self.limit {
            return Err(Error::TransactionLimit(self.limit));
        }
        if capacity > self.changes.capacity() {
            self.changes.reserve_exact(capacity - self.changes.len());
        }
        self.changes.push(owned);
        self.bytes = total;
        Ok(())
    }
    pub fn rollback(&mut self) {
        self.changes.clear();
        self.bytes = 0;
        self.active = false;
    }
    pub fn commit_with(
        &mut self,
        state: &State,
        state_path: Option<&Path>,
        emit: impl FnOnce(&[OwnedChange]) -> Result<()>,
    ) -> Result<()> {
        if !self.active {
            return Err(crate::error::invalid("commit outside BEGIN"));
        }
        emit(&self.changes)?;
        if let Some(path) = state_path {
            state.save(path)?;
        }
        self.rollback();
        Ok(())
    }
}
