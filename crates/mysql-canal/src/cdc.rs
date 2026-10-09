//! Configuration-driven CDC and committed transaction listeners.

use crate::{
    Error, OwnedChange, Result,
    config::AppConfig,
    engine::{Runtime, checkpoint::State},
    sink::TransactionSink,
};
use std::sync::{Arc, Mutex};

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

/// Replicates the configured sources and delivers committed changes to handlers.
///
/// There is no default handler. Register at least one before starting, to avoid
/// advancing checkpoints without a consumer. Use `with_runtime` to register
/// custom source plugins or version adapters.
pub struct Cdc {
    config: AppConfig,
    runtime: Runtime,
    handlers: Vec<Box<dyn TransactionHandler>>,
}

impl Cdc {
    pub fn new(config: AppConfig) -> Result<Self> {
        Self::with_runtime(config, Runtime::default())
    }

    pub fn with_runtime(config: AppConfig, runtime: Runtime) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            runtime,
            handlers: Vec::new(),
        })
    }

    /// Registers a handler or a `Fn(CommittedTransaction<'_>) -> Result<()>` closure.
    pub fn add_handle(&mut self, handler: impl TransactionHandler + 'static) -> &mut Self {
        self.handlers.push(Box::new(handler));
        self
    }

    /// Validates the upstream settings and whitelist without consuming events.
    pub fn check(&self) -> Result<()> {
        self.runtime.check(&self.config)
    }

    /// Creates initial checkpoints without consuming events.
    pub fn initialize(self) -> Result<()> {
        self.runtime.initialize(self.config)
    }

    /// Consumes continuously until a source or handler fails.
    pub fn run(self) -> Result<()> {
        self.start(false)
    }

    /// Consumes the currently available binlog and returns at its end.
    pub fn run_once(self) -> Result<()> {
        self.start(true)
    }

    fn start(self, once: bool) -> Result<()> {
        if self.handlers.is_empty() {
            return Err(Error::Sink("register at least one event handler".into()));
        }
        self.runtime.run(
            self.config,
            once,
            Arc::new(Handlers {
                handlers: self.handlers,
                failed: Mutex::new(false),
            }),
        )
    }
}

struct Handlers {
    handlers: Vec<Box<dyn TransactionHandler>>,
    failed: Mutex<bool>,
}

impl TransactionSink for Handlers {
    fn commit(&self, source_id: &str, state: &State, changes: &[OwnedChange]) -> Result<()> {
        // Serialize whole transactions, including all registered callbacks.
        let mut failed = self
            .failed
            .lock()
            .map_err(|_| Error::Sink("event handler lock poisoned".into()))?;
        if *failed {
            return Err(Error::Sink("an event handler failed previously".into()));
        }
        let event = CommittedTransaction {
            source_id,
            checkpoint: state,
            changes,
        };
        for handler in &self.handlers {
            if let Err(error) = handler.handle(event) {
                *failed = true;
                return Err(error);
            }
        }
        Ok(())
    }
}
