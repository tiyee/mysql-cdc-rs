//! Canal source lifecycle and committed transaction delivery.

use crate::{
    CommittedTransaction, Error, OwnedChange, Result, TransactionHandler,
    config::AppConfig,
    engine::{Runtime, checkpoint::State},
    sink::TransactionSink,
};
use std::sync::{Arc, Mutex};

/// Replicates the configured sources and delivers committed changes to handlers.
///
/// There is no default handler. Register at least one before starting, to avoid
/// advancing checkpoints without a consumer. Use `with_runtime` to register
/// custom source plugins or version adapters.
/// Each source selects FULL or MINIMAL through its replication configuration.
pub struct Canal {
    config: AppConfig,
    runtime: Runtime,
    handlers: Vec<Box<dyn TransactionHandler>>,
}

impl Canal {
    /// Constructs the consumer for all configured sources.
    ///
    /// ```no_run
    /// use mysql_canal::{Canal, CommittedTransaction, Result, config::AppConfig};
    /// use std::path::Path;
    /// fn main() -> Result<()> {
    ///     let mut canal = Canal::new(AppConfig::read(Path::new("canal.toml"))?)?;
    ///     canal.add_handle(|_: CommittedTransaction<'_>| Ok(()));
    ///     canal.run()
    /// }
    /// ```
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
