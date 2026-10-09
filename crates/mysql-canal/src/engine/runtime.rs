use crate::{
    config::{AppConfig, SourceEntry},
    engine::checkpoint::{State, StateLock},
    engine::processor::Processor,
    protocol::mysql::version::VersionRegistry,
    sink::TransactionSink,
    source::{SourceRegistry, SourceSession},
    Error, Result,
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Default)]
pub struct Runtime {
    pub sources: SourceRegistry,
    pub versions: VersionRegistry,
}
struct Prepared {
    entry: SourceEntry,
    session: Box<dyn SourceSession>,
    state: State,
    _lock: StateLock,
}
impl Runtime {
    fn connect_all(&self, config: &AppConfig) -> Result<Vec<Box<dyn SourceSession>>> {
        config.validate()?;
        let mut identities = HashSet::new();
        let mut sessions = Vec::with_capacity(config.sources.len());
        for entry in &config.sources {
            let session = self
                .sources
                .resolve(&entry.config.source.plugin)?
                .connect(&entry.config, &self.versions)
                .map_err(|e| Error::Sources(format!("{}: {e}", entry.id)))?;
            let info = session.info();
            if info.uuid.is_empty()
                || !identities.insert((info.uuid.clone(), entry.config.source.server_id))
            {
                return Err(Error::Schema(
                    "same upstream UUID requires distinct replication server_id values".into(),
                ));
            }
            eprintln!(
                "source {}: {} / {} / UUID {}",
                entry.id, info.version.raw, info.adapter, info.uuid
            );
            sessions.push(session);
        }
        Ok(sessions)
    }
    pub fn check(&self, config: &AppConfig) -> Result<()> {
        for (entry, mut session) in config.sources.iter().zip(self.connect_all(config)?) {
            let tables = session.load_tables(&entry.config)?;
            eprintln!(
                "source {}: validated {} whitelist tables",
                entry.id,
                tables.len()
            );
        }
        Ok(())
    }
    fn prepare(&self, config: AppConfig, initialize_only: bool) -> Result<Vec<Prepared>> {
        let sessions = self.connect_all(&config)?;
        // Acquire all locks and inspect existing checkpoints before initializing.
        let locks: Vec<_> = config
            .sources
            .iter()
            .map(|s| StateLock::acquire(&s.config.state_file))
            .collect::<Result<_>>()?;
        let mut saved = Vec::new();
        for (entry, session) in config.sources.iter().zip(&sessions) {
            let state = if entry.config.state_file.exists() {
                if initialize_only {
                    return Err(Error::Schema(format!(
                        "{}: state file exists; initialize with a new state_file",
                        entry.id
                    )));
                }
                let state = State::read(&entry.config.state_file)?;
                state.verify_whitelist(&entry.config)?;
                if state.source_uuid != session.info().uuid
                    || state.source_id != entry.id
                    || state.source_plugin != entry.config.source.plugin
                {
                    return Err(Error::Schema(format!(
                        "{}: checkpoint source UUID/id/plugin mismatch",
                        entry.id
                    )));
                }
                Some(state)
            } else {
                None
            };
            saved.push(state);
        }
        let mut ready = Vec::new();
        for (((entry, mut session), lock), saved) in config
            .sources
            .into_iter()
            .zip(sessions)
            .zip(locks)
            .zip(saved)
        {
            let state = match saved {
                Some(state) => state,
                None => session.initialize(&entry.id, &entry.config)?,
            };
            state.validate()?;
            state.verify_whitelist(&entry.config)?;
            if state.source_uuid != session.info().uuid
                || state.source_id != entry.id
                || state.source_plugin != entry.config.source.plugin
            {
                return Err(Error::Schema(format!(
                    "{}: initialized checkpoint UUID/id/plugin differs from configured source",
                    entry.id
                )));
            }
            state.save(&entry.config.state_file)?;
            ready.push(Prepared {
                entry,
                session,
                state,
                _lock: lock,
            });
        }
        Ok(ready)
    }
    pub fn initialize(&self, config: AppConfig) -> Result<()> {
        self.prepare(config, true)?;
        Ok(())
    }
    /// Fail fast as a process: on a worker failure, peers stop between events.
    /// Blocking socket reads are bounded by the source plugin's read timeout.
    pub fn run(&self, config: AppConfig, once: bool, sink: Arc<dyn TransactionSink>) -> Result<()> {
        let prepared = self.prepare(config, false)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for prepared in prepared {
                let sink = Arc::clone(&sink);
                let cancelled = Arc::clone(&cancelled);
                let registry = self.versions.clone();
                workers.push(scope.spawn(move || {
                    let _panic_guard = CancelOnPanic(&cancelled);
                    let Prepared {
                        entry,
                        session,
                        state,
                        _lock,
                    } = prepared;
                    let result = (|| {
                        let mut processor = Processor::with_registry(
                            state,
                            Some(entry.config.state_file.clone()),
                            entry.config.max_transaction_bytes,
                            registry,
                            entry.config.source.adapter.clone(),
                        )?;
                        let mut stream = session.start(&entry.config, processor.state(), once)?;
                        while !cancelled.load(Ordering::Acquire) {
                            let Some(event) = stream.next_event()? else {
                                return processor.finish();
                            };
                            if cancelled.load(Ordering::Acquire) {
                                return Ok(());
                            }
                            processor.process_to(event, sink.as_ref())?;
                        }
                        Ok(())
                    })();
                    if let Err(error) = &result {
                        cancelled.store(true, Ordering::Release);
                        eprintln!("source {} stopped: {error}", entry.id);
                    }
                    result.map_err(|e| format!("{}: {e}", entry.id))
                }));
            }
            let mut failures = Vec::new();
            for worker in workers {
                match worker.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => failures.push(error),
                    Err(_) => {
                        cancelled.store(true, Ordering::Release);
                        failures.push("source worker panicked".into());
                    }
                }
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(Error::Sources(failures.join("; ")))
            }
        })
    }
}

struct CancelOnPanic<'a>(&'a AtomicBool);
impl Drop for CancelOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}
