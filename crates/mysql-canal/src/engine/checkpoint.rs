//! Per-source schema snapshots, atomic checkpoints and exclusive state locks.
use crate::{Error, Result, TableMeta, config::Config, error::invalid};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub version: u32,
    pub source_uuid: String,
    pub source_id: String,
    pub source_plugin: String,
    pub file: String,
    pub position: u64,
    pub tables: Vec<TableMeta>,
}
impl State {
    pub fn read(path: &Path) -> Result<Self> {
        Ok(read_checkpoint(path)?.0)
    }

    /// Selects the recorded schema at a replay boundary in the named file.
    pub fn read_at(path: &Path, file: &str, position: u64) -> Result<Self> {
        let (latest, history) = read_checkpoint(path)?;
        if latest.file == file && latest.position == position {
            return Ok(latest);
        }
        if latest.file == file && position > latest.position {
            return Err(invalid("requested position is after the checkpoint"));
        }
        let mut state = history
            .into_iter()
            .rev()
            .find(|s| s.file == file && s.position <= position)
            .ok_or_else(|| invalid("no historical schema at requested file/position"))?;
        state.position = position;
        state.validate()?;
        Ok(state)
    }
    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.file.is_empty()
            || self.position < 4
            || self.position > u64::from(u32::MAX)
            || self.source_uuid.is_empty()
            || self.source_id.is_empty()
            || self.source_plugin.is_empty()
            || self.tables.is_empty()
        {
            return Err(invalid("invalid state version/source/file/position/tables"));
        }
        let mut names = BTreeSet::new();
        for table in &self.tables {
            table.validate()?;
            if !names.insert((&table.schema, &table.table)) {
                return Err(invalid("duplicate state table"));
            }
        }
        Ok(())
    }
    pub fn verify_whitelist(&self, config: &Config) -> Result<()> {
        let saved: BTreeSet<_> = self.tables.iter().map(|t| (&t.schema, &t.table)).collect();
        let requested: BTreeSet<_> = config
            .tables
            .iter()
            .map(|t| (&t.schema, &t.table))
            .collect();
        if saved != requested {
            return Err(Error::Schema(
                "whitelist differs from saved state; initialize a new state file".into(),
            ));
        }
        Ok(())
    }
    /// Atomic replacement; fsync the contents and parent directory before returning.
    pub fn save(&self, path: &Path) -> Result<()> {
        if path.try_exists()? {
            let input = std::io::BufReader::new(File::open(path)?);
            match serde_json::from_reader::<_, HistoryFile>(input) {
                Ok(existing) => {
                    validate_history(&existing.checkpoint, &existing.schema_history)?;
                    validate_history(self, &existing.schema_history)?;
                    return save_checkpoint(path, self, &existing.schema_history);
                }
                Err(error) if error.is_io() => return Err(error.into()),
                Err(_) => {}
            }
        }
        save_checkpoint(path, self, &[])
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryFile {
    checkpoint: State,
    schema_history: Vec<State>,
}

pub(super) fn read_checkpoint(path: &Path) -> Result<(State, Vec<State>)> {
    let bytes = std::fs::read(path)?;
    let file = match serde_json::from_slice::<State>(&bytes) {
        Ok(state) => {
            state.validate()?;
            return Ok((state, Vec::new()));
        }
        Err(legacy_error) => {
            serde_json::from_slice::<HistoryFile>(&bytes).map_err(|_| legacy_error)?
        }
    };
    let HistoryFile {
        checkpoint,
        schema_history,
    } = file;
    validate_history(&checkpoint, &schema_history)?;
    Ok((checkpoint, schema_history))
}

fn validate_history(checkpoint: &State, schema_history: &[State]) -> Result<()> {
    checkpoint.validate()?;
    let mut files = BTreeSet::new();
    let mut previous: Option<&State> = None;
    for version in schema_history {
        version.validate()?;
        if version.source_id != checkpoint.source_id
            || version.source_plugin != checkpoint.source_plugin
            || version.source_uuid != checkpoint.source_uuid
            || !version
                .tables
                .iter()
                .map(|t| (&t.schema, &t.table))
                .eq(checkpoint.tables.iter().map(|t| (&t.schema, &t.table)))
        {
            return Err(invalid("schema history identity or whitelist mismatch"));
        }
        if previous.is_some_and(|s| s.file == version.file) {
            if previous.is_some_and(|s| s.position >= version.position) {
                return Err(invalid("schema history positions must increase"));
            }
        } else if !files.insert(&version.file) {
            return Err(invalid("repeated file in schema history"));
        }
        previous = Some(version);
    }
    if !previous.is_some_and(|s| {
        s.file == checkpoint.file
            && s.position <= checkpoint.position
            && s.tables == checkpoint.tables
    }) {
        return Err(invalid("schema history differs from current checkpoint"));
    }
    Ok(())
}

pub(super) fn save_checkpoint(path: &Path, state: &State, history: &[State]) -> Result<()> {
    #[derive(Serialize)]
    struct WithHistory<'a> {
        checkpoint: &'a State,
        schema_history: &'a [State],
    }
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let (temporary, file) = loop {
        let suffix = NEXT.fetch_add(1, Ordering::Relaxed);
        let mut name = path.as_os_str().to_owned();
        name.push(format!(".{}.{suffix}.tmp", std::process::id()));
        let temporary = std::path::PathBuf::from(name);
        match options.open(&temporary) {
            Ok(file) => break (temporary, file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    };
    let result = (|| {
        let mut file = std::io::BufWriter::new(file);
        if history.is_empty() {
            serde_json::to_writer_pretty(&mut file, state)?;
        } else {
            serde_json::to_writer_pretty(
                &mut file,
                &WithHistory {
                    checkpoint: state,
                    schema_history: history,
                },
            )?;
        }
        file.write_all(b"\n")?;
        file.flush()?;
        file.get_ref().sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    if let Err(error) = result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error);
    }
    File::open(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}

pub struct StateLock {
    _file: File,
}
impl StateLock {
    pub fn acquire(path: &Path) -> Result<Self> {
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        file.try_lock_exclusive()?;
        Ok(Self { _file: file })
    }
}
