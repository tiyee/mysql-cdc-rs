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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        let state: Self = serde_json::from_reader(File::open(path)?)?;
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
            serde_json::to_writer_pretty(&mut file, self)?;
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
