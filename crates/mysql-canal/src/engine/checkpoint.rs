//! Per-source schema snapshots, atomic checkpoints and exclusive state locks.
use crate::{config::Config, error::invalid, Error, Result, TableMeta};
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
        let mut temporary = path.as_os_str().to_owned();
        temporary.push(format!(".{}.tmp", std::process::id()));
        let temporary = std::path::PathBuf::from(temporary);
        let result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            serde_json::to_writer_pretty(&mut file, self)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            std::fs::rename(&temporary, path)?;
            File::open(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new(".")),
            )?
            .sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result
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
