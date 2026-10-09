use crate::{Error, Result};
use serde::Deserialize;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub source: SourceConfig,
    pub state_file: PathBuf,
    #[serde(default = "transaction_budget")]
    pub max_transaction_bytes: usize,
    pub tables: Vec<WhitelistTable>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    #[serde(default = "url_env")]
    pub url_env: String,
    pub server_id: u32,
    pub tls_ca: Option<PathBuf>,
    #[serde(default = "mysql_plugin")]
    pub plugin: String,
    pub adapter: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhitelistTable {
    pub schema: String,
    pub table: String,
}
fn mysql_plugin() -> String {
    "mysql".into()
}
fn url_env() -> String {
    "MYSQL_URL".into()
}
fn transaction_budget() -> usize {
    64 * 1024 * 1024
}

impl Config {
    pub fn read(path: &Path) -> Result<Self> {
        let mut value: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        value.resolve(path)?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<()> {
        let value = self;
        if value.source.server_id == 0
            || value.max_transaction_bytes == 0
            || value.tables.is_empty()
        {
            return Err(Error::Schema(
                "server_id, transaction budget and whitelist must be nonempty/nonzero".into(),
            ));
        }
        let mut tables = HashSet::new();
        for table in &value.tables {
            if table.schema.is_empty()
                || table.table.is_empty()
                || !tables.insert((&table.schema, &table.table))
            {
                return Err(Error::Schema("empty/duplicate whitelist entry".into()));
            }
        }
        if value.source.plugin.is_empty()
            || value.source.url_env.is_empty()
            || value.source.adapter.as_ref().is_some_and(String::is_empty)
        {
            return Err(Error::Schema("empty plugin, url_env or adapter".into()));
        }
        Ok(())
    }
    fn resolve(&mut self, path: &Path) -> Result<()> {
        self.validate()?;
        let value = self;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        if value.state_file.is_relative() {
            value.state_file = base.join(&value.state_file);
        }
        if let Some(ca) = &mut value.source.tls_ca
            && ca.is_relative()
        {
            *ca = base.join(&*ca);
        }
        Ok(())
    }
}

/// Each source owns its schema, connection and checkpoint. Legacy single-source
/// TOML remains accepted with the stable ID "default".
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEntry {
    pub id: String,
    #[serde(flatten)]
    pub config: Config,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub sources: Vec<SourceEntry>,
}
impl AppConfig {
    pub fn read(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let raw: toml::Value = toml::from_str(&text)?;
        let mut app = if raw.get("sources").is_some() {
            toml::from_str::<Self>(&text)?
        } else {
            Self {
                sources: vec![SourceEntry {
                    id: "default".into(),
                    config: toml::from_str(&text)?,
                }],
            }
        };
        for entry in &mut app.sources {
            entry.config.resolve(path)?;
        }
        app.validate()?;
        Ok(app)
    }
    pub fn validate(&self) -> Result<()> {
        if self.sources.is_empty() {
            return Err(Error::Schema("no sources configured".into()));
        }
        let mut ids = HashSet::new();
        let mut states = HashSet::new();
        for entry in &self.sources {
            entry.config.validate()?;
            if entry.id.is_empty() || !ids.insert(&entry.id) {
                return Err(Error::Schema("empty/duplicate source id".into()));
            }
            // Canonicalize the parent even for checkpoints not yet created.
            let path = &entry.config.state_file;
            let key = if path.exists() {
                std::fs::canonicalize(path)?
            } else {
                let parent = path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                std::fs::canonicalize(parent)?.join(
                    path.file_name()
                        .ok_or_else(|| Error::Schema("state_file requires filename".into()))?,
                )
            };
            if !states.insert(key) {
                return Err(Error::Schema(
                    "sources must use different state files".into(),
                ));
            }
        }
        Ok(())
    }
}
